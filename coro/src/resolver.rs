use std::{marker::PhantomData, num::NonZeroUsize};

use commonware_actor::Feedback;
use commonware_cryptography::ed25519;
use commonware_resolver::{opaque, Consumer, Fetch, Resolver as ResolverTrait};
use commonware_runtime::{Clock, Metrics, Spawner};

use crate::{
    backend::Backend,
    config::{ReaderConfig, ResolverConfig},
    error::Result,
    types::{BlobKey, BlobRef, VerifiedBlob},
};

pub struct CelestiaResolver<E, B, C>
where
    C: Consumer<Key = BlobKey, Value = VerifiedBlob>,
{
    inner: opaque::Resolver<BlobKey, C::Subscriber, ed25519::PublicKey>,
    _phantom: PhantomData<fn(E, B, C)>,
}

#[derive(Clone)]
struct Fetcher<B> {
    backend: B,
    config: ReaderConfig,
}

impl<E, B, C> Clone for CelestiaResolver<E, B, C>
where
    C: Consumer<Key = BlobKey, Value = VerifiedBlob>,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<E, B, C> CelestiaResolver<E, B, C>
where
    E: Clock + Spawner + Metrics,
    B: Backend,
    C: Consumer<Key = BlobKey, Value = VerifiedBlob>,
    C::Subscriber: Ord,
{
    pub fn new(
        context: E,
        backend: B,
        reader_config: ReaderConfig,
        config: ResolverConfig,
        consumer: C,
    ) -> Result<Self> {
        config.validate()?;
        let fetcher = Fetcher {
            backend,
            config: reader_config,
        };
        let inner = opaque::init(
            context,
            fetcher,
            consumer,
            NonZeroUsize::new(config.max_inflight).expect("validated non-zero"),
            config.retry_interval,
        );
        Ok(Self {
            inner,
            _phantom: PhantomData,
        })
    }
}

impl<B> opaque::Fetcher for Fetcher<B>
where
    B: Backend + Sync,
{
    type Key = BlobKey;
    type Value = VerifiedBlob;

    async fn fetch(&self, key: Self::Key) -> Option<Self::Value> {
        let blob_ref = BlobRef::from(key);
        if blob_ref.namespace != self.config.chain.namespace {
            return None;
        }
        crate::reader::read_exact_once(self.backend.clone(), blob_ref, self.config.verification)
            .await
            .ok()
    }
}

impl<E, B, C> ResolverTrait for CelestiaResolver<E, B, C>
where
    C: Consumer<Key = BlobKey, Value = VerifiedBlob>,
    E: 'static,
    B: 'static,
{
    type Key = BlobKey;
    type Subscriber = C::Subscriber;

    fn fetch<F>(&mut self, key: F) -> Feedback
    where
        F: Into<Fetch<Self::Key, Self::Subscriber>> + Send,
    {
        self.inner.fetch(key)
    }

    fn fetch_all<F>(&mut self, keys: Vec<F>) -> Feedback
    where
        F: Into<Fetch<Self::Key, Self::Subscriber>> + Send,
    {
        self.inner.fetch_all(keys)
    }

    fn retain(
        &mut self,
        predicate: impl Fn(&Self::Key, &Self::Subscriber) -> bool + Send + 'static,
    ) -> Feedback {
        self.inner.retain(predicate)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use bytes::Bytes;
    use commonware_resolver::{Consumer, Delivery, Resolver as _};
    use commonware_runtime::{deterministic, Runner, Supervisor};
    use tokio::sync::oneshot;

    use super::*;
    use crate::{
        backend::MockBackend,
        config::{ChainConfig, ReaderConfig, ResolverConfig, VerificationMode},
        types::{BlobKey, NamespaceId, Verification},
        Error,
    };

    #[derive(Clone)]
    struct HangingBackend;

    #[async_trait]
    impl Backend for HangingBackend {
        async fn broadcast(
            &self,
            _namespace: NamespaceId,
            _payload: Bytes,
            _tx_config: ::celestia_client::tx::TxConfig,
        ) -> crate::Result<crate::backend::BroadcastedSubmission> {
            Err(Error::UnsupportedVerificationMode)
        }

        async fn confirm(
            &self,
            _namespace: NamespaceId,
            _payload: Bytes,
            _tx_config: ::celestia_client::tx::TxConfig,
            _submission: &crate::backend::BroadcastedSubmission,
        ) -> crate::Result<Option<crate::backend::SubmittedBlob>> {
            Err(Error::UnsupportedVerificationMode)
        }

        async fn get(&self, _blob_ref: BlobRef) -> crate::Result<Bytes> {
            std::future::pending::<()>().await;
            unreachable!()
        }

        async fn verify(
            &self,
            _blob_ref: BlobRef,
            _mode: VerificationMode,
        ) -> crate::Result<Verification> {
            unreachable!()
        }
    }

    #[derive(Clone, Default)]
    struct RecordingConsumer {
        deliveries: Arc<Mutex<Vec<VerifiedBlob>>>,
    }

    impl Consumer for RecordingConsumer {
        type Key = BlobKey;
        type Value = VerifiedBlob;
        type Subscriber = u64;

        fn deliver(
            &mut self,
            _delivery: Delivery<Self::Key, Self::Subscriber>,
            value: Self::Value,
        ) -> oneshot::Receiver<bool> {
            self.deliveries.lock().unwrap().push(value);
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(true);
            rx
        }
    }

    fn user_namespace() -> NamespaceId {
        let mut ns = [0u8; 29];
        ns[19..].copy_from_slice(b"namespace1");
        NamespaceId(ns)
    }

    #[test]
    fn delivers_only_after_successful_fetch() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let backend = MockBackend::new().with_proof_verification(true);
            let payload = Bytes::from_static(b"payload");
            let namespace = user_namespace();
            let commitment = crate::types::compute_commitment(namespace, payload.as_ref()).unwrap();
            let blob_ref = BlobRef {
                height: 1,
                namespace,
                commitment,
            };
            backend.insert_blob(blob_ref, payload.clone());

            let consumer = RecordingConsumer::default();
            let deliveries = consumer.deliveries.clone();
            let mut resolver = CelestiaResolver::new(
                context.child("resolver"),
                backend,
                ReaderConfig {
                    chain: ChainConfig {
                        namespace,
                        max_payload_bytes: 1024,
                    },
                    verification: VerificationMode::RpcIncluded,
                    read_timeout: Duration::from_secs(1),
                },
                ResolverConfig {
                    retry_interval: Duration::from_millis(1),
                    max_inflight: 4,
                },
                consumer,
            )
            .unwrap();

            resolver.fetch(BlobKey::from(blob_ref));
            context.sleep(Duration::from_millis(10)).await;

            let deliveries = deliveries.lock().unwrap();
            assert_eq!(deliveries.len(), 1);
            assert_eq!(deliveries[0].payload, payload);
            assert_eq!(deliveries[0].verification, Verification::RpcIncluded);
        });
    }

    #[test]
    fn retain_cancels_pending_delivery() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let consumer = RecordingConsumer::default();
            let deliveries = consumer.deliveries.clone();
            let mut resolver = CelestiaResolver::new(
                context.child("resolver"),
                HangingBackend,
                ReaderConfig {
                    chain: ChainConfig {
                        namespace: user_namespace(),
                        max_payload_bytes: 1024,
                    },
                    verification: VerificationMode::RpcIncluded,
                    read_timeout: Duration::from_secs(1),
                },
                ResolverConfig {
                    retry_interval: Duration::from_millis(1),
                    max_inflight: 4,
                },
                consumer,
            )
            .unwrap();

            let blob_ref = BlobRef {
                height: 1,
                namespace: user_namespace(),
                commitment: crate::types::BlobCommitment([9; 32]),
            };
            resolver.fetch(commonware_resolver::Fetch {
                key: BlobKey::from(blob_ref),
                subscriber: 7,
            });
            resolver.retain(|_, _| false);
            context.sleep(Duration::from_millis(10)).await;
            assert!(deliveries.lock().unwrap().is_empty());
        });
    }
}
