use std::sync::Arc;

use commonware_runtime::Clock;

use crate::{
    backend::Backend,
    config::{ReaderConfig, VerificationMode},
    error::Result,
    types::{validate_commitment, BlobRef, VerifiedBlob},
    Error,
};

#[derive(Clone)]
pub struct Reader<C, B> {
    context: Arc<C>,
    backend: B,
    config: ReaderConfig,
}

impl<C, B> Reader<C, B>
where
    C: Clock + Send + Sync + 'static,
    B: Backend,
{
    pub fn new(context: C, backend: B, config: ReaderConfig) -> Self {
        Self {
            context: Arc::new(context),
            backend,
            config,
        }
    }

    pub fn from_shared(context: Arc<C>, backend: B, config: ReaderConfig) -> Self {
        Self {
            context,
            backend,
            config,
        }
    }

    pub async fn read(&self, blob_ref: BlobRef) -> Result<VerifiedBlob> {
        self.config.validate()?;
        if blob_ref.namespace != self.config.chain.namespace {
            return Err(Error::InvalidNamespace);
        }

        let backend = self.backend.clone();
        let verification_mode = self.config.verification;
        Ok(self
            .context
            .timeout(self.config.read_timeout, async move {
                read_exact_once(backend, blob_ref, verification_mode).await
            })
            .await??)
    }
}

pub(crate) async fn read_exact_once<B>(
    backend: B,
    blob_ref: BlobRef,
    verification_mode: VerificationMode,
) -> Result<VerifiedBlob>
where
    B: Backend,
{
    let payload = backend.get(blob_ref).await?;
    validate_commitment(blob_ref, payload.as_ref())?;
    let verification = backend.verify(blob_ref, verification_mode).await?;
    Ok(VerifiedBlob {
        blob_ref,
        payload,
        verification,
    })
}

#[cfg(test)]
mod tests {
    use std::{future::pending, sync::Arc, time::Duration};

    use async_trait::async_trait;
    use bytes::Bytes;
    use commonware_runtime::{deterministic, Runner};

    use super::*;
    use crate::{
        backend::{Backend, BroadcastedSubmission, MockBackend, SubmittedBlob},
        config::{ChainConfig, VerificationMode},
        types::{compute_commitment, BlobCommitment, NamespaceId, Verification},
    };

    fn user_namespace() -> NamespaceId {
        let mut ns = [0u8; 29];
        ns[19..].copy_from_slice(b"namespace1");
        NamespaceId(ns)
    }

    fn config(verification: VerificationMode) -> ReaderConfig {
        ReaderConfig {
            chain: ChainConfig {
                namespace: user_namespace(),
                max_payload_bytes: 1024,
            },
            verification,
            read_timeout: Duration::from_millis(5),
        }
    }

    #[derive(Clone)]
    struct HangingBackend;

    #[async_trait]
    impl Backend for HangingBackend {
        async fn broadcast(
            &self,
            _namespace: NamespaceId,
            _payload: Bytes,
            _tx_config: ::celestia_client::tx::TxConfig,
        ) -> Result<BroadcastedSubmission> {
            Err(Error::UnsupportedVerificationMode)
        }

        async fn confirm(
            &self,
            _namespace: NamespaceId,
            _payload: Bytes,
            _tx_config: ::celestia_client::tx::TxConfig,
            _submission: &BroadcastedSubmission,
        ) -> Result<Option<SubmittedBlob>> {
            Err(Error::UnsupportedVerificationMode)
        }

        async fn get(&self, _blob_ref: BlobRef) -> Result<Bytes> {
            pending::<()>().await;
            unreachable!()
        }

        async fn verify(&self, _blob_ref: BlobRef, _mode: VerificationMode) -> Result<Verification> {
            unreachable!()
        }
    }

    #[test]
    fn wrong_namespace_is_rejected() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let reader = Reader::<deterministic::Context, _>::from_shared(
                context,
                backend,
                config(VerificationMode::RpcIncluded),
            );
            let mut wrong_ns = [0u8; 29];
            wrong_ns[19..].copy_from_slice(b"namespace2");
            let blob_ref = BlobRef {
                height: 1,
                namespace: NamespaceId(wrong_ns),
                commitment: BlobCommitment([7; 32]),
            };
            assert!(matches!(reader.read(blob_ref).await, Err(Error::InvalidNamespace)));
        });
    }

    #[test]
    fn wrong_height_and_commitment_return_not_found() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let payload = Bytes::from_static(b"payload");
            let blob_ref = BlobRef {
                height: 5,
                namespace: user_namespace(),
                commitment: compute_commitment(user_namespace(), payload.as_ref()).unwrap(),
            };
            backend.insert_blob(blob_ref, payload);
            let reader = Reader::<deterministic::Context, _>::from_shared(
                context,
                backend,
                config(VerificationMode::RpcIncluded),
            );

            let wrong_height = BlobRef { height: 6, ..blob_ref };
            assert!(matches!(reader.read(wrong_height).await, Err(Error::NotFound)));

            let wrong_commitment = BlobRef {
                commitment: BlobCommitment([9; 32]),
                ..blob_ref
            };
            assert!(matches!(reader.read(wrong_commitment).await, Err(Error::NotFound)));
        });
    }

    #[test]
    fn commitment_mismatch_is_detected() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let blob_ref = BlobRef {
                height: 5,
                namespace: user_namespace(),
                commitment: BlobCommitment([3; 32]),
            };
            backend.insert_blob(blob_ref, Bytes::from_static(b"different-payload"));
            let reader = Reader::<deterministic::Context, _>::from_shared(
                context,
                backend,
                config(VerificationMode::RpcIncluded),
            );
            assert!(matches!(reader.read(blob_ref).await, Err(Error::CommitmentMismatch)));
        });
    }

    #[test]
    fn proof_required_is_supported_by_mock_when_enabled() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new().with_proof_verification(true);
            let payload = Bytes::from_static(b"payload");
            let blob_ref = BlobRef {
                height: 5,
                namespace: user_namespace(),
                commitment: compute_commitment(user_namespace(), payload.as_ref()).unwrap(),
            };
            backend.insert_blob(blob_ref, payload);
            let reader = Reader::<deterministic::Context, _>::from_shared(
                context,
                backend,
                config(VerificationMode::ProofRequired),
            );
            let verified = reader.read(blob_ref).await.unwrap();
            assert_eq!(verified.verification, Verification::ProofVerified);
        });
    }

    #[test]
    fn timeout_is_reported() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let reader = Reader::<deterministic::Context, _>::from_shared(
                context,
                HangingBackend,
                config(VerificationMode::RpcIncluded),
            );
            let blob_ref = BlobRef {
                height: 1,
                namespace: user_namespace(),
                commitment: BlobCommitment([1; 32]),
            };
            assert!(matches!(reader.read(blob_ref).await, Err(Error::Timeout)));
        });
    }
}
