use std::{cmp, sync::Arc, time::Duration};

use commonware_runtime::{Clock, Storage};
use tracing::{debug, warn};

use crate::{
    backend::{Backend, BroadcastedSubmission, SubmittedBlob},
    config::PublisherConfig,
    error::Result,
    store::{payload_hash, PendingRecord, PendingStage, ReceiptRecord, Store},
    types::{PublishReceipt, PublishRequest, VerifiedBlob},
    Error,
};

#[derive(Clone)]
pub struct Publisher<C, B> {
    context: Arc<C>,
    backend: B,
    config: PublisherConfig,
    store: Store<C>,
}

impl<C, B> Publisher<C, B>
where
    C: Clock + Storage + Send + Sync + 'static,
    B: Backend,
{
    pub fn new(context: C, backend: B, config: PublisherConfig) -> Self {
        let context = Arc::new(context);
        let store = Store::new(
            context.clone(),
            config.chain.max_payload_bytes,
            config.partition.clone(),
        );
        Self {
            context,
            backend,
            config,
            store,
        }
    }

    pub fn from_shared(context: Arc<C>, backend: B, config: PublisherConfig) -> Self {
        let store = Store::new(
            context.clone(),
            config.chain.max_payload_bytes,
            config.partition.clone(),
        );
        Self {
            context,
            backend,
            config,
            store,
        }
    }

    pub async fn publish(&self, request: PublishRequest) -> Result<PublishReceipt> {
        self.config.validate()?;
        self.store.ensure_initialized().await?;

        if request.payload.len() > self.config.chain.max_payload_bytes {
            return Err(Error::PayloadTooLarge);
        }

        if let Some(receipt) = self.store.load_receipt(request.id).await? {
            if receipt.payload_hash != payload_hash(request.payload.as_ref()) {
                return Err(Error::SubmissionConflict);
            }
            return Ok(receipt.into());
        }

        let pending = match self.store.load_pending(request.id).await? {
            Some(existing) => {
                if existing.payload_hash != payload_hash(request.payload.as_ref())
                    || existing.payload != request.payload
                {
                    return Err(Error::SubmissionConflict);
                }
                existing
            }
            None => {
                let record = PendingRecord::new(request.id, request.payload.clone());
                self.store.write_pending(&record).await?;
                record
            }
        };

        self.publish_pending(pending).await
    }

    pub async fn recover_pending(&self) -> Result<Vec<PublishReceipt>> {
        self.config.validate()?;
        self.store.ensure_initialized().await?;

        let mut receipts = Vec::new();
        for id in self.store.pending_ids().await? {
            if let Some(receipt) = self.store.load_receipt(id).await? {
                self.store.delete_pending(id).await?;
                receipts.push(receipt.into());
                continue;
            }
            if let Some(pending) = self.store.load_pending(id).await? {
                receipts.push(self.publish_pending(pending).await?);
            }
        }
        Ok(receipts)
    }

    async fn publish_pending(&self, mut pending: PendingRecord) -> Result<PublishReceipt> {
        let submission = match pending.stage.clone() {
            PendingStage::AwaitingBroadcast => {
                let namespace = self.config.chain.namespace;
                let payload = pending.payload.clone();
                let tx = self.config.tx.clone();
                let backend = self.backend.clone();
                let submission = retry_with_backoff(
                    self.context.as_ref(),
                    self.config.retry.initial_backoff,
                    self.config.retry.max_backoff,
                    self.config.retry.max_attempts,
                    move || {
                        let backend = backend.clone();
                        let payload = payload.clone();
                        let tx = tx.clone();
                        async move { backend.broadcast(namespace, payload, tx).await }
                    },
                )
                .await?;
                pending.stage = PendingStage::Broadcasted(submission.clone());
                self.store.write_pending(&pending).await?;
                submission
            }
            PendingStage::Broadcasted(submission) => submission,
        };

        let submitted = self.confirm_submission(&pending, &submission).await?;
        let verified = self.readback(submitted.clone()).await?;
        let receipt = PublishReceipt {
            id: pending.id,
            blob_ref: verified.blob_ref,
            tx_hash: submitted.tx_hash,
        };
        self.store
            .write_receipt(&ReceiptRecord::from((receipt.clone(), pending.payload_hash)))
            .await?;
        self.store.delete_pending(pending.id).await?;
        Ok(receipt)
    }

    async fn confirm_submission(
        &self,
        pending: &PendingRecord,
        submission: &BroadcastedSubmission,
    ) -> Result<SubmittedBlob> {
        let namespace = self.config.chain.namespace;
        let payload = pending.payload.clone();
        let tx = self.config.tx.clone();
        let backend = self.backend.clone();
        retry_until_some(
            self.context.as_ref(),
            self.config.retry.initial_backoff,
            self.config.retry.max_backoff,
            self.config.retry.max_attempts,
            move || {
                let backend = backend.clone();
                let payload = payload.clone();
                let tx = tx.clone();
                let submission = submission.clone();
                async move { backend.confirm(namespace, payload, tx, &submission).await }
            },
        )
        .await
    }

    async fn readback(&self, submitted: SubmittedBlob) -> Result<VerifiedBlob> {
        let backend = self.backend.clone();
        let context = self.context.clone();
        let timeout = self.config.readback_timeout;
        retry_with_backoff(
            self.context.as_ref(),
            self.config.retry.initial_backoff,
            self.config.retry.max_backoff,
            self.config.retry.max_attempts,
            move || {
                let backend = backend.clone();
                let context = context.clone();
                async move {
                    context
                        .timeout(timeout, async move {
                            crate::reader::read_exact_once(
                                backend,
                                submitted.blob_ref,
                                crate::config::VerificationMode::RpcIncluded,
                            )
                            .await
                        })
                        .await?
                }
            },
        )
        .await
    }
}

async fn retry_with_backoff<C, F, Fut, T>(
    context: &C,
    initial_backoff: Duration,
    max_backoff: Duration,
    max_attempts: Option<u32>,
    mut f: F,
) -> Result<T>
where
    C: Clock,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut attempt = 0u32;
    let mut backoff = initial_backoff;

    loop {
        attempt = attempt.saturating_add(1);
        match f().await {
            Ok(value) => {
                debug!(attempt, "operation completed");
                return Ok(value);
            }
            Err(error) => {
                if !is_retryable(&error) {
                    return Err(error);
                }
                if let Some(max_attempts) = max_attempts {
                    if attempt >= max_attempts {
                        return Err(error);
                    }
                }
                warn!(attempt, ?backoff, %error, "retrying operation");
                context.sleep(backoff).await;
                backoff = cmp::min(backoff.saturating_mul(2), max_backoff);
            }
        }
    }
}

async fn retry_until_some<C, F, Fut, T>(
    context: &C,
    initial_backoff: Duration,
    max_backoff: Duration,
    max_attempts: Option<u32>,
    mut f: F,
) -> Result<T>
where
    C: Clock,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<T>>>,
{
    let mut attempt = 0u32;
    let mut backoff = initial_backoff;

    loop {
        attempt = attempt.saturating_add(1);
        match f().await {
            Ok(Some(value)) => return Ok(value),
            Ok(None) => {
                if let Some(max_attempts) = max_attempts {
                    if attempt >= max_attempts {
                        return Err(Error::Timeout);
                    }
                }
            }
            Err(error) => {
                if !is_retryable(&error) {
                    return Err(error);
                }
                if let Some(max_attempts) = max_attempts {
                    if attempt >= max_attempts {
                        return Err(error);
                    }
                }
            }
        }
        context.sleep(backoff).await;
        backoff = cmp::min(backoff.saturating_mul(2), max_backoff);
    }
}

fn is_retryable(error: &Error) -> bool {
    matches!(
        error,
        Error::NotFound
            | Error::CelestiaSubmit(_)
            | Error::CelestiaRead(_)
            | Error::InclusionVerificationFailed
            | Error::Timeout
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use commonware_runtime::{deterministic, Runner};

    use super::*;
    use crate::{
        backend::MockBackend,
        config::{ChainConfig, RetryConfig},
        store::{PendingRecord, PendingStage, ReceiptRecord, Store},
        types::{NamespaceId, SubmissionId},
    };

    fn user_namespace() -> NamespaceId {
        let mut ns = [0u8; 29];
        ns[19..].copy_from_slice(b"namespace1");
        NamespaceId(ns)
    }

    fn config() -> PublisherConfig {
        PublisherConfig {
            chain: ChainConfig {
                namespace: user_namespace(),
                max_payload_bytes: 1024,
            },
            partition: "publisher".into(),
            retry: RetryConfig {
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(8),
                max_attempts: Some(5),
            },
            readback_timeout: Duration::from_secs(1),
            tx: Default::default(),
        }
    }

    #[test]
    fn recover_after_crash_before_submit() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let store = Store::new(context.clone(), 1024, "publisher".into());
            store.ensure_initialized().await.unwrap();
            store
                .write_pending(&PendingRecord::new(
                    SubmissionId([4; 32]),
                    Bytes::from_static(b"payload"),
                ))
                .await
                .unwrap();

            let publisher = Publisher::<deterministic::Context, _>::from_shared(
                context,
                backend,
                config(),
            );
            let receipts = publisher.recover_pending().await.unwrap();
            assert_eq!(receipts.len(), 1);
            assert_eq!(receipts[0].blob_ref.height, 1);
        });
    }

    #[test]
    fn recover_after_crash_after_broadcast_before_receipt() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let payload = Bytes::from_static(b"payload");
            let submission = backend
                .broadcast(user_namespace(), payload.clone(), Default::default())
                .await
                .unwrap();

            let store = Store::new(context.clone(), 1024, "publisher".into());
            store.ensure_initialized().await.unwrap();
            store
                .write_pending(&PendingRecord {
                    id: SubmissionId([5; 32]),
                    payload: payload.clone(),
                    payload_hash: payload_hash(payload.as_ref()),
                    stage: PendingStage::Broadcasted(submission.clone()),
                })
                .await
                .unwrap();

            let publisher = Publisher::<deterministic::Context, _>::from_shared(
                context,
                backend,
                config(),
            );
            let receipts = publisher.recover_pending().await.unwrap();
            assert_eq!(receipts.len(), 1);
            assert_eq!(receipts[0].blob_ref.height, 1);
            assert_eq!(receipts[0].tx_hash, submission.tx_hash);
        });
    }

    #[test]
    fn recover_after_crash_after_receipt_before_pending_delete() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let payload = Bytes::from_static(b"payload");
            let submission = backend
                .broadcast(user_namespace(), payload.clone(), Default::default())
                .await
                .unwrap();
            let submitted = backend
                .confirm(
                    user_namespace(),
                    payload.clone(),
                    Default::default(),
                    &submission,
                )
                .await
                .unwrap()
                .unwrap();

            let store = Store::new(context.clone(), 1024, "publisher".into());
            store.ensure_initialized().await.unwrap();
            let id = SubmissionId([6; 32]);
            store
                .write_pending(&PendingRecord {
                    id,
                    payload: payload.clone(),
                    payload_hash: payload_hash(payload.as_ref()),
                    stage: PendingStage::Broadcasted(submission.clone()),
                })
                .await
                .unwrap();
            store
                .write_receipt(&ReceiptRecord::from((
                    PublishReceipt {
                        id,
                        blob_ref: submitted.blob_ref,
                        tx_hash: submitted.tx_hash,
                    },
                    payload_hash(payload.as_ref()),
                )))
                .await
                .unwrap();

            let publisher = Publisher::<deterministic::Context, _>::from_shared(
                context.clone(),
                backend,
                config(),
            );
            let receipts = publisher.recover_pending().await.unwrap();
            assert_eq!(receipts.len(), 1);
            assert_eq!(receipts[0].blob_ref.height, 1);
            assert!(store.pending_ids().await.unwrap().is_empty());
        });
    }

    #[test]
    fn payload_size_limit() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let publisher = Publisher::<deterministic::Context, _>::from_shared(
                context.clone(),
                backend,
                config(),
            );
            let request = PublishRequest {
                id: SubmissionId([1; 32]),
                payload: Bytes::from(vec![0u8; 1025]),
            };
            assert!(matches!(publisher.publish(request).await, Err(Error::PayloadTooLarge)));
        });
    }

    #[test]
    fn idempotent_same_payload_returns_same_receipt() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let publisher = Publisher::<deterministic::Context, _>::from_shared(
                context.clone(),
                backend,
                config(),
            );
            let request = PublishRequest {
                id: SubmissionId([1; 32]),
                payload: Bytes::from_static(b"payload"),
            };
            let first = publisher.publish(request.clone()).await.unwrap();
            let second = publisher.publish(request).await.unwrap();
            assert_eq!(first, second);
        });
    }

    #[test]
    fn conflicting_same_id_is_rejected() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let publisher = Publisher::<deterministic::Context, _>::from_shared(
                context.clone(),
                backend,
                config(),
            );
            publisher
                .publish(PublishRequest {
                    id: SubmissionId([1; 32]),
                    payload: Bytes::from_static(b"payload-a"),
                })
                .await
                .unwrap();

            let err = publisher
                .publish(PublishRequest {
                    id: SubmissionId([1; 32]),
                    payload: Bytes::from_static(b"payload-b"),
                })
                .await
                .unwrap_err();
            assert!(matches!(err, Error::SubmissionConflict));
        });
    }

    #[test]
    fn retry_and_recovery_with_mock_backend() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            backend.fail_next_submits(1);
            let publisher = Publisher::<deterministic::Context, _>::from_shared(
                context.clone(),
                backend.clone(),
                config(),
            );
            let request = PublishRequest {
                id: SubmissionId([9; 32]),
                payload: Bytes::from_static(b"payload"),
            };
            let receipt = publisher.publish(request).await.unwrap();
            assert_eq!(receipt.blob_ref.height, 1);

            let second = Publisher::<deterministic::Context, _>::from_shared(
                context.clone(),
                backend,
                config(),
            );
            let recovered = second.recover_pending().await.unwrap();
            assert!(recovered.is_empty());
        });
    }

    #[test]
    fn publish_retry_exhaustion_returns_error() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            backend.fail_next_submits(4);
            let mut cfg = config();
            cfg.retry.max_attempts = Some(2);
            let publisher = Publisher::<deterministic::Context, _>::from_shared(
                context,
                backend,
                cfg,
            );
            let err = publisher
                .publish(PublishRequest {
                    id: SubmissionId([7; 32]),
                    payload: Bytes::from_static(b"payload"),
                })
                .await
                .unwrap_err();
            assert!(matches!(err, Error::CelestiaSubmit(_)));
        });
    }

    #[test]
    fn confirm_retry_exhaustion_returns_error() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let payload = Bytes::from_static(b"payload");
            let submission = backend
                .broadcast(user_namespace(), payload.clone(), Default::default())
                .await
                .unwrap();
            backend.fail_next_confirms(submission.tx_hash, 3);

            let store = Store::new(context.clone(), 1024, "publisher".into());
            store.ensure_initialized().await.unwrap();
            store
                .write_pending(&PendingRecord {
                    id: SubmissionId([8; 32]),
                    payload: payload.clone(),
                    payload_hash: payload_hash(payload.as_ref()),
                    stage: PendingStage::Broadcasted(submission),
                })
                .await
                .unwrap();

            let mut cfg = config();
            cfg.retry.max_attempts = Some(2);
            let publisher = Publisher::<deterministic::Context, _>::from_shared(
                context,
                backend,
                cfg,
            );
            let err = publisher.recover_pending().await.unwrap_err();
            assert!(matches!(err, Error::CelestiaSubmit(_)));
        });
    }
}
