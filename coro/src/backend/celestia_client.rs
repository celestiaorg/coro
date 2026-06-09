use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use ::celestia_client::{tx::TxConfig, Client};
use ::celestia_grpc::GrpcClient;
use ::celestia_grpc::grpc::{BroadcastMode, TxStatus};
use ::celestia_client::types::Blob as CelestiaBlob;
use ::celestia_client::types::hash::Hash;

use crate::{
    backend::{Backend, BroadcastedSubmission, SubmittedBlob},
    config::VerificationMode,
    error::Result,
    types::{celestia_commitment, validate_commitment, BlobRef, NamespaceId, Verification},
    Error,
};

#[derive(Clone)]
pub struct CelestiaClientBackend {
    client: Arc<Client>,
    submit_client: Option<Arc<GrpcClient>>,
}

impl CelestiaClientBackend {
    pub fn new(client: Client) -> Self {
        Self {
            client: Arc::new(client),
            submit_client: None,
        }
    }

    pub fn with_submit_client(client: Client, submit_client: GrpcClient) -> Self {
        Self {
            client: Arc::new(client),
            submit_client: Some(Arc::new(submit_client)),
        }
    }

    fn submit_client(&self) -> Result<&GrpcClient> {
        self.submit_client
            .as_deref()
            .ok_or_else(|| Error::CelestiaSubmit("submit client not configured".into()))
    }

    fn tendermint_hash(tx_hash: [u8; 32]) -> Hash {
        Hash::Sha256(tx_hash)
    }
}

#[async_trait]
impl Backend for CelestiaClientBackend {
    async fn broadcast(
        &self,
        namespace: NamespaceId,
        payload: Bytes,
        tx_config: TxConfig,
    ) -> Result<BroadcastedSubmission> {
        let namespace_value = namespace.to_celestia()?;
        let blob = CelestiaBlob::new(namespace_value, payload.to_vec(), None)
            .map_err(|err| Error::CelestiaSubmit(err.to_string()))?;
        let submitted_tx = self
            .submit_client()?
            .broadcast_blobs(&[blob], tx_config)
            .await
            .map_err(|err| Error::CelestiaSubmit(err.to_string()))?;
        let mut tx_hash = [0u8; 32];
        tx_hash.copy_from_slice(submitted_tx.tx_ref().hash.as_ref());
        Ok(BroadcastedSubmission {
            tx_hash,
            tx_bytes: Bytes::from(submitted_tx.tx_ref().tx.clone()),
        })
    }

    async fn confirm(
        &self,
        namespace: NamespaceId,
        payload: Bytes,
        _tx_config: TxConfig,
        submission: &BroadcastedSubmission,
    ) -> Result<Option<SubmittedBlob>> {
        let tx_hash = Self::tendermint_hash(submission.tx_hash);
        let tx_status = self
            .submit_client()?
            .tx_status(tx_hash)
            .await
            .map_err(|err| Error::CelestiaSubmit(err.to_string()))?;

        match tx_status.status {
            TxStatus::Pending | TxStatus::Unknown => Ok(None),
            TxStatus::Committed => {
                if tx_status.execution_code != ::celestia_client::types::state::ErrorCode::Success {
                    return Err(Error::CelestiaSubmit(tx_status.error));
                }
                Ok(Some(SubmittedBlob {
                    blob_ref: BlobRef {
                        height: tx_status.height.value(),
                        namespace,
                        commitment: crate::types::compute_commitment(namespace, payload.as_ref())?,
                    },
                    tx_hash: submission.tx_hash,
                }))
            }
            TxStatus::Rejected => Err(Error::CelestiaSubmit(tx_status.error)),
            TxStatus::Evicted => {
                match self
                    .submit_client()?
                    .broadcast_tx(submission.tx_bytes.to_vec(), BroadcastMode::Sync)
                    .await
                {
                    Ok(_) => Ok(None),
                    Err(::celestia_grpc::Error::TxBroadcastFailed(
                        _,
                        ::celestia_client::types::state::ErrorCode::TxInMempoolCache,
                        _,
                    )) => Ok(None),
                    Err(err) => Err(Error::CelestiaSubmit(err.to_string())),
                }
            }
        }
    }

    async fn get(&self, blob_ref: BlobRef) -> Result<Bytes> {
        let blob = self
            .client
            .blob()
            .get(
                blob_ref.height,
                blob_ref.namespace.to_celestia()?,
                celestia_commitment(blob_ref.commitment),
            )
            .await
            .map_err(|err| Error::CelestiaRead(err.to_string()))?;
        validate_commitment(blob_ref, &blob.data)?;
        Ok(Bytes::from(blob.data))
    }

    async fn verify(&self, blob_ref: BlobRef, mode: VerificationMode) -> Result<Verification> {
        match mode {
            VerificationMode::RpcIncluded => Ok(Verification::RpcIncluded),
            VerificationMode::ProofRequired => {
                let namespace = blob_ref.namespace.to_celestia()?;
                let header = self
                    .client
                    .header()
                    .get_by_height(blob_ref.height)
                    .await
                    .map_err(|err| Error::CelestiaRead(err.to_string()))?;
                let blob = self
                    .client
                    .blob()
                    .get(
                        blob_ref.height,
                        namespace,
                        celestia_commitment(blob_ref.commitment),
                    )
                    .await
                    .map_err(|err| Error::CelestiaRead(err.to_string()))?;
                let Some(index) = blob.index else {
                    return Err(Error::InclusionVerificationFailed);
                };
                let shares = self
                    .client
                    .share()
                    .get_range(blob_ref.height, index, index + blob.shares_len() as u64)
                    .await
                    .map_err(|err| Error::CelestiaRead(err.to_string()))?;
                shares
                    .proof
                    .verify(header.dah.hash())
                    .map_err(|_| Error::InclusionVerificationFailed)?;
                let reconstructed = CelestiaBlob::reconstruct(shares.shares.iter())
                .map_err(|_| Error::InclusionVerificationFailed)?;
                reconstructed
                    .validate_with_commitment(&blob.commitment)
                    .map_err(|_| Error::InclusionVerificationFailed)?;
                Ok(Verification::ProofVerified)
            }
        }
    }
}
