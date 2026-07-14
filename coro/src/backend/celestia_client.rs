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
        // Validate the node-returned transaction hash length before copying it into a
        // fixed 32-byte array. `copy_from_slice` panics on any length mismatch, and the
        // hash comes straight from the (untrusted) gRPC endpoint: a `code = 0` response
        // with an empty or short `txhash` would otherwise crash the process here.
        let hash_ref = submitted_tx.tx_ref().hash.as_ref();
        if hash_ref.len() != 32 {
            return Err(Error::CelestiaSubmit(format!(
                "node returned a transaction hash of {} bytes, expected 32",
                hash_ref.len()
            )));
        }
        let mut tx_hash = [0u8; 32];
        tx_hash.copy_from_slice(hash_ref);
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
                // `index` and `shares_len` are derived from the node-supplied blob, so
                // compute the range end with `checked_add` instead of `+`: a crafted
                // response could otherwise overflow the `u64` (panicking in debug builds,
                // wrapping to a bogus range in release) before the range is even fetched.
                let range_end = index
                    .checked_add(blob.shares_len() as u64)
                    .ok_or(Error::InclusionVerificationFailed)?;
                let shares = self
                    .client
                    .share()
                    .get_range(blob_ref.height, index, range_end)
                    .await
                    .map_err(|err| Error::CelestiaRead(err.to_string()))?;
                // Contain panics from the untrusted NMT proof verifier. nmt-rs 0.2.5
                // panics (rather than returning `Err`) when a proof carries out-of-order
                // namespace siblings, so a malicious `share.GetRange` response could crash
                // the process here before the recomputed root is ever compared to the
                // trusted DAH root. `catch_unwind` converts that panic into the same
                // `InclusionVerificationFailed` a well-formed rejection produces, which
                // preserves `ProofRequired` semantics (malformed proof -> error, not crash).
                let dah_hash = header.dah.hash();
                let proof = shares.proof.clone();
                let verify_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    move || proof.verify(dah_hash),
                ))
                .map_err(|_| Error::InclusionVerificationFailed)?;
                verify_result.map_err(|_| Error::InclusionVerificationFailed)?;
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
