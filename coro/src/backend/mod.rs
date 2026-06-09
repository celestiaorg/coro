use async_trait::async_trait;
use bytes::Bytes;
use ::celestia_client::tx::TxConfig;

use crate::{config::VerificationMode, error::Result, types::{BlobRef, NamespaceId, Verification}};

pub mod celestia_client;
pub mod mock;

pub use celestia_client::CelestiaClientBackend;
pub use mock::MockBackend;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubmittedBlob {
    pub blob_ref: BlobRef,
    pub tx_hash: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BroadcastedSubmission {
    pub tx_hash: [u8; 32],
    pub tx_bytes: Bytes,
}

#[async_trait]
pub trait Backend: Clone + Send + Sync + 'static {
    async fn broadcast(
        &self,
        namespace: NamespaceId,
        payload: Bytes,
        tx_config: TxConfig,
    ) -> Result<BroadcastedSubmission>;

    async fn confirm(
        &self,
        namespace: NamespaceId,
        payload: Bytes,
        tx_config: TxConfig,
        submission: &BroadcastedSubmission,
    ) -> Result<Option<SubmittedBlob>>;

    async fn get(&self, blob_ref: BlobRef) -> Result<Bytes>;

    async fn verify(&self, blob_ref: BlobRef, mode: VerificationMode) -> Result<Verification>;
}
