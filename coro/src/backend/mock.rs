use std::{collections::BTreeMap, sync::{Arc, Mutex}};

use async_trait::async_trait;
use bytes::Bytes;
use celestia_client::tx::TxConfig;

use crate::{
    backend::{Backend, BroadcastedSubmission, SubmittedBlob},
    config::VerificationMode,
    error::Result,
    types::{compute_commitment, BlobRef, NamespaceId, Verification},
    Error,
};

#[derive(Clone, Default)]
pub struct MockBackend {
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    blobs: BTreeMap<BlobRef, Bytes>,
    broadcasts: BTreeMap<[u8; 32], (NamespaceId, Bytes, SubmittedBlob)>,
    submit_failures_remaining: u32,
    confirm_failures_remaining: BTreeMap<[u8; 32], u32>,
    get_failures_remaining: BTreeMap<BlobRef, u32>,
    verify_failures_remaining: BTreeMap<(BlobRef, VerificationMode), u32>,
    next_height: u64,
    next_tx_hash: u64,
    proof_supported: bool,
}

impl MockBackend {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_proof_verification(self, enabled: bool) -> Self {
        self.state.lock().unwrap().proof_supported = enabled;
        self
    }

    pub fn fail_next_submits(&self, failures: u32) {
        self.state.lock().unwrap().submit_failures_remaining = failures;
    }

    pub fn fail_next_gets(&self, blob_ref: BlobRef, failures: u32) {
        self.state
            .lock()
            .unwrap()
            .get_failures_remaining
            .insert(blob_ref, failures);
    }

    pub fn fail_next_confirms(&self, tx_hash: [u8; 32], failures: u32) {
        self.state
            .lock()
            .unwrap()
            .confirm_failures_remaining
            .insert(tx_hash, failures);
    }

    pub fn fail_next_verifies(&self, blob_ref: BlobRef, mode: VerificationMode, failures: u32) {
        self.state
            .lock()
            .unwrap()
            .verify_failures_remaining
            .insert((blob_ref, mode), failures);
    }

    pub fn insert_blob(&self, blob_ref: BlobRef, payload: Bytes) {
        self.state.lock().unwrap().blobs.insert(blob_ref, payload);
    }
}

#[async_trait]
impl Backend for MockBackend {
    async fn broadcast(
        &self,
        namespace: NamespaceId,
        payload: Bytes,
        _tx_config: TxConfig,
    ) -> Result<BroadcastedSubmission> {
        let mut state = self.state.lock().unwrap();
        if state.submit_failures_remaining > 0 {
            state.submit_failures_remaining -= 1;
            return Err(Error::CelestiaSubmit("mock submit failure".into()));
        }

        state.next_height += 1;
        state.next_tx_hash += 1;
        let mut tx_hash = [0u8; 32];
        tx_hash[24..].copy_from_slice(&state.next_tx_hash.to_be_bytes());

        let blob_ref = BlobRef {
            height: state.next_height,
            namespace,
            commitment: compute_commitment(namespace, payload.as_ref())?,
        };
        let submitted = SubmittedBlob { blob_ref, tx_hash };
        state.blobs.insert(blob_ref, payload.clone());
        state
            .broadcasts
            .insert(tx_hash, (namespace, payload, submitted.clone()));
        Ok(BroadcastedSubmission {
            tx_hash,
            tx_bytes: Bytes::from_static(b"mock-broadcasted-tx"),
        })
    }

    async fn confirm(
        &self,
        namespace: NamespaceId,
        payload: Bytes,
        _tx_config: TxConfig,
        submission: &BroadcastedSubmission,
    ) -> Result<Option<SubmittedBlob>> {
        let mut state = self.state.lock().unwrap();
        if let Some(remaining) = state.confirm_failures_remaining.get_mut(&submission.tx_hash) {
            if *remaining > 0 {
                *remaining -= 1;
                return Err(Error::CelestiaSubmit("mock confirm failure".into()));
            }
        }
        let Some((stored_namespace, stored_payload, submitted)) =
            state.broadcasts.get(&submission.tx_hash).cloned()
        else {
            return Err(Error::NotFound);
        };
        if stored_namespace != namespace || stored_payload != payload {
            return Err(Error::SubmissionConflict);
        }
        Ok(Some(submitted))
    }

    async fn get(&self, blob_ref: BlobRef) -> Result<Bytes> {
        let mut state = self.state.lock().unwrap();
        if let Some(remaining) = state.get_failures_remaining.get_mut(&blob_ref) {
            if *remaining > 0 {
                *remaining -= 1;
                return Err(Error::CelestiaRead("mock get failure".into()));
            }
        }

        state.blobs.get(&blob_ref).cloned().ok_or(Error::NotFound)
    }

    async fn verify(&self, blob_ref: BlobRef, mode: VerificationMode) -> Result<Verification> {
        let mut state = self.state.lock().unwrap();
        if let Some(remaining) = state.verify_failures_remaining.get_mut(&(blob_ref, mode)) {
            if *remaining > 0 {
                *remaining -= 1;
                return Err(Error::InclusionVerificationFailed);
            }
        }
        if !state.blobs.contains_key(&blob_ref) {
            return Err(Error::NotFound);
        }

        match mode {
            VerificationMode::RpcIncluded => Ok(Verification::RpcIncluded),
            VerificationMode::ProofRequired if state.proof_supported => {
                Ok(Verification::ProofVerified)
            }
            VerificationMode::ProofRequired => Err(Error::UnsupportedVerificationMode),
        }
    }
}
