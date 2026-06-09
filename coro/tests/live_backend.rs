use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use celestia_client::Client;
use celestia_grpc::GrpcClient;
use commonware_codec::{FixedSize, Read, ReadExt, Write};
use coro::{
    backend::CelestiaClientBackend,
    single_sequencer::{Application, BatchNumber, BatchPolicy, ExecutedBatch, SequencerConfig, SingleSequencer},
    ChainConfig, Publisher, PublisherConfig, Reader, ReaderConfig, RetryConfig, VerificationMode,
};
use commonware_runtime::{tokio as runtime_tokio, Runner, Supervisor};
use commonware_runtime::{Buf, BufMut};

fn required_env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

fn namespace() -> coro::NamespaceId {
    let mut ns = [0u8; 29];
    ns[19..].copy_from_slice(b"cwcelestia");
    coro::NamespaceId(ns)
}

fn unique_payload() -> Bytes {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .to_be_bytes();
    Bytes::from(nanos.to_vec())
}

async fn clients_from_env() -> Option<(Client, GrpcClient)> {
    let rpc_url = required_env("CELESTIA_RPC_URL")?;
    let grpc_url = required_env("CELESTIA_GRPC_URL")?;
    let private_key_hex = required_env("CELESTIA_PRIVATE_KEY_HEX")?;

    let client = Client::builder()
        .rpc_url(&rpc_url)
        .grpc_url(&grpc_url)
        .private_key_hex(&private_key_hex)
        .build()
        .await
        .expect("build celestia-client");
    let grpc = GrpcClient::builder()
        .url(grpc_url)
        .private_key_hex(&private_key_hex)
        .build()
        .expect("build celestia-grpc client");
    Some((client, grpc))
}

fn publisher_config() -> PublisherConfig {
    PublisherConfig {
        chain: ChainConfig {
            namespace: namespace(),
            max_payload_bytes: 1024,
        },
        partition: "live-backend".into(),
        retry: RetryConfig {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
            max_attempts: Some(20),
        },
        readback_timeout: Duration::from_secs(30),
        tx: Default::default(),
    }
}

fn reader_config(verification: VerificationMode) -> ReaderConfig {
    ReaderConfig {
        chain: ChainConfig {
            namespace: namespace(),
            max_payload_bytes: 1024,
        },
        verification,
        read_timeout: Duration::from_secs(30),
    }
}

#[derive(Clone)]
struct LiveMetadata {
    txs: u64,
}

impl Write for LiveMetadata {
    fn write(&self, buf: &mut impl BufMut) {
        self.txs.write(buf);
    }
}

impl FixedSize for LiveMetadata {
    const SIZE: usize = u64::SIZE;
}

impl Read for LiveMetadata {
    type Cfg = ();

    fn read_cfg(buf: &mut impl Buf, _: &()) -> Result<Self, commonware_codec::Error> {
        Ok(Self { txs: u64::read(buf)? })
    }
}

struct LiveApp;

#[async_trait::async_trait]
impl Application for LiveApp {
    type Tx = u64;
    type Metadata = LiveMetadata;
    type Error = String;

    async fn execute_batch(
        &mut self,
        _sequence: BatchNumber,
        txs: Vec<Self::Tx>,
    ) -> Result<ExecutedBatch<Self::Metadata>, Self::Error> {
        let mut payload = Vec::with_capacity(txs.len() * 8);
        for tx in &txs {
            payload.extend_from_slice(&tx.to_be_bytes());
        }
        Ok(ExecutedBatch {
            payload: Bytes::from(payload),
            metadata: LiveMetadata { txs: txs.len() as u64 },
        })
    }
}

fn sequencer_config() -> SequencerConfig {
    SequencerConfig {
        partition: "live-sequencer".into(),
        batch_policy: BatchPolicy {
            max_txs: 2,
            max_payload_bytes: 1024,
            max_delay: Duration::from_secs(1),
        },
        max_tx_bytes: 32,
        max_metadata_bytes: 32,
    }
}

#[ignore = "requires local Celestia RPC/gRPC endpoints and signer env vars"]
#[test]
fn live_submit_read_and_verify() {
    let runner = runtime_tokio::Runner::default();
    runner.start(|context| async move {
        let Some((client, grpc)) = clients_from_env().await else {
            eprintln!("set CELESTIA_RPC_URL, CELESTIA_GRPC_URL, CELESTIA_PRIVATE_KEY_HEX to run");
            return;
        };

        let backend = CelestiaClientBackend::with_submit_client(client, grpc);
        let payload = unique_payload();
        let request = coro::PublishRequest {
            id: coro::SubmissionId([11; 32]),
            payload: payload.clone(),
        };

        let publisher = Publisher::new(context.child("publisher"), backend.clone(), publisher_config());
        let receipt = publisher.publish(request).await.expect("publish blob");

        let rpc_reader = Reader::new(
            context.child("reader_rpc"),
            backend.clone(),
            reader_config(VerificationMode::RpcIncluded),
        );
        let rpc_blob = rpc_reader.read(receipt.blob_ref).await.expect("rpc-included read");
        assert_eq!(rpc_blob.payload, payload);
        assert_eq!(rpc_blob.verification, coro::Verification::RpcIncluded);

        let proof_reader = Reader::new(
            context.child("reader_proof"),
            backend,
            reader_config(VerificationMode::ProofRequired),
        );
        let proof_blob = proof_reader
            .read(receipt.blob_ref)
            .await
            .expect("proof-required read");
        assert_eq!(proof_blob.payload, payload);
        assert_eq!(proof_blob.verification, coro::Verification::ProofVerified);
    });
}

#[ignore = "requires local Celestia RPC/gRPC endpoints and signer env vars"]
#[test]
fn live_exact_ref_failure_on_wrong_commitment() {
    let runner = runtime_tokio::Runner::default();
    runner.start(|context| async move {
        let Some((client, grpc)) = clients_from_env().await else {
            eprintln!("set CELESTIA_RPC_URL, CELESTIA_GRPC_URL, CELESTIA_PRIVATE_KEY_HEX to run");
            return;
        };

        let backend = CelestiaClientBackend::with_submit_client(client, grpc);
        let payload = unique_payload();
        let request = coro::PublishRequest {
            id: coro::SubmissionId([12; 32]),
            payload,
        };

        let publisher = Publisher::new(context.child("publisher"), backend.clone(), publisher_config());
        let receipt = publisher.publish(request).await.expect("publish blob");
        let reader = Reader::new(
            context.child("reader_proof"),
            backend,
            reader_config(VerificationMode::ProofRequired),
        );
        let wrong_ref = coro::BlobRef {
            commitment: coro::BlobCommitment([0u8; 32]),
            ..receipt.blob_ref
        };
        let err = reader.read(wrong_ref).await.unwrap_err();
        assert!(matches!(
            err,
            coro::Error::NotFound | coro::Error::CommitmentMismatch | coro::Error::CelestiaRead(_)
        ));
    });
}

#[ignore = "requires local Celestia RPC/gRPC endpoints and signer env vars"]
#[test]
fn live_single_sequencer_submit_archive_and_readback() {
    let runner = runtime_tokio::Runner::default();
    runner.start(|context| async move {
        let Some((client, grpc)) = clients_from_env().await else {
            eprintln!("set CELESTIA_RPC_URL, CELESTIA_GRPC_URL, CELESTIA_PRIVATE_KEY_HEX to run");
            return;
        };

        let backend = CelestiaClientBackend::with_submit_client(client, grpc);
        let sequencer = SingleSequencer::new(
            context.child("sequencer"),
            backend.clone(),
            publisher_config(),
            reader_config(VerificationMode::ProofRequired),
            sequencer_config(),
            LiveApp,
        );

        sequencer.submit(11).await.expect("submit tx 11");
        sequencer.submit(12).await.expect("submit tx 12");
        let cursor = sequencer
            .process_ready()
            .await
            .expect("process batch")
            .expect("batch cursor");
        assert_eq!(cursor.sequence, BatchNumber(0));

        let archived = sequencer
            .archived_batch(BatchNumber(0))
            .await
            .expect("archived batch")
            .expect("batch present");
        assert_eq!(archived.metadata.txs, 2);

        let payload = sequencer
            .payload(BatchNumber(0))
            .await
            .expect("payload")
            .expect("payload present");
        assert_eq!(payload.len(), 16);

        let restarted = SingleSequencer::new(
            context.child("sequencer_restart"),
            backend,
            publisher_config(),
            reader_config(VerificationMode::ProofRequired),
            sequencer_config(),
            LiveApp,
        );
        let report = restarted.recover().await.expect("recovery report");
        assert!(report.finalized_batches.iter().any(|batch| batch.sequence == BatchNumber(0)));
    });
}
