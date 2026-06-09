//! Minimal HTTP demo control plane for `coro` single-sequencer replicas.
//!
//! This crate keeps HTTP server/client plumbing outside the core `coro` crate.
//! It transports only:
//! - canonical published sequencer head
//! - canonical `BatchCursor` by sequence
//! - optional payload bytes by sequence
//! - optional operator-facing archived head and batch status
//!
//! Replicas still trust sequencer-defined canonical order, prefer local or served
//! payloads when available, and fall back to Celestia exact-ref reads through
//! `coro::Replica` when payload bytes are not served.
//! Archived-only batches are local soft confirmations, not replica-visible head.

use std::{net::SocketAddr, sync::Arc};

use async_trait::async_trait;
use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use bytes::Bytes;
use coro::{BatchCursor, BatchNumber, ReplicaSource, SingleSequencer};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default)]
pub struct HistoryServerConfig {
    pub serve_payloads: bool,
}

#[async_trait]
pub trait SequencerHistory: Send + Sync + 'static {
    async fn head(&self) -> coro::Result<Option<BatchNumber>>;
    async fn archived_head(&self) -> coro::Result<Option<BatchNumber>>;
    async fn cursor(&self, sequence: BatchNumber) -> coro::Result<Option<BatchCursor>>;
    async fn status(&self, sequence: BatchNumber) -> coro::Result<Option<coro::BatchStatus>>;
    async fn payload(&self, sequence: BatchNumber) -> coro::Result<Option<Bytes>>;
}

#[async_trait]
impl<C, B, A> SequencerHistory for SingleSequencer<C, B, A>
where
    C: commonware_runtime::Clock + commonware_runtime::Storage + Send + Sync + 'static,
    B: coro::backend::Backend,
    A: coro::single_sequencer::Application,
{
    async fn head(&self) -> coro::Result<Option<BatchNumber>> {
        SingleSequencer::head(self).await.map_err(|err| match err {
            coro::single_sequencer::SequencerError::Core(core) => core,
            coro::single_sequencer::SequencerError::Application(_) => {
                coro::Error::Decode("sequencer application error while serving head".into())
            }
        })
    }

    async fn archived_head(&self) -> coro::Result<Option<BatchNumber>> {
        SingleSequencer::archived_head(self)
            .await
            .map_err(|err| match err {
                coro::single_sequencer::SequencerError::Core(core) => core,
                coro::single_sequencer::SequencerError::Application(_) => {
                    coro::Error::Decode("sequencer application error while serving archived head".into())
                }
            })
    }

    async fn cursor(&self, sequence: BatchNumber) -> coro::Result<Option<BatchCursor>> {
        SingleSequencer::batch_cursor(self, sequence)
            .await
            .map_err(|err| match err {
                coro::single_sequencer::SequencerError::Core(core) => core,
                coro::single_sequencer::SequencerError::Application(_) => {
                    coro::Error::Decode("sequencer application error while serving cursor".into())
                }
            })
    }

    async fn status(&self, sequence: BatchNumber) -> coro::Result<Option<coro::BatchStatus>> {
        SingleSequencer::status(self, sequence)
            .await
            .map_err(|err| match err {
                coro::single_sequencer::SequencerError::Core(core) => core,
                coro::single_sequencer::SequencerError::Application(_) => {
                    coro::Error::Decode("sequencer application error while serving status".into())
                }
            })
    }

    async fn payload(&self, sequence: BatchNumber) -> coro::Result<Option<Bytes>> {
        SingleSequencer::payload(self, sequence).await.map_err(|err| match err {
            coro::single_sequencer::SequencerError::Core(core) => core,
            coro::single_sequencer::SequencerError::Application(_) => {
                coro::Error::Decode("sequencer application error while serving payload".into())
            }
        })
    }
}

#[derive(Clone)]
struct AppState {
    history: Arc<dyn SequencerHistory>,
    config: HistoryServerConfig,
}

pub fn sequencer_router(history: Arc<dyn SequencerHistory>, config: HistoryServerConfig) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/head", get(head))
        .route("/archived-head", get(archived_head))
        .route("/cursor/:sequence", get(cursor))
        .route("/status/:sequence", get(status))
        .route("/payload/:sequence", get(payload))
        .with_state(AppState { history, config })
}

pub async fn serve(history: Arc<dyn SequencerHistory>, config: HistoryServerConfig, bind: SocketAddr) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, sequencer_router(history, config)).await
}

async fn healthz() -> &'static str {
    "ok"
}

async fn head(State(state): State<AppState>) -> Response {
    match state.history.head().await {
        Ok(head) => Json(HeadResponse {
            head: head.map(|number| number.0),
        })
        .into_response(),
        Err(err) => server_error(err),
    }
}

async fn archived_head(State(state): State<AppState>) -> Response {
    match state.history.archived_head().await {
        Ok(head) => Json(HeadResponse {
            head: head.map(|number| number.0),
        })
        .into_response(),
        Err(err) => server_error(err),
    }
}

async fn cursor(Path(sequence): Path<u64>, State(state): State<AppState>) -> Response {
    match state.history.cursor(BatchNumber(sequence)).await {
        Ok(Some(cursor)) => Json(CursorResponse::from(cursor)).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => server_error(err),
    }
}

async fn status(Path(sequence): Path<u64>, State(state): State<AppState>) -> Response {
    match state.history.status(BatchNumber(sequence)).await {
        Ok(Some(status)) => Json(StatusResponse::from(status)).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => server_error(err),
    }
}

async fn payload(Path(sequence): Path<u64>, State(state): State<AppState>) -> Response {
    if !state.config.serve_payloads {
        return StatusCode::NOT_FOUND.into_response();
    }
    match state.history.payload(BatchNumber(sequence)).await {
        Ok(Some(payload)) => {
            let mut response = Response::new(Body::from(payload));
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            );
            response
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => server_error(err),
    }
}

fn server_error(err: coro::Error) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
}

#[derive(Clone)]
pub struct HttpReplicaSource {
    client: reqwest::Client,
    base_url: String,
}

impl HttpReplicaSource {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
        }
    }

    pub fn with_client(base_url: impl Into<String>, client: reqwest::Client) -> Self {
        Self {
            client,
            base_url: base_url.into().trim_end_matches('/').to_string(),
        }
    }

    async fn get_json<T>(&self, path: &str) -> coro::Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let response = self
            .client
            .get(format!("{}{path}", self.base_url))
            .send()
            .await
            .map_err(|err| coro::Error::Decode(format!("http request failed: {err}")))?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = response
            .error_for_status()
            .map_err(|err| coro::Error::Decode(format!("http status error: {err}")))?;
        let parsed = response
            .json::<T>()
            .await
            .map_err(|err| coro::Error::Decode(format!("http json decode failed: {err}")))?;
        Ok(Some(parsed))
    }
}

#[async_trait]
impl ReplicaSource for HttpReplicaSource {
    async fn head(&self) -> coro::Result<Option<BatchNumber>> {
        let Some(response) = self.get_json::<HeadResponse>("/head").await? else {
            return Ok(None);
        };
        Ok(response.head.map(BatchNumber))
    }

    async fn cursor(&self, sequence: BatchNumber) -> coro::Result<Option<BatchCursor>> {
        let Some(response) = self
            .get_json::<CursorResponse>(&format!("/cursor/{}", sequence.0))
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(response.try_into()?))
    }

    async fn payload(&self, sequence: BatchNumber) -> coro::Result<Option<Bytes>> {
        let response = self
            .client
            .get(format!("{}/payload/{}", self.base_url, sequence.0))
            .send()
            .await
            .map_err(|err| coro::Error::Decode(format!("http request failed: {err}")))?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = response
            .error_for_status()
            .map_err(|err| coro::Error::Decode(format!("http status error: {err}")))?;
        let bytes = response
            .bytes()
            .await
            .map_err(|err| coro::Error::Decode(format!("http payload read failed: {err}")))?;
        Ok(Some(bytes))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct HeadResponse {
    head: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
enum StatusResponse {
    Archived,
    Published { cursor: CursorResponse },
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct CursorResponse {
    sequence: u64,
    blob_ref: BlobRefResponse,
    payload_hash: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct BlobRefResponse {
    height: u64,
    namespace: String,
    commitment: String,
}

impl From<BatchCursor> for CursorResponse {
    fn from(value: BatchCursor) -> Self {
        Self {
            sequence: value.sequence.0,
            blob_ref: BlobRefResponse {
                height: value.blob_ref.height,
                namespace: hex::encode(value.blob_ref.namespace.0),
                commitment: hex::encode(value.blob_ref.commitment.0),
            },
            payload_hash: hex::encode(value.payload_hash),
        }
    }
}

impl TryFrom<CursorResponse> for BatchCursor {
    type Error = coro::Error;

    fn try_from(value: CursorResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            sequence: BatchNumber(value.sequence),
            blob_ref: coro::BlobRef {
                height: value.blob_ref.height,
                namespace: coro::NamespaceId(decode_fixed::<29>(&value.blob_ref.namespace, "namespace")?),
                commitment: coro::BlobCommitment(decode_fixed::<32>(&value.blob_ref.commitment, "commitment")?),
            },
            payload_hash: decode_fixed::<32>(&value.payload_hash, "payload_hash")?,
        })
    }
}

impl From<coro::BatchStatus> for StatusResponse {
    fn from(value: coro::BatchStatus) -> Self {
        match value {
            coro::BatchStatus::Archived => Self::Archived,
            coro::BatchStatus::Published(cursor) => Self::Published {
                cursor: cursor.into(),
            },
        }
    }
}

fn decode_fixed<const N: usize>(value: &str, field: &str) -> coro::Result<[u8; N]> {
    let bytes = hex::decode(value)
        .map_err(|err| coro::Error::Decode(format!("invalid {field} hex: {err}")))?;
    if bytes.len() != N {
        return Err(coro::Error::Decode(format!(
            "invalid {field} length: expected {N}, got {}",
            bytes.len()
        )));
    }
    let mut array = [0u8; N];
    array.copy_from_slice(&bytes);
    Ok(array)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc, time::Duration};

    use async_trait::async_trait;
    use bytes::{Buf, BufMut};
    use commonware_codec::{FixedSize, Read, ReadExt, Write};
    use commonware_runtime::{tokio as runtime_tokio, Runner, Supervisor};
    use sha2::{Digest, Sha256};
    use tokio::net::TcpListener;

    use super::*;

    #[derive(Clone, Default)]
    struct StubHistory {
        head: Arc<tokio::sync::Mutex<Option<BatchNumber>>>,
        archived_head: Arc<tokio::sync::Mutex<Option<BatchNumber>>>,
        cursors: Arc<tokio::sync::Mutex<BTreeMap<BatchNumber, BatchCursor>>>,
        statuses: Arc<tokio::sync::Mutex<BTreeMap<BatchNumber, coro::BatchStatus>>>,
        payloads: Arc<tokio::sync::Mutex<BTreeMap<BatchNumber, Bytes>>>,
    }

    #[async_trait]
    impl SequencerHistory for StubHistory {
        async fn head(&self) -> coro::Result<Option<BatchNumber>> {
            Ok(*self.head.lock().await)
        }

        async fn archived_head(&self) -> coro::Result<Option<BatchNumber>> {
            Ok(*self.archived_head.lock().await)
        }

        async fn cursor(&self, sequence: BatchNumber) -> coro::Result<Option<BatchCursor>> {
            Ok(self.cursors.lock().await.get(&sequence).cloned())
        }

        async fn status(&self, sequence: BatchNumber) -> coro::Result<Option<coro::BatchStatus>> {
            Ok(self.statuses.lock().await.get(&sequence).cloned())
        }

        async fn payload(&self, sequence: BatchNumber) -> coro::Result<Option<Bytes>> {
            Ok(self.payloads.lock().await.get(&sequence).cloned())
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct DemoMetadata(u64);

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct DemoOutput(u64);

    impl Write for DemoMetadata {
        fn write(&self, buf: &mut impl BufMut) {
            self.0.write(buf);
        }
    }

    impl FixedSize for DemoMetadata {
        const SIZE: usize = u64::SIZE;
    }

    impl Read for DemoMetadata {
        type Cfg = ();

        fn read_cfg(buf: &mut impl Buf, _: &()) -> Result<Self, commonware_codec::Error> {
            Ok(Self(u64::read(buf)?))
        }
    }

    impl Write for DemoOutput {
        fn write(&self, buf: &mut impl BufMut) {
            self.0.write(buf);
        }
    }

    impl FixedSize for DemoOutput {
        const SIZE: usize = u64::SIZE;
    }

    impl Read for DemoOutput {
        type Cfg = ();

        fn read_cfg(buf: &mut impl Buf, _: &()) -> Result<Self, commonware_codec::Error> {
            Ok(Self(u64::read(buf)?))
        }
    }

    #[derive(Default)]
    struct DemoSequencerApp;

    #[async_trait]
    impl coro::single_sequencer::Application for DemoSequencerApp {
        type Tx = u64;
        type Metadata = DemoMetadata;
        type Error = String;

        async fn execute_batch(
            &mut self,
            _sequence: BatchNumber,
            txs: Vec<Self::Tx>,
        ) -> Result<coro::ExecutedBatch<Self::Metadata>, Self::Error> {
            let mut payload = Vec::with_capacity(txs.len() * 8);
            for tx in &txs {
                payload.extend_from_slice(&tx.to_be_bytes());
            }
            Ok(coro::ExecutedBatch {
                payload: Bytes::from(payload),
                metadata: DemoMetadata(txs.len() as u64),
            })
        }
    }

    #[derive(Clone, Default)]
    struct DemoReplicaApp {
        applied: Arc<tokio::sync::Mutex<Vec<BatchNumber>>>,
    }

    #[async_trait]
    impl coro::ReplicaApplication for DemoReplicaApp {
        type Metadata = DemoMetadata;
        type Output = DemoOutput;
        type Error = String;

        async fn apply_batch(
            &mut self,
            sequence: BatchNumber,
            payload: Bytes,
        ) -> Result<coro::ReplicaAppliedBatch<Self::Metadata, Self::Output>, Self::Error> {
            self.applied.lock().await.push(sequence);
            Ok(coro::ReplicaAppliedBatch {
                metadata: DemoMetadata(payload.len() as u64),
                output: DemoOutput(sequence.0),
            })
        }
    }

    fn namespace() -> coro::NamespaceId {
        let mut ns = [0u8; 29];
        ns[19..].copy_from_slice(b"http_demo_");
        coro::NamespaceId(ns)
    }

    fn publisher_config() -> coro::PublisherConfig {
        coro::PublisherConfig {
            chain: coro::ChainConfig {
                namespace: namespace(),
                max_payload_bytes: 1024,
            },
            partition: "http-demo-publisher".into(),
            retry: coro::RetryConfig {
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(8),
                max_attempts: Some(5),
            },
            readback_timeout: Duration::from_secs(1),
            tx: Default::default(),
        }
    }

    fn reader_config() -> coro::ReaderConfig {
        coro::ReaderConfig {
            chain: coro::ChainConfig {
                namespace: namespace(),
                max_payload_bytes: 1024,
            },
            verification: coro::VerificationMode::RpcIncluded,
            read_timeout: Duration::from_secs(1),
        }
    }

    fn sequencer_config() -> coro::SequencerConfig {
        coro::SequencerConfig {
            partition: "http-demo-sequencer".into(),
            batch_policy: coro::BatchPolicy {
                max_txs: 2,
                max_payload_bytes: 1024,
                max_delay: Duration::from_millis(1),
            },
            max_tx_bytes: 32,
            max_metadata_bytes: 32,
        }
    }

    fn replica_config() -> coro::ReplicaConfig {
        coro::ReplicaConfig {
            partition: "http-demo-replica".into(),
            max_payload_bytes: 1024,
            max_metadata_bytes: 32,
            max_output_bytes: 32,
        }
    }

    async fn spawn_server(router: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        format!("http://{addr}")
    }

    fn sample_cursor() -> BatchCursor {
        BatchCursor {
            sequence: BatchNumber(3),
            blob_ref: coro::BlobRef {
                height: 7,
                namespace: namespace(),
                commitment: coro::BlobCommitment([2; 32]),
            },
            payload_hash: payload_hash(b"payload"),
        }
    }

    fn payload_hash(payload: &[u8]) -> [u8; 32] {
        let digest = Sha256::digest(payload);
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&digest);
        hash
    }

    #[tokio::test]
    async fn head_response_parsing() {
        let history = StubHistory::default();
        *history.head.lock().await = Some(BatchNumber(4));
        let base = spawn_server(sequencer_router(Arc::new(history), HistoryServerConfig::default())).await;
        let source = HttpReplicaSource::new(base);
        assert_eq!(source.head().await.unwrap(), Some(BatchNumber(4)));
    }

    #[tokio::test]
    async fn archived_head_and_status_endpoints_behave() {
        let history = StubHistory::default();
        let cursor = sample_cursor();
        *history.head.lock().await = Some(BatchNumber(2));
        *history.archived_head.lock().await = Some(BatchNumber(3));
        history
            .statuses
            .lock()
            .await
            .insert(BatchNumber(3), coro::BatchStatus::Archived);
        history
            .statuses
            .lock()
            .await
            .insert(BatchNumber(2), coro::BatchStatus::Published(cursor.clone()));
        let base = spawn_server(sequencer_router(Arc::new(history), HistoryServerConfig::default())).await;
        let client = reqwest::Client::new();

        let archived_head: HeadResponse = client
            .get(format!("{base}/archived-head"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(archived_head.head, Some(3));

        let archived_status: StatusResponse = client
            .get(format!("{base}/status/3"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(archived_status, StatusResponse::Archived);

        let published_status: StatusResponse = client
            .get(format!("{base}/status/2"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            published_status,
            StatusResponse::Published {
                cursor: cursor.into()
            }
        );
    }

    #[tokio::test]
    async fn cursor_response_parsing() {
        let history = StubHistory::default();
        history.cursors.lock().await.insert(BatchNumber(3), sample_cursor());
        let base = spawn_server(sequencer_router(Arc::new(history), HistoryServerConfig::default())).await;
        let source = HttpReplicaSource::new(base);
        assert_eq!(source.cursor(BatchNumber(3)).await.unwrap(), Some(sample_cursor()));
    }

    #[tokio::test]
    async fn payload_present_and_absent_mapping() {
        let history = StubHistory::default();
        history.payloads.lock().await.insert(BatchNumber(1), Bytes::from_static(b"payload"));
        let base = spawn_server(sequencer_router(
            Arc::new(history),
            HistoryServerConfig { serve_payloads: true },
        ))
        .await;
        let source = HttpReplicaSource::new(base);
        assert_eq!(source.payload(BatchNumber(1)).await.unwrap(), Some(Bytes::from_static(b"payload")));
        assert_eq!(source.payload(BatchNumber(9)).await.unwrap(), None);
    }

    #[tokio::test]
    async fn cursor_404_returns_none() {
        let base = spawn_server(sequencer_router(Arc::new(StubHistory::default()), HistoryServerConfig::default())).await;
        let source = HttpReplicaSource::new(base);
        assert_eq!(source.cursor(BatchNumber(10)).await.unwrap(), None);
    }

    #[tokio::test]
    async fn malformed_response_handling() {
        let router = Router::new().route("/head", get(|| async { (StatusCode::OK, "not-json") }));
        let base = spawn_server(router).await;
        let source = HttpReplicaSource::new(base);
        let err = source.head().await.unwrap_err();
        assert!(matches!(err, coro::Error::Decode(_)));
    }

    #[test]
    fn replica_catches_up_through_http_source() {
        let runner = runtime_tokio::Runner::default();
        runner.start(|context| async move {
            let backend = coro::backend::MockBackend::new();
            let sequencer = Arc::new(coro::SingleSequencer::new(
                context.child("sequencer"),
                backend.clone(),
                publisher_config(),
                reader_config(),
                sequencer_config(),
                DemoSequencerApp,
            ));
            sequencer.submit(11).await.unwrap();
            sequencer.submit(12).await.unwrap();
            sequencer.flush().await.unwrap();

            let base = spawn_server(sequencer_router(
                sequencer as Arc<dyn SequencerHistory>,
                HistoryServerConfig { serve_payloads: true },
            ))
            .await;
            let replica_app = DemoReplicaApp::default();
            let applied = replica_app.applied.clone();
            let replica = coro::Replica::new(
                context.child("replica"),
                backend,
                reader_config(),
                replica_config(),
                replica_app,
            );
            let source = HttpReplicaSource::new(base);
            let batches = replica.catch_up(&source).await.unwrap();
            assert_eq!(batches.len(), 1);
            assert_eq!(*applied.lock().await, vec![BatchNumber(0)]);
        });
    }

    #[test]
    fn replica_falls_back_to_da_when_http_payload_missing() {
        let runner = runtime_tokio::Runner::default();
        runner.start(|context| async move {
            let backend = coro::backend::MockBackend::new();
            let sequencer = Arc::new(coro::SingleSequencer::new(
                context.child("sequencer"),
                backend.clone(),
                publisher_config(),
                reader_config(),
                sequencer_config(),
                DemoSequencerApp,
            ));
            sequencer.submit(21).await.unwrap();
            sequencer.submit(22).await.unwrap();
            let cursor = sequencer.flush().await.unwrap().unwrap();
            let base = spawn_server(sequencer_router(
                sequencer as Arc<dyn SequencerHistory>,
                HistoryServerConfig { serve_payloads: false },
            ))
            .await;
            let replica = coro::Replica::new(
                context.child("replica"),
                backend.clone(),
                reader_config(),
                replica_config(),
                DemoReplicaApp::default(),
            );
            backend.fail_next_gets(cursor.blob_ref, 0);
            let source = HttpReplicaSource::new(base);
            let batch = replica.sync_once(&source).await.unwrap().unwrap();
            assert_eq!(batch.sequence, BatchNumber(0));
        });
    }

    #[test]
    fn restart_recovery_still_works_with_http_source() {
        let runner = runtime_tokio::Runner::default();
        runner.start(|context| async move {
            let context = Arc::new(context);
            let backend = coro::backend::MockBackend::new();
            let sequencer = Arc::new(coro::SingleSequencer::new(
                context.child("sequencer"),
                backend.clone(),
                publisher_config(),
                reader_config(),
                sequencer_config(),
                DemoSequencerApp,
            ));
            sequencer.submit(31).await.unwrap();
            sequencer.submit(32).await.unwrap();
            sequencer.flush().await.unwrap();
            let base = spawn_server(sequencer_router(
                sequencer as Arc<dyn SequencerHistory>,
                HistoryServerConfig { serve_payloads: true },
            ))
            .await;
            let source = HttpReplicaSource::new(base);
            let first = coro::Replica::from_shared(
                context.clone(),
                coro::Reader::from_shared(context.clone(), backend.clone(), reader_config()),
                replica_config(),
                DemoReplicaApp::default(),
            );
            first.sync_once(&source).await.unwrap();

            let restarted = coro::Replica::from_shared(
                context.clone(),
                coro::Reader::from_shared(context, backend, reader_config()),
                replica_config(),
                DemoReplicaApp::default(),
            );
            let report = restarted.recover().await.unwrap();
            assert_eq!(report.next_sequence, BatchNumber(1));
            assert!(restarted.sync_once(&source).await.unwrap().is_none());
        });
    }

    #[test]
    fn payload_hash_mismatch_from_http_source_is_rejected() {
        let runner = runtime_tokio::Runner::default();
        runner.start(|context| async move {
            let backend = coro::backend::MockBackend::new();
            let sequencer = Arc::new(coro::SingleSequencer::new(
                context.child("sequencer"),
                backend.clone(),
                publisher_config(),
                reader_config(),
                sequencer_config(),
                DemoSequencerApp,
            ));
            sequencer.submit(41).await.unwrap();
            sequencer.submit(42).await.unwrap();
            let cursor = sequencer.flush().await.unwrap().unwrap();
            let history = StubHistory::default();
            *history.head.lock().await = Some(BatchNumber(0));
            history.cursors.lock().await.insert(BatchNumber(0), cursor);
            history.payloads.lock().await.insert(BatchNumber(0), Bytes::from_static(b"bad-payload"));
            let base = spawn_server(sequencer_router(
                Arc::new(history),
                HistoryServerConfig { serve_payloads: true },
            ))
            .await;
            let replica = coro::Replica::new(
                context.child("replica"),
                backend,
                reader_config(),
                replica_config(),
                DemoReplicaApp::default(),
            );
            let err = replica.sync_once(&HttpReplicaSource::new(base)).await.unwrap_err();
            assert!(matches!(err, coro::single_sequencer::SequencerError::Core(coro::Error::Decode(_))));
        });
    }

    #[test]
    fn source_head_ahead_of_missing_cursor_is_rejected() {
        let runner = runtime_tokio::Runner::default();
        runner.start(|context| async move {
            let replica = coro::Replica::new(
                context.child("replica"),
                coro::backend::MockBackend::new(),
                reader_config(),
                replica_config(),
                DemoReplicaApp::default(),
            );
            let history = StubHistory::default();
            *history.head.lock().await = Some(BatchNumber(0));
            let base = spawn_server(sequencer_router(
                Arc::new(history),
                HistoryServerConfig { serve_payloads: true },
            ))
            .await;
            let err = replica.sync_once(&HttpReplicaSource::new(base)).await.unwrap_err();
            assert!(matches!(err, coro::single_sequencer::SequencerError::Core(coro::Error::Decode(_))));
        });
    }

    #[test]
    fn da_fallback_failure_propagates() {
        let runner = runtime_tokio::Runner::default();
        runner.start(|context| async move {
            use coro::backend::Backend as _;

            let backend = coro::backend::MockBackend::new();
            let payload = Bytes::from_static(b"payload");
            let submission = backend
                .broadcast(namespace(), payload.clone(), Default::default())
                .await
                .unwrap();
            let submitted = backend
                .confirm(namespace(), payload.clone(), Default::default(), &submission)
                .await
                .unwrap()
                .unwrap();
            let cursor = BatchCursor {
                sequence: BatchNumber(0),
                blob_ref: submitted.blob_ref,
                payload_hash: payload_hash(payload.as_ref()),
            };
            let history = StubHistory::default();
            *history.head.lock().await = Some(BatchNumber(0));
            history.cursors.lock().await.insert(BatchNumber(0), cursor.clone());
            let base = spawn_server(sequencer_router(
                Arc::new(history),
                HistoryServerConfig { serve_payloads: false },
            ))
            .await;
            backend.fail_next_gets(cursor.blob_ref, 1);
            let replica = coro::Replica::new(
                context.child("replica"),
                backend,
                reader_config(),
                replica_config(),
                DemoReplicaApp::default(),
            );
            let err = replica.sync_once(&HttpReplicaSource::new(base)).await.unwrap_err();
            assert!(matches!(err, coro::single_sequencer::SequencerError::Core(coro::Error::CelestiaRead(_))));
        });
    }

}
