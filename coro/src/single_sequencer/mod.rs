//! Reusable single-sequencer framework built on top of `coro`'s DA-only layer.
//!
//! This module keeps sequencing, local archive management, and canonical
//! `BatchNumber -> BlobRef` mapping above the lower-level Celestia transport.
//!
//! Trust assumptions:
//! - one active sequencer operator is trusted to order transactions
//! - local archive/history is primary
//! - Celestia DA is used for availability and recovery, not archival indexing
//! - host applications must still validate that batch bytes are valid for their chain
//! - `submit()` is a durable local acceptance boundary only
//! - archived batches are a local soft-confirmation stage
//! - published batches are the canonical replica-visible stage
//!
//! ```no_run
//! use async_trait::async_trait;
//! use bytes::{Buf, BufMut, Bytes};
//! use commonware_codec::{Encode, FixedSize, Read, ReadExt, Write};
//! use coro::{
//!     backend::MockBackend,
//!     single_sequencer::{
//!         Application, BatchNumber, BatchPolicy, ExecutedBatch, SequencerConfig,
//!         SingleSequencer,
//!     },
//!     ChainConfig, PublisherConfig, ReaderConfig, RetryConfig, VerificationMode,
//! };
//! use commonware_runtime::{tokio as runtime_tokio, Clock, Runner, Storage, Supervisor};
//! use std::time::Duration;
//!
//! #[derive(Clone)]
//! struct Metadata {
//!     txs: u64,
//! }
//!
//! impl Write for Metadata {
//!     fn write(&self, buf: &mut impl BufMut) {
//!         self.txs.write(buf);
//!     }
//! }
//!
//! impl FixedSize for Metadata {
//!     const SIZE: usize = u64::SIZE;
//! }
//!
//! impl Read for Metadata {
//!     type Cfg = ();
//!
//!     fn read_cfg(buf: &mut impl Buf, _: &()) -> Result<Self, commonware_codec::Error> {
//!         Ok(Self { txs: u64::read(buf)? })
//!     }
//! }
//!
//! struct App;
//!
//! #[async_trait]
//! impl Application for App {
//!     type Tx = u64;
//!     type Metadata = Metadata;
//!     type Error = String;
//!
//!     async fn execute_batch(
//!         &mut self,
//!         _sequence: BatchNumber,
//!         txs: Vec<Self::Tx>,
//!     ) -> Result<ExecutedBatch<Self::Metadata>, Self::Error> {
//!         let mut payload = Vec::with_capacity(txs.len() * 8);
//!         for tx in &txs {
//!             payload.extend_from_slice(&tx.to_be_bytes());
//!         }
//!         Ok(ExecutedBatch {
//!             payload: Bytes::from(payload),
//!             metadata: Metadata { txs: txs.len() as u64 },
//!         })
//!     }
//! }
//!
//! fn namespace() -> coro::NamespaceId {
//!     let mut ns = [0u8; 29];
//!     ns[19..].copy_from_slice(b"sequencer1");
//!     coro::NamespaceId(ns)
//! }
//!
//! let runner = runtime_tokio::Runner::default();
//! runner.start(|context| async move {
//!     let backend = MockBackend::new();
//!     let sequencer = SingleSequencer::new(
//!         context.child("sequencer"),
//!         backend,
//!         PublisherConfig {
//!             chain: ChainConfig {
//!                 namespace: namespace(),
//!                 max_payload_bytes: 1024,
//!             },
//!             partition: "example-publisher".into(),
//!             retry: RetryConfig {
//!                 initial_backoff: Duration::from_millis(10),
//!                 max_backoff: Duration::from_secs(1),
//!                 max_attempts: Some(5),
//!             },
//!             readback_timeout: Duration::from_secs(5),
//!             tx: Default::default(),
//!         },
//!         ReaderConfig {
//!             chain: ChainConfig {
//!                 namespace: namespace(),
//!                 max_payload_bytes: 1024,
//!             },
//!             verification: VerificationMode::RpcIncluded,
//!             read_timeout: Duration::from_secs(5),
//!         },
//!         SequencerConfig {
//!             partition: "example-sequencer".into(),
//!             batch_policy: BatchPolicy {
//!                 max_txs: 32,
//!                 max_payload_bytes: 1024,
//!                 max_delay: Duration::from_secs(1),
//!             },
//!             max_tx_bytes: 64,
//!             max_metadata_bytes: 32,
//!         },
//!         App,
//!     );
//!
//!     sequencer.submit(7).await.unwrap();
//!     let _ = sequencer.flush().await.unwrap();
//! });
//! ```

use std::{
    fmt,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes};
use commonware_codec::{Decode, DecodeExt, Encode, EncodeSize, FixedSize, Read, ReadExt, Write};
use commonware_runtime::{Clock, Storage};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::{
    backend::Backend,
    publisher::Publisher,
    reader::Reader,
    store::payload_hash,
    types::{BlobRef, PayloadCfg, PublishRequest, SubmissionId},
    Error,
};

pub mod replica;
mod store;

pub use replica::{
    AppliedBatch, Replica, ReplicaApplication, ReplicaBatch, ReplicaConfig,
    ReplicaRecoveryReport, ReplicaSource, ReplicaState,
};

use store::{ArchiveRecord, IngressRecord, SequencerStore, StoreState};

pub type Result<T, E> = std::result::Result<T, SequencerError<E>>;

#[derive(Debug)]
pub enum SequencerError<E> {
    Core(Error),
    Application(E),
}

impl<E> From<Error> for SequencerError<E> {
    fn from(value: Error) -> Self {
        Self::Core(value)
    }
}

impl<E: fmt::Display> fmt::Display for SequencerError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Core(err) => err.fmt(f),
            Self::Application(err) => write!(f, "application error: {err}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BatchNumber(pub u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchPolicy {
    pub max_txs: usize,
    pub max_payload_bytes: usize,
    pub max_delay: Duration,
}

impl BatchPolicy {
    pub fn validate(&self) -> crate::Result<()> {
        if self.max_txs == 0 {
            return Err(Error::Decode("batch max_txs must be > 0".into()));
        }
        if self.max_payload_bytes == 0 {
            return Err(Error::Decode("batch max_payload_bytes must be > 0".into()));
        }
        if self.max_delay.is_zero() {
            return Err(Error::Decode("batch max_delay must be non-zero".into()));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SequencerConfig {
    pub partition: String,
    pub batch_policy: BatchPolicy,
    pub max_tx_bytes: usize,
    pub max_metadata_bytes: usize,
}

impl SequencerConfig {
    pub fn validate(&self) -> crate::Result<()> {
        if self.partition.is_empty()
            || self
                .partition
                .chars()
                .any(|c| !(c.is_ascii_alphanumeric() || ['_', '-'].contains(&c)))
        {
            return Err(Error::Decode(
                "sequencer partition must only contain alphanumeric, '-' or '_'".into(),
            ));
        }
        self.batch_policy.validate()?;
        if self.max_tx_bytes == 0 {
            return Err(Error::Decode("sequencer max_tx_bytes must be > 0".into()));
        }
        if self.max_metadata_bytes == 0 {
            return Err(Error::Decode("sequencer max_metadata_bytes must be > 0".into()));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutedBatch<M> {
    pub payload: Bytes,
    pub metadata: M,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchivedBatch<M> {
    pub sequence: BatchNumber,
    pub payload: Bytes,
    pub payload_hash: [u8; 32],
    pub metadata: M,
    pub blob_ref: Option<BlobRef>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchCursor {
    pub sequence: BatchNumber,
    pub blob_ref: BlobRef,
    pub payload_hash: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BatchStatus {
    /// The batch exists in the local archive but is not yet replica-visible canonical history.
    Archived,
    /// The batch has a durable canonical cursor and is visible to replicas.
    Published(BatchCursor),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    pub resumed_batches: Vec<BatchNumber>,
    pub finalized_batches: Vec<BatchCursor>,
    pub pending_batches: Vec<BatchNumber>,
}

#[async_trait]
pub trait Application: Send + 'static {
    type Tx: Clone + Decode<Cfg = ()> + Encode + Send + Sync + 'static;
    type Metadata: Clone + Decode<Cfg = ()> + Encode + Send + Sync + 'static;
    type Error: fmt::Display + Send + Sync + 'static;

    async fn execute_batch(
        &mut self,
        sequence: BatchNumber,
        txs: Vec<Self::Tx>,
    ) -> std::result::Result<ExecutedBatch<Self::Metadata>, Self::Error>;
}

pub struct SingleSequencer<C, B, A>
where
    A: Application,
{
    context: Arc<C>,
    publisher: Publisher<C, B>,
    reader: Reader<C, B>,
    application: Mutex<A>,
    store: SequencerStore<C, A::Metadata>,
    config: SequencerConfig,
    state: Mutex<StoreState>,
    writer: Mutex<()>,
}

impl<C, B, A> SingleSequencer<C, B, A>
where
    C: Clock + Storage + Send + Sync + 'static,
    B: Backend,
    A: Application,
{
    pub fn new(
        context: C,
        backend: B,
        publisher_config: crate::PublisherConfig,
        reader_config: crate::ReaderConfig,
        config: SequencerConfig,
        application: A,
    ) -> Self {
        let context = Arc::new(context);
        Self::from_shared(
            context.clone(),
            Publisher::from_shared(context.clone(), backend.clone(), publisher_config),
            Reader::from_shared(context.clone(), backend, reader_config),
            config,
            application,
        )
    }

    pub fn from_shared(
        context: Arc<C>,
        publisher: Publisher<C, B>,
        reader: Reader<C, B>,
        config: SequencerConfig,
        application: A,
    ) -> Self {
        Self {
            store: SequencerStore::new(
                context.clone(),
                config.partition.clone(),
                config.max_tx_bytes,
                config.max_metadata_bytes,
                config.batch_policy.max_payload_bytes,
            ),
            context,
            publisher,
            reader,
            application: Mutex::new(application),
            config,
            state: Mutex::new(StoreState::default()),
            writer: Mutex::new(()),
        }
    }

    pub async fn submit(&self, tx: A::Tx) -> Result<(), A::Error> {
        let _writer = self.writer.lock().await;
        self.ensure_initialized().await?;

        let encoded = tx.encode();
        if encoded.len() > self.config.max_tx_bytes {
            return Err(Error::PayloadTooLarge.into());
        }

        let accepted_at_ms = now_millis(self.context.current())?;
        let mut state = self.state.lock().await;
        let position = state.next_ingress_position;
        self.store
            .write_ingress(position, &IngressRecord::new(accepted_at_ms, encoded.into()))
            .await?;
        state.next_ingress_position = state.next_ingress_position.saturating_add(1);
        Ok(())
    }

    pub async fn process_ready(&self) -> Result<Option<BatchCursor>, A::Error> {
        let _writer = self.writer.lock().await;
        let Some(record) = self.archive_inner(false).await? else {
            return Ok(None);
        };
        self.publish_archive_record(record).await.map(Some)
    }

    pub async fn flush(&self) -> Result<Option<BatchCursor>, A::Error> {
        let _writer = self.writer.lock().await;
        let Some(record) = self.archive_inner(true).await? else {
            return Ok(None);
        };
        self.publish_archive_record(record).await.map(Some)
    }

    /// Archives one ready batch without publishing it to DA.
    ///
    /// This is the durable local soft-confirmation boundary for single-sequencer
    /// deployments. Existing ingress is cut according to the configured
    /// [`BatchPolicy`]. If the policy is not ready yet, this returns `None`.
    pub async fn archive_ready(&self) -> Result<Option<ArchivedBatch<A::Metadata>>, A::Error> {
        let _writer = self.writer.lock().await;
        self.archive_inner(false)
            .await?
            .map(|record| record.validate())
            .transpose()
            .map_err(Into::into)
    }

    /// Archives pending ingress immediately without publishing it to DA.
    pub async fn flush_archive(&self) -> Result<Option<ArchivedBatch<A::Metadata>>, A::Error> {
        let _writer = self.writer.lock().await;
        self.archive_inner(true)
            .await?
            .map(|record| record.validate())
            .transpose()
            .map_err(Into::into)
    }

    /// Returns archived batches that do not yet have a durable published cursor.
    pub async fn unpublished_archived_batches(&self) -> Result<Vec<BatchNumber>, A::Error> {
        self.ensure_initialized().await?;
        let mut sequences = Vec::new();
        for record in self.store.archived_batches().await? {
            let archived = record.validate()?;
            if archived.blob_ref.is_none()
                && self.store.load_cursor(archived.sequence).await?.is_none()
            {
                sequences.push(archived.sequence);
            }
        }
        Ok(sequences)
    }

    /// Publishes a previously archived batch and records its canonical cursor.
    ///
    /// The operation is idempotent for the same archived payload. It returns
    /// `None` when the requested sequence is not archived locally.
    pub async fn publish_archived_sequence(
        &self,
        sequence: BatchNumber,
    ) -> Result<Option<BatchCursor>, A::Error> {
        let record = {
            let _writer = self.writer.lock().await;
            self.ensure_initialized().await?;
            if let Some(cursor) = self.batch_cursor(sequence).await? {
                return Ok(Some(cursor));
            }
            let Some(record) = self.store.load_archive(sequence).await? else {
                return Ok(None);
            };
            record
        };

        let receipt = self
            .publisher
            .publish(PublishRequest {
                id: submission_id(record.sequence, record.payload_hash),
                payload: record.payload.clone(),
            })
            .await?;
        let cursor = BatchCursor {
            sequence: record.sequence,
            blob_ref: receipt.blob_ref,
            payload_hash: record.payload_hash,
        };
        self.record_published_cursor(cursor).await
    }

    /// Records a cursor for an archived batch that was published externally.
    ///
    /// This keeps Coro's durable archive/status model authoritative even when a
    /// host application uses its own DA submission strategy.
    pub async fn record_published_cursor(
        &self,
        cursor: BatchCursor,
    ) -> Result<Option<BatchCursor>, A::Error> {
        let _writer = self.writer.lock().await;
        self.ensure_initialized().await?;
        if let Some(existing) = self.batch_cursor(cursor.sequence).await? {
            if existing != cursor {
                return Err(Error::SubmissionConflict.into());
            }
            return Ok(Some(existing));
        }

        let Some(record) = self.store.load_archive(cursor.sequence).await? else {
            return Ok(None);
        };
        let archived = record.validate()?;
        if archived.payload_hash != cursor.payload_hash {
            return Err(Error::SubmissionConflict.into());
        }
        self.store.write_cursor(&cursor).await?;
        self.store
            .write_archive_blob_ref(cursor.sequence, cursor.blob_ref)
            .await?;
        Ok(Some(cursor))
    }

    pub async fn recover(&self) -> Result<RecoveryReport, A::Error> {
        let _writer = self.writer.lock().await;
        self.ensure_initialized().await?;

        let mut report = RecoveryReport::default();
        self.prune_replayed_ingress().await?;

        for batch in self.store.archived_batches().await? {
            let archived = batch.clone().validate()?;
            if let Some(blob_ref) = archived.blob_ref {
                let cursor = BatchCursor {
                    sequence: archived.sequence,
                    blob_ref,
                    payload_hash: archived.payload_hash,
                };
                self.store.write_cursor(&cursor).await?;
                report.finalized_batches.push(cursor);
                continue;
            }

            report.pending_batches.push(archived.sequence);
            report.resumed_batches.push(archived.sequence);
            let cursor = self.publish_archive_record(batch).await?;
            report.finalized_batches.push(cursor);
        }

        let next_sequence = self
            .store
            .next_sequence_from_storage()
            .await?
            .unwrap_or(0);
        self.state.lock().await.next_sequence = next_sequence;
        Ok(report)
    }

    pub async fn archived_batch(
        &self,
        sequence: BatchNumber,
    ) -> Result<Option<ArchivedBatch<A::Metadata>>, A::Error> {
        self.ensure_initialized().await?;
        self.store
            .load_archive(sequence)
            .await?
            .map(|record| record.validate())
            .transpose()
            .map_err(Into::into)
    }

    pub async fn batch_cursor(
        &self,
        sequence: BatchNumber,
    ) -> Result<Option<BatchCursor>, A::Error> {
        self.ensure_initialized().await?;
        if let Some(cursor) = self.store.load_cursor(sequence).await? {
            return Ok(Some(cursor));
        }
        if let Some(batch) = self.store.load_archive(sequence).await? {
            let batch = batch.validate()?;
            if let Some(blob_ref) = batch.blob_ref {
                let cursor = BatchCursor {
                    sequence,
                    blob_ref,
                    payload_hash: batch.payload_hash,
                };
                self.store.write_cursor(&cursor).await?;
                return Ok(Some(cursor));
            }
        }
        Ok(None)
    }

    pub async fn payload(&self, sequence: BatchNumber) -> Result<Option<Bytes>, A::Error> {
        self.ensure_initialized().await?;
        if let Some(batch) = self.store.load_archive(sequence).await? {
            return Ok(Some(batch.validate()?.payload));
        }
        let Some(cursor) = self.batch_cursor(sequence).await? else {
            return Ok(None);
        };
        let verified = self.reader.read(cursor.blob_ref).await?;
        Ok(Some(verified.payload))
    }

    /// Returns the highest published canonical batch.
    ///
    /// This is replica-visible head, not merely the highest locally archived batch.
    pub async fn head(&self) -> Result<Option<BatchNumber>, A::Error> {
        self.published_head().await
    }

    /// Returns the highest locally archived batch, including soft-confirmed unpublished batches.
    pub async fn archived_head(&self) -> Result<Option<BatchNumber>, A::Error> {
        let _writer = self.writer.lock().await;
        self.ensure_initialized().await?;
        self.store.archived_head().await.map_err(Into::into)
    }

    /// Returns the highest published canonical batch with a durable [`BatchCursor`].
    pub async fn published_head(&self) -> Result<Option<BatchNumber>, A::Error> {
        let _writer = self.writer.lock().await;
        self.ensure_initialized().await?;
        self.store.published_head().await.map_err(Into::into)
    }

    /// Returns the current durable status of a sequence if it is known locally.
    pub async fn status(&self, sequence: BatchNumber) -> Result<Option<BatchStatus>, A::Error> {
        let _writer = self.writer.lock().await;
        self.ensure_initialized().await?;

        if let Some(cursor) = self.store.load_cursor(sequence).await? {
            return Ok(Some(BatchStatus::Published(cursor)));
        }

        if let Some(batch) = self.store.load_archive(sequence).await? {
            let batch = batch.validate()?;
            if let Some(blob_ref) = batch.blob_ref {
                let cursor = BatchCursor {
                    sequence,
                    blob_ref,
                    payload_hash: batch.payload_hash,
                };
                self.store.write_cursor(&cursor).await?;
                return Ok(Some(BatchStatus::Published(cursor)));
            }
            return Ok(Some(BatchStatus::Archived));
        }

        Ok(None)
    }

    async fn archive_inner(
        &self,
        force: bool,
    ) -> Result<Option<ArchiveRecord<A::Metadata>>, A::Error> {
        self.ensure_initialized().await?;
        self.prune_replayed_ingress().await?;

        let ingress = self.store.ingress_records().await?;
        let Some(cut) = self.select_batch(&ingress, force)? else {
            return Ok(None);
        };

        let sequence = self.state.lock().await.next_sequence_batch_number();
        let txs = cut
            .records
            .iter()
            .map(|(_, record)| A::Tx::decode(record.tx.clone()))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Error::from)?;

        let executed = self
            .application
            .lock()
            .await
            .execute_batch(sequence, txs)
            .await
            .map_err(SequencerError::Application)?;

        if executed.payload.len() > self.config.batch_policy.max_payload_bytes {
            return Err(Error::PayloadTooLarge.into());
        }

        if executed.metadata.encode_size() > self.config.max_metadata_bytes {
            return Err(Error::Decode("batch metadata exceeds configured maximum".into()).into());
        }

        let record = ArchiveRecord::new(
            sequence,
            executed.payload,
            executed.metadata,
            cut.records.last().expect("non-empty cut").0,
        );
        self.store.write_archive(&record).await?;
        self.store.delete_ingress_through(record.last_ingress_position).await?;
        self.state.lock().await.next_sequence = sequence.0.saturating_add(1);

        Ok(Some(record))
    }

    fn select_batch(
        &self,
        ingress: &[(u64, IngressRecord)],
        force: bool,
    ) -> Result<Option<BatchCut>, A::Error> {
        let Some((_, first)) = ingress.first() else {
            return Ok(None);
        };
        let mut selected = Vec::new();
        let mut total_bytes = 0usize;

        for (position, record) in ingress {
            if record.tx.len() > self.config.batch_policy.max_payload_bytes {
                return Err(Error::PayloadTooLarge.into());
            }
            if selected.len() == self.config.batch_policy.max_txs {
                break;
            }
            if !selected.is_empty()
                && total_bytes.saturating_add(record.tx.len())
                    > self.config.batch_policy.max_payload_bytes
            {
                break;
            }
            total_bytes = total_bytes.saturating_add(record.tx.len());
            selected.push((*position, record.clone()));
        }

        if selected.is_empty() {
            return Ok(None);
        }

        let oldest = millis_to_system_time(first.accepted_at_ms)?;
        let ready = force
            || selected.len() >= self.config.batch_policy.max_txs
            || total_bytes >= self.config.batch_policy.max_payload_bytes
            || self.context.current() >= oldest + self.config.batch_policy.max_delay;
        if !ready {
            return Ok(None);
        }
        Ok(Some(BatchCut { records: selected }))
    }

    async fn publish_archive_record(
        &self,
        record: ArchiveRecord<A::Metadata>,
    ) -> Result<BatchCursor, A::Error> {
        let receipt = self
            .publisher
            .publish(PublishRequest {
                id: submission_id(record.sequence, record.payload_hash),
                payload: record.payload.clone(),
            })
            .await?;
        let cursor = BatchCursor {
            sequence: record.sequence,
            blob_ref: receipt.blob_ref,
            payload_hash: record.payload_hash,
        };
        self.store.write_cursor(&cursor).await?;
        self.store.write_archive_blob_ref(record.sequence, receipt.blob_ref).await?;
        Ok(cursor)
    }

    async fn prune_replayed_ingress(&self) -> Result<(), A::Error> {
        if let Some(last) = self.store.max_archived_ingress_position().await? {
            self.store.delete_ingress_through(last).await?;
        }
        Ok(())
    }

    async fn ensure_initialized(&self) -> Result<(), A::Error> {
        self.config.validate()?;
        let mut state = self.state.lock().await;
        if state.initialized {
            return Ok(());
        }
        self.store.ensure_initialized().await?;
        *state = self.store.load_state().await?;
        Ok(())
    }
}

#[derive(Clone)]
struct BatchCut {
    records: Vec<(u64, IngressRecord)>,
}

fn submission_id(sequence: BatchNumber, payload_hash: [u8; 32]) -> SubmissionId {
    let mut hasher = Sha256::new();
    hasher.update(b"coro.single_sequencer.v1");
    hasher.update(sequence.0.to_be_bytes());
    hasher.update(payload_hash);
    SubmissionId(hasher.finalize().into())
}

fn now_millis(now: SystemTime) -> crate::Result<u64> {
    now.duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Decode("clock is before unix epoch".into()))
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
}

fn millis_to_system_time(millis: u64) -> crate::Result<SystemTime> {
    UNIX_EPOCH
        .checked_add(Duration::from_millis(millis))
        .ok_or_else(|| Error::Decode("stored timestamp overflowed system time".into()))
}

impl BatchNumber {
    fn encode_key(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

impl StoreState {
    fn next_sequence_batch_number(&self) -> BatchNumber {
        BatchNumber(self.next_sequence)
    }
}

impl Write for BatchNumber {
    fn write(&self, buf: &mut impl BufMut) {
        self.0.write(buf);
    }
}

impl FixedSize for BatchNumber {
    const SIZE: usize = u64::SIZE;
}

impl Read for BatchNumber {
    type Cfg = ();

    fn read_cfg(buf: &mut impl Buf, _: &()) -> std::result::Result<Self, commonware_codec::Error> {
        Ok(Self(u64::read(buf)?))
    }
}

impl Write for BatchCursor {
    fn write(&self, buf: &mut impl BufMut) {
        self.sequence.write(buf);
        self.blob_ref.write(buf);
        self.payload_hash.write(buf);
    }
}

impl FixedSize for BatchCursor {
    const SIZE: usize = BatchNumber::SIZE + BlobRef::SIZE + <[u8; 32]>::SIZE;
}

impl Read for BatchCursor {
    type Cfg = ();

    fn read_cfg(buf: &mut impl Buf, _: &()) -> std::result::Result<Self, commonware_codec::Error> {
        Ok(Self {
            sequence: BatchNumber::read(buf)?,
            blob_ref: BlobRef::read(buf)?,
            payload_hash: <[u8; 32]>::read(buf)?,
        })
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ArchiveRecordCfg {
    pub max_metadata_bytes: usize,
    pub max_payload_bytes: usize,
}

impl<M> ArchiveRecord<M>
where
    M: Clone,
{
    fn new(sequence: BatchNumber, payload: Bytes, metadata: M, last_ingress_position: u64) -> Self {
        Self {
            sequence,
            payload_hash: payload_hash(payload.as_ref()),
            payload,
            metadata,
            blob_ref: None,
            last_ingress_position,
        }
    }
}

impl<M> ArchiveRecord<M>
where
    M: Clone,
{
    fn validate(self) -> crate::Result<ArchivedBatch<M>> {
        let computed = payload_hash(self.payload.as_ref());
        if computed != self.payload_hash {
            return Err(Error::Decode("archived payload hash mismatch".into()));
        }
        Ok(ArchivedBatch {
            sequence: self.sequence,
            payload: self.payload,
            payload_hash: self.payload_hash,
            metadata: self.metadata,
            blob_ref: self.blob_ref,
        })
    }
}

impl Write for IngressRecord {
    fn write(&self, buf: &mut impl BufMut) {
        self.accepted_at_ms.write(buf);
        self.tx.write(buf);
    }
}

impl EncodeSize for IngressRecord {
    fn encode_size(&self) -> usize {
        self.accepted_at_ms.encode_size() + self.tx.encode_size()
    }
}

impl Read for IngressRecord {
    type Cfg = PayloadCfg;

    fn read_cfg(buf: &mut impl Buf, cfg: &Self::Cfg) -> std::result::Result<Self, commonware_codec::Error> {
        Ok(Self {
            accepted_at_ms: u64::read(buf)?,
            tx: Bytes::read_cfg(buf, &(0..=cfg.max_payload_bytes).into())?,
        })
    }
}

impl<M> Write for ArchiveRecord<M>
where
    M: Encode,
{
    fn write(&self, buf: &mut impl BufMut) {
        let metadata = self.metadata.encode();
        self.sequence.write(buf);
        self.payload_hash.write(buf);
        self.payload.write(buf);
        Bytes::from(metadata).write(buf);
        match self.blob_ref {
            Some(blob_ref) => {
                1u8.write(buf);
                blob_ref.write(buf);
            }
            None => 0u8.write(buf),
        }
        self.last_ingress_position.write(buf);
    }
}

impl<M> EncodeSize for ArchiveRecord<M>
where
    M: Encode,
{
    fn encode_size(&self) -> usize {
        let metadata = self.metadata.encode();
        self.sequence.encode_size()
            + self.payload_hash.encode_size()
            + self.payload.encode_size()
            + Bytes::from(metadata).encode_size()
            + u8::SIZE
            + self.blob_ref.map_or(0, |_| BlobRef::SIZE)
            + self.last_ingress_position.encode_size()
    }
}

impl<M> Read for ArchiveRecord<M>
where
    M: Decode<Cfg = ()>,
{
    type Cfg = ArchiveRecordCfg;

    fn read_cfg(buf: &mut impl Buf, cfg: &Self::Cfg) -> std::result::Result<Self, commonware_codec::Error> {
        let sequence = BatchNumber::read(buf)?;
        let payload_hash = <[u8; 32]>::read(buf)?;
        let payload = Bytes::read_cfg(buf, &(0..=cfg.max_payload_bytes).into())?;

        let metadata_bytes = Bytes::read_cfg(buf, &(0..=cfg.max_metadata_bytes).into())?;
        let metadata = M::decode(metadata_bytes)?;

        let blob_ref = match u8::read(buf)? {
            0 => None,
            1 => Some(BlobRef::read(buf)?),
            other => return Err(commonware_codec::Error::InvalidEnum(other)),
        };
        let last_ingress_position = u64::read(buf)?;

        Ok(Self {
            sequence,
            payload_hash,
            payload,
            metadata,
            blob_ref,
            last_ingress_position,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use bytes::Bytes;
    use commonware_codec::{Encode, ReadExt};
    use commonware_runtime::{deterministic, Runner};

    use super::*;
    use crate::{
        backend::MockBackend,
        config::{ChainConfig, PublisherConfig, ReaderConfig, RetryConfig, VerificationMode},
        types::{BlobCommitment, NamespaceId},
    };

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestMetadata {
        applied: u64,
    }

    impl Write for TestMetadata {
        fn write(&self, buf: &mut impl BufMut) {
            self.applied.write(buf);
        }
    }

    impl FixedSize for TestMetadata {
        const SIZE: usize = u64::SIZE;
    }

    impl Read for TestMetadata {
        type Cfg = ();

        fn read_cfg(buf: &mut impl Buf, _: &()) -> std::result::Result<Self, commonware_codec::Error> {
            Ok(Self { applied: u64::read(buf)? })
        }
    }

    #[derive(Default)]
    struct TestApp {
        executed: Vec<Vec<u64>>,
    }

    #[async_trait]
    impl Application for TestApp {
        type Tx = u64;
        type Metadata = TestMetadata;
        type Error = String;

        async fn execute_batch(
            &mut self,
            _sequence: BatchNumber,
            txs: Vec<Self::Tx>,
        ) -> std::result::Result<ExecutedBatch<Self::Metadata>, Self::Error> {
            self.executed.push(txs.clone());
            let mut payload = Vec::with_capacity(txs.len() * 8);
            for tx in &txs {
                payload.extend_from_slice(&tx.to_be_bytes());
            }
            Ok(ExecutedBatch {
                payload: Bytes::from(payload),
                metadata: TestMetadata {
                    applied: txs.len() as u64,
                },
            })
        }
    }

    fn namespace() -> NamespaceId {
        let mut ns = [0u8; 29];
        ns[19..].copy_from_slice(b"sequencer1");
        NamespaceId(ns)
    }

    fn publisher_config() -> PublisherConfig {
        PublisherConfig {
            chain: ChainConfig {
                namespace: namespace(),
                max_payload_bytes: 1024,
            },
            partition: "sequencer-publisher".into(),
            retry: RetryConfig {
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(8),
                max_attempts: Some(5),
            },
            readback_timeout: Duration::from_secs(1),
            tx: Default::default(),
        }
    }

    fn reader_config() -> ReaderConfig {
        ReaderConfig {
            chain: ChainConfig {
                namespace: namespace(),
                max_payload_bytes: 1024,
            },
            verification: VerificationMode::RpcIncluded,
            read_timeout: Duration::from_secs(1),
        }
    }

    fn sequencer_config() -> SequencerConfig {
        SequencerConfig {
            partition: "sequencer".into(),
            batch_policy: BatchPolicy {
                max_txs: 2,
                max_payload_bytes: 32,
                max_delay: Duration::from_millis(5),
            },
            max_tx_bytes: 16,
            max_metadata_bytes: 32,
        }
    }

    #[test]
    fn batch_numbering_is_monotonic() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let sequencer = SingleSequencer::from_shared(
                context.clone(),
                Publisher::from_shared(context.clone(), backend.clone(), publisher_config()),
                Reader::from_shared(context, backend, reader_config()),
                sequencer_config(),
                TestApp::default(),
            );

            sequencer.submit(1).await.unwrap();
            sequencer.submit(2).await.unwrap();
            let first = sequencer.process_ready().await.unwrap().unwrap();
            sequencer.submit(3).await.unwrap();
            sequencer.submit(4).await.unwrap();
            let second = sequencer.process_ready().await.unwrap().unwrap();
            assert_eq!(first.sequence, BatchNumber(0));
            assert_eq!(second.sequence, BatchNumber(1));
        });
    }

    #[test]
    fn batch_policy_respects_delay_and_size() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let sequencer = SingleSequencer::from_shared(
                context.clone(),
                Publisher::from_shared(context.clone(), backend.clone(), publisher_config()),
                Reader::from_shared(context.clone(), backend, reader_config()),
                sequencer_config(),
                TestApp::default(),
            );

            sequencer.submit(1).await.unwrap();
            assert!(sequencer.process_ready().await.unwrap().is_none());
            context.sleep(Duration::from_millis(5)).await;
            assert_eq!(sequencer.process_ready().await.unwrap().unwrap().sequence, BatchNumber(0));
        });
    }

    #[test]
    fn archive_ready_does_not_publish() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let sequencer = SingleSequencer::from_shared(
                context.clone(),
                Publisher::from_shared(context.clone(), backend.clone(), publisher_config()),
                Reader::from_shared(context, backend, reader_config()),
                sequencer_config(),
                TestApp::default(),
            );

            sequencer.submit(1).await.unwrap();
            sequencer.submit(2).await.unwrap();
            let archived = sequencer.archive_ready().await.unwrap().unwrap();

            assert_eq!(archived.sequence, BatchNumber(0));
            assert_eq!(archived.metadata, TestMetadata { applied: 2 });
            assert_eq!(sequencer.archived_head().await.unwrap(), Some(BatchNumber(0)));
            assert_eq!(sequencer.published_head().await.unwrap(), None);
            assert_eq!(sequencer.head().await.unwrap(), None);
            assert_eq!(
                sequencer.status(BatchNumber(0)).await.unwrap(),
                Some(BatchStatus::Archived)
            );
            assert_eq!(
                sequencer.unpublished_archived_batches().await.unwrap(),
                vec![BatchNumber(0)]
            );
        });
    }

    #[test]
    fn publish_archived_sequence_is_idempotent() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let sequencer = SingleSequencer::from_shared(
                context.clone(),
                Publisher::from_shared(context.clone(), backend.clone(), publisher_config()),
                Reader::from_shared(context, backend, reader_config()),
                sequencer_config(),
                TestApp::default(),
            );

            sequencer.submit(1).await.unwrap();
            sequencer.submit(2).await.unwrap();
            sequencer.archive_ready().await.unwrap().unwrap();

            let first = sequencer
                .publish_archived_sequence(BatchNumber(0))
                .await
                .unwrap()
                .unwrap();
            let second = sequencer
                .publish_archived_sequence(BatchNumber(0))
                .await
                .unwrap()
                .unwrap();

            assert_eq!(first, second);
            assert_eq!(sequencer.published_head().await.unwrap(), Some(BatchNumber(0)));
            assert_eq!(
                sequencer.unpublished_archived_batches().await.unwrap(),
                Vec::new()
            );
            assert_eq!(
                sequencer.status(BatchNumber(0)).await.unwrap(),
                Some(BatchStatus::Published(first))
            );
        });
    }

    #[test]
    fn record_published_cursor_accepts_external_publication() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let sequencer = SingleSequencer::from_shared(
                context.clone(),
                Publisher::from_shared(context.clone(), backend.clone(), publisher_config()),
                Reader::from_shared(context, backend, reader_config()),
                sequencer_config(),
                TestApp::default(),
            );

            sequencer.submit(1).await.unwrap();
            sequencer.submit(2).await.unwrap();
            let archived = sequencer.archive_ready().await.unwrap().unwrap();
            let cursor = BatchCursor {
                sequence: archived.sequence,
                blob_ref: BlobRef {
                    height: 42,
                    namespace: namespace(),
                    commitment: BlobCommitment([7; 32]),
                },
                payload_hash: archived.payload_hash,
            };

            assert_eq!(
                sequencer.record_published_cursor(cursor.clone()).await.unwrap(),
                Some(cursor.clone())
            );
            assert_eq!(
                sequencer.record_published_cursor(cursor.clone()).await.unwrap(),
                Some(cursor.clone())
            );
            assert_eq!(sequencer.published_head().await.unwrap(), Some(BatchNumber(0)));
            assert_eq!(
                sequencer.status(BatchNumber(0)).await.unwrap(),
                Some(BatchStatus::Published(cursor))
            );
            assert_eq!(
                sequencer.unpublished_archived_batches().await.unwrap(),
                Vec::new()
            );
        });
    }

    #[test]
    fn archive_record_codec_roundtrip() {
        let record = ArchiveRecord {
            sequence: BatchNumber(7),
            payload: Bytes::from_static(b"payload"),
            payload_hash: payload_hash(b"payload"),
            metadata: TestMetadata { applied: 3 },
            blob_ref: Some(BlobRef {
                height: 9,
                namespace: NamespaceId([1; 29]),
                commitment: BlobCommitment([2; 32]),
            }),
            last_ingress_position: 11,
        };
        let bytes = record.encode();
        let decoded = ArchiveRecord::<TestMetadata>::decode_cfg(
            bytes,
            &ArchiveRecordCfg {
                max_metadata_bytes: 32,
                max_payload_bytes: 1024,
            },
        )
        .unwrap();
        assert_eq!(decoded.sequence, record.sequence);
        assert_eq!(decoded.payload, record.payload);
        assert_eq!(decoded.metadata, record.metadata);
        assert_eq!(decoded.blob_ref, record.blob_ref);
        assert_eq!(decoded.last_ingress_position, record.last_ingress_position);
    }

    #[test]
    fn sequence_to_blob_ref_mapping_persists() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let sequencer_backend = backend.clone();
            let restarted_backend = backend.clone();
            let sequencer = SingleSequencer::from_shared(
                context.clone(),
                Publisher::from_shared(context.clone(), sequencer_backend.clone(), publisher_config()),
                Reader::from_shared(context.clone(), sequencer_backend, reader_config()),
                sequencer_config(),
                TestApp::default(),
            );

            sequencer.submit(1).await.unwrap();
            sequencer.submit(2).await.unwrap();
            let cursor = sequencer.process_ready().await.unwrap().unwrap();

            let restarted = SingleSequencer::from_shared(
                context,
                Publisher::from_shared(Arc::clone(&sequencer.context), restarted_backend.clone(), publisher_config()),
                Reader::from_shared(Arc::clone(&sequencer.context), restarted_backend, reader_config()),
                sequencer_config(),
                TestApp::default(),
            );
            assert_eq!(restarted.batch_cursor(BatchNumber(0)).await.unwrap(), Some(cursor));
        });
    }

    #[test]
    fn duplicate_recovery_is_safe() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let publisher = Publisher::from_shared(context.clone(), backend.clone(), publisher_config());
            let reader = Reader::from_shared(context.clone(), backend, reader_config());
            let sequencer = SingleSequencer::from_shared(
                context,
                publisher,
                reader,
                sequencer_config(),
                TestApp::default(),
            );

            sequencer.submit(1).await.unwrap();
            sequencer.submit(2).await.unwrap();
            let cursor = sequencer.process_ready().await.unwrap().unwrap();
            let first = sequencer.recover().await.unwrap();
            let second = sequencer.recover().await.unwrap();
            assert!(first.finalized_batches.contains(&cursor));
            assert!(second.finalized_batches.contains(&cursor));
        });
    }

    #[test]
    fn recovery_after_archived_but_unpublished_batch() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let publisher = Publisher::from_shared(context.clone(), backend.clone(), publisher_config());
            let reader = Reader::from_shared(context.clone(), backend, reader_config());
            let sequencer = SingleSequencer::from_shared(
                context.clone(),
                publisher,
                reader,
                sequencer_config(),
                TestApp::default(),
            );
            sequencer.ensure_initialized().await.unwrap();
            sequencer
                .store
                .write_archive(&ArchiveRecord::new(
                    BatchNumber(0),
                    Bytes::from_static(b"payload"),
                    TestMetadata { applied: 1 },
                    0,
                ))
                .await
                .unwrap();

            let report = sequencer.recover().await.unwrap();
            assert_eq!(report.resumed_batches, vec![BatchNumber(0)]);
            assert_eq!(report.finalized_batches.len(), 1);
        });
    }

    #[test]
    fn recovery_after_published_but_not_finalized_batch() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let publisher = Publisher::from_shared(context.clone(), backend.clone(), publisher_config());
            let reader = Reader::from_shared(context.clone(), backend, reader_config());
            let sequencer = SingleSequencer::from_shared(
                context.clone(),
                publisher,
                reader,
                sequencer_config(),
                TestApp::default(),
            );
            sequencer.ensure_initialized().await.unwrap();
            let record = ArchiveRecord::new(
                BatchNumber(0),
                Bytes::from_static(b"payload"),
                TestMetadata { applied: 1 },
                0,
            );
            sequencer
                .publisher
                .publish(PublishRequest {
                    id: submission_id(record.sequence, record.payload_hash),
                    payload: record.payload.clone(),
                })
                .await
                .unwrap();
            sequencer.store.write_archive(&record).await.unwrap();

            let report = sequencer.recover().await.unwrap();
            assert_eq!(report.finalized_batches.len(), 1);
            assert_eq!(sequencer.batch_cursor(BatchNumber(0)).await.unwrap().unwrap().sequence, BatchNumber(0));
        });
    }

    #[test]
    fn local_archive_is_preferred_over_da() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let publisher = Publisher::from_shared(context.clone(), backend.clone(), publisher_config());
            let reader = Reader::from_shared(context.clone(), backend.clone(), reader_config());
            let sequencer = SingleSequencer::from_shared(
                context.clone(),
                publisher,
                reader,
                sequencer_config(),
                TestApp::default(),
            );

            sequencer.submit(1).await.unwrap();
            sequencer.submit(2).await.unwrap();
            let cursor = sequencer.process_ready().await.unwrap().unwrap();
            backend.fail_next_gets(cursor.blob_ref, 1);
            assert_eq!(sequencer.payload(BatchNumber(0)).await.unwrap(), Some(Bytes::copy_from_slice(&[0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2])));
        });
    }

    #[test]
    fn corrupted_archived_payload_hash_is_rejected() {
        let record = ArchiveRecord {
            sequence: BatchNumber(0),
            payload: Bytes::from_static(b"payload"),
            payload_hash: [9; 32],
            metadata: TestMetadata { applied: 1 },
            blob_ref: None,
            last_ingress_position: 0,
        };
        let err = record.validate().unwrap_err();
        assert!(matches!(err, Error::Decode(_)));
    }

    #[test]
    fn da_present_but_archive_missing_uses_fallback() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let publisher = Publisher::from_shared(context.clone(), backend.clone(), publisher_config());
            let reader = Reader::from_shared(context.clone(), backend, reader_config());
            let sequencer = SingleSequencer::from_shared(
                context.clone(),
                publisher,
                reader,
                sequencer_config(),
                TestApp::default(),
            );

            sequencer.submit(1).await.unwrap();
            sequencer.submit(2).await.unwrap();
            let cursor = sequencer.process_ready().await.unwrap().unwrap();
            sequencer.store.delete_archive(BatchNumber(0)).await.unwrap();
            assert_eq!(sequencer.payload(BatchNumber(0)).await.unwrap().unwrap().len(), 16);
            assert_eq!(sequencer.batch_cursor(BatchNumber(0)).await.unwrap(), Some(cursor));
        });
    }

    #[test]
    fn archived_and_published_head_are_distinct() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let sequencer = SingleSequencer::from_shared(
                context.clone(),
                Publisher::from_shared(context.clone(), backend, publisher_config()),
                Reader::from_shared(context.clone(), MockBackend::new(), reader_config()),
                sequencer_config(),
                TestApp::default(),
            );
            sequencer.ensure_initialized().await.unwrap();
            sequencer
                .store
                .write_archive(&ArchiveRecord::new(
                    BatchNumber(0),
                    Bytes::from_static(b"payload"),
                    TestMetadata { applied: 1 },
                    0,
                ))
                .await
                .unwrap();

            assert_eq!(sequencer.archived_head().await.unwrap(), Some(BatchNumber(0)));
            assert_eq!(sequencer.published_head().await.unwrap(), None);
            assert_eq!(sequencer.head().await.unwrap(), None);
            assert_eq!(sequencer.status(BatchNumber(0)).await.unwrap(), Some(BatchStatus::Archived));
        });
    }

    #[test]
    fn published_head_advances_only_after_cursor_materializes() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let sequencer = SingleSequencer::from_shared(
                context.clone(),
                Publisher::from_shared(context.clone(), backend.clone(), publisher_config()),
                Reader::from_shared(context, backend, reader_config()),
                sequencer_config(),
                TestApp::default(),
            );

            sequencer.submit(1).await.unwrap();
            sequencer.submit(2).await.unwrap();
            let cursor = sequencer.process_ready().await.unwrap().unwrap();

            assert_eq!(sequencer.archived_head().await.unwrap(), Some(BatchNumber(0)));
            assert_eq!(sequencer.published_head().await.unwrap(), Some(BatchNumber(0)));
            assert_eq!(sequencer.head().await.unwrap(), Some(BatchNumber(0)));
            assert_eq!(
                sequencer.status(BatchNumber(0)).await.unwrap(),
                Some(BatchStatus::Published(cursor))
            );
            assert_eq!(sequencer.status(BatchNumber(1)).await.unwrap(), None);
        });
    }

    #[test]
    fn recover_preserves_archived_vs_published_distinction() {
        let executor = deterministic::Runner::default();
        executor.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let sequencer = SingleSequencer::from_shared(
                context.clone(),
                Publisher::from_shared(context.clone(), backend.clone(), publisher_config()),
                Reader::from_shared(context.clone(), backend.clone(), reader_config()),
                sequencer_config(),
                TestApp::default(),
            );
            sequencer.ensure_initialized().await.unwrap();
            let unpublished = ArchiveRecord::new(
                BatchNumber(0),
                Bytes::from_static(b"payload-0"),
                TestMetadata { applied: 1 },
                0,
            );
            sequencer.store.write_archive(&unpublished).await.unwrap();
            let published = ArchiveRecord::new(
                BatchNumber(1),
                Bytes::from_static(b"payload-1"),
                TestMetadata { applied: 1 },
                1,
            );
            let receipt = sequencer
                .publisher
                .publish(PublishRequest {
                    id: submission_id(published.sequence, published.payload_hash),
                    payload: published.payload.clone(),
                })
                .await
                .unwrap();
            sequencer.store.write_archive(&published).await.unwrap();
            sequencer
                .store
                .write_archive_blob_ref(BatchNumber(1), receipt.blob_ref)
                .await
                .unwrap();

            assert_eq!(sequencer.archived_head().await.unwrap(), Some(BatchNumber(1)));
            assert_eq!(sequencer.published_head().await.unwrap(), Some(BatchNumber(1)));
            assert_eq!(sequencer.status(BatchNumber(0)).await.unwrap(), Some(BatchStatus::Archived));
            assert!(matches!(
                sequencer.status(BatchNumber(1)).await.unwrap(),
                Some(BatchStatus::Published(_))
            ));

            let report = sequencer.recover().await.unwrap();
            assert!(report.resumed_batches.contains(&BatchNumber(0)));
            assert_eq!(sequencer.archived_head().await.unwrap(), Some(BatchNumber(1)));
            assert_eq!(sequencer.published_head().await.unwrap(), Some(BatchNumber(1)));
        });
    }
}
