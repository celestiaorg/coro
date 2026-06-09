use std::{fmt, sync::Arc};

use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes};
use commonware_codec::{Decode, DecodeExt, Encode, EncodeSize, FixedSize, Read, ReadExt, Write};
use commonware_runtime::{Blob as RuntimeBlob, Clock, Storage};
use tokio::sync::Mutex;

use crate::{reader::Reader, store::payload_hash, Error};

use super::{BatchCursor, BatchNumber, Result as SequencerResult, SequencerError};

const META_PARTITION: &str = "replica_meta";
const APPLIED_PARTITION: &str = "replica_applied";
const CACHE_PARTITION: &str = "replica_cache";
const STORE_VERSION_NAME: &[u8] = b"store-version";
const STATE_NAME: &[u8] = b"state";
const STORE_VERSION: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaConfig {
    pub partition: String,
    pub max_payload_bytes: usize,
    pub max_metadata_bytes: usize,
    pub max_output_bytes: usize,
}

impl ReplicaConfig {
    pub fn validate(&self) -> crate::Result<()> {
        if self.partition.is_empty()
            || self
                .partition
                .chars()
                .any(|c| !(c.is_ascii_alphanumeric() || ['_', '-'].contains(&c)))
        {
            return Err(Error::Decode(
                "replica partition must only contain alphanumeric, '-' or '_'".into(),
            ));
        }
        if self.max_payload_bytes == 0 {
            return Err(Error::Decode("replica max_payload_bytes must be > 0".into()));
        }
        if self.max_metadata_bytes == 0 {
            return Err(Error::Decode("replica max_metadata_bytes must be > 0".into()));
        }
        if self.max_output_bytes == 0 {
            return Err(Error::Decode("replica max_output_bytes must be > 0".into()));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaState {
    pub next_sequence: BatchNumber,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppliedBatch<M, O> {
    pub metadata: M,
    pub output: O,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaBatch<M, O> {
    pub sequence: BatchNumber,
    pub cursor: BatchCursor,
    pub payload: Bytes,
    pub metadata: M,
    pub output: O,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaRecoveryReport {
    pub resumed_from: Option<BatchNumber>,
    pub applied: Vec<BatchNumber>,
    pub next_sequence: BatchNumber,
}

#[async_trait]
pub trait ReplicaSource: Send + Sync + 'static {
    async fn head(&self) -> crate::Result<Option<BatchNumber>>;
    async fn cursor(&self, sequence: BatchNumber) -> crate::Result<Option<BatchCursor>>;
    async fn payload(&self, sequence: BatchNumber) -> crate::Result<Option<Bytes>>;
}

#[async_trait]
pub trait ReplicaApplication: Send + 'static {
    type Metadata: Clone + Decode<Cfg = ()> + Encode + Send + Sync + 'static;
    type Output: Clone + Decode<Cfg = ()> + Encode + Send + Sync + 'static;
    type Error: fmt::Display + Send + Sync + 'static;

    async fn apply_batch(
        &mut self,
        sequence: BatchNumber,
        payload: Bytes,
    ) -> std::result::Result<AppliedBatch<Self::Metadata, Self::Output>, Self::Error>;
}

pub struct Replica<C, B, A>
where
    A: ReplicaApplication,
{
    reader: Reader<C, B>,
    application: Mutex<A>,
    store: ReplicaStore<C, A::Metadata, A::Output>,
    config: ReplicaConfig,
    state: Mutex<ReplicaStoreState>,
    syncer: Mutex<()>,
}

impl<C, B, A> Replica<C, B, A>
where
    C: Clock + Storage + Send + Sync + 'static,
    B: crate::backend::Backend,
    A: ReplicaApplication,
{
    pub fn new(
        context: C,
        backend: B,
        reader_config: crate::ReaderConfig,
        config: ReplicaConfig,
        application: A,
    ) -> Self {
        let context = Arc::new(context);
        Self::from_shared(
            context.clone(),
            Reader::from_shared(context.clone(), backend, reader_config),
            config,
            application,
        )
    }

    pub fn from_shared(
        context: Arc<C>,
        reader: Reader<C, B>,
        config: ReplicaConfig,
        application: A,
    ) -> Self {
        Self {
            reader,
            application: Mutex::new(application),
            store: ReplicaStore::new(
                context,
                config.partition.clone(),
                config.max_payload_bytes,
                config.max_metadata_bytes,
                config.max_output_bytes,
            ),
            config,
            state: Mutex::new(ReplicaStoreState::default()),
            syncer: Mutex::new(()),
        }
    }

    pub async fn recover(&self) -> SequencerResult<ReplicaRecoveryReport, A::Error> {
        let _syncer = self.syncer.lock().await;
        self.ensure_initialized().await?;
        let state = self.state.lock().await;
        Ok(ReplicaRecoveryReport {
            resumed_from: if state.next_sequence.0 == 0 {
                None
            } else {
                Some(state.next_sequence)
            },
            applied: Vec::new(),
            next_sequence: state.next_sequence,
        })
    }

    pub async fn state(&self) -> SequencerResult<ReplicaState, A::Error> {
        self.ensure_initialized().await?;
        Ok(ReplicaState {
            next_sequence: self.state.lock().await.next_sequence,
        })
    }

    pub async fn applied_batch(
        &self,
        sequence: BatchNumber,
    ) -> SequencerResult<Option<ReplicaBatch<A::Metadata, A::Output>>, A::Error> {
        self.ensure_initialized().await?;
        let Some(record) = self
            .store
            .load_applied(sequence)
            .await
            .map_err(SequencerError::Core)?
        else {
            return Ok(None);
        };
        let Some(payload) = self
            .store
            .load_cache(sequence)
            .await
            .map_err(SequencerError::Core)?
        else {
            return Err(SequencerError::Core(Error::NotFound));
        };
        verify_payload(record.payload_hash, payload.clone()).map_err(SequencerError::Core)?;
        Ok(Some(ReplicaBatch {
            sequence: record.sequence,
            cursor: record.cursor,
            payload,
            metadata: record.metadata,
            output: record.output,
        }))
    }

    pub async fn sync_once<S>(&self, source: &S) -> SequencerResult<Option<ReplicaBatch<A::Metadata, A::Output>>, A::Error>
    where
        S: ReplicaSource,
    {
        let _syncer = self.syncer.lock().await;
        self.ensure_initialized().await?;

        let next_sequence = self.state.lock().await.next_sequence;
        let Some(head) = source.head().await.map_err(SequencerError::Core)? else {
            return Ok(None);
        };
        if head.0 < next_sequence.0 {
            return Ok(None);
        }

        let Some(cursor) = source.cursor(next_sequence).await.map_err(SequencerError::Core)? else {
            return Err(SequencerError::Core(Error::Decode(
                "replica source head ahead of available cursor".into(),
            )));
        };

        let payload = if let Some(payload) = source
            .payload(next_sequence)
            .await
            .map_err(SequencerError::Core)?
        {
            verify_payload(cursor.payload_hash, payload.clone()).map_err(SequencerError::Core)?;
            payload
        } else if let Some(payload) = self
            .store
            .load_cache(next_sequence)
            .await
            .map_err(SequencerError::Core)?
        {
            verify_payload(cursor.payload_hash, payload.clone()).map_err(SequencerError::Core)?;
            payload
        } else {
            let verified = self.reader.read(cursor.blob_ref).await.map_err(SequencerError::Core)?;
            verify_payload(cursor.payload_hash, verified.payload.clone()).map_err(SequencerError::Core)?;
            verified.payload
        };

        let applied = self
            .application
            .lock()
            .await
            .apply_batch(next_sequence, payload.clone())
            .await
            .map_err(SequencerError::Application)?;

        let metadata_size = applied.metadata.encode_size();
        if metadata_size > self.config.max_metadata_bytes {
            return Err(SequencerError::Core(Error::Decode(
                "replica metadata exceeds configured maximum".into(),
            )));
        }
        let output_size = applied.output.encode_size();
        if output_size > self.config.max_output_bytes {
            return Err(SequencerError::Core(Error::Decode(
                "replica output exceeds configured maximum".into(),
            )));
        }

        let record = AppliedRecord {
            sequence: next_sequence,
            cursor: cursor.clone(),
            payload_hash: cursor.payload_hash,
            metadata: applied.metadata,
            output: applied.output,
        };
        self.store
            .write_applied(&record)
            .await
            .map_err(SequencerError::Core)?;
        self.store
            .write_cache(next_sequence, payload.clone())
            .await
            .map_err(SequencerError::Core)?;

        let mut state = self.state.lock().await;
        state.next_sequence = BatchNumber(next_sequence.0.saturating_add(1));
        self.store
            .write_state(ReplicaState {
                next_sequence: state.next_sequence,
            })
            .await
            .map_err(SequencerError::Core)?;

        Ok(Some(ReplicaBatch {
            sequence: next_sequence,
            cursor,
            payload,
            metadata: record.metadata,
            output: record.output,
        }))
    }

    pub async fn catch_up<S>(&self, source: &S) -> SequencerResult<Vec<ReplicaBatch<A::Metadata, A::Output>>, A::Error>
    where
        S: ReplicaSource,
    {
        let mut batches = Vec::new();
        while let Some(batch) = self.sync_once(source).await? {
            batches.push(batch);
        }
        Ok(batches)
    }

    async fn ensure_initialized(&self) -> SequencerResult<(), A::Error> {
        self.config.validate().map_err(SequencerError::Core)?;
        let mut state = self.state.lock().await;
        if state.initialized {
            return Ok(());
        }
        self.store
            .ensure_initialized()
            .await
            .map_err(SequencerError::Core)?;
        *state = self.store.load_state().await.map_err(SequencerError::Core)?;
        Ok(())
    }
}

fn verify_payload(expected_hash: [u8; 32], payload: Bytes) -> crate::Result<()> {
    if payload_hash(payload.as_ref()) != expected_hash {
        return Err(Error::Decode("replica payload hash mismatch".into()));
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct ReplicaStoreState {
    initialized: bool,
    next_sequence: BatchNumber,
}

impl Default for ReplicaStoreState {
    fn default() -> Self {
        Self {
            initialized: false,
            next_sequence: BatchNumber(0),
        }
    }
}

#[derive(Clone)]
struct ReplicaStore<S, M, O> {
    storage: Arc<S>,
    partition_prefix: Vec<u8>,
    max_payload_bytes: usize,
    max_metadata_bytes: usize,
    max_output_bytes: usize,
    _marker: std::marker::PhantomData<fn() -> (M, O)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AppliedRecord<M, O> {
    sequence: BatchNumber,
    cursor: BatchCursor,
    payload_hash: [u8; 32],
    metadata: M,
    output: O,
}

#[derive(Clone, Copy)]
struct AppliedRecordCfg {
    max_metadata_bytes: usize,
    max_output_bytes: usize,
}

impl<S, M, O> ReplicaStore<S, M, O>
where
    S: Storage + Send + Sync + 'static,
    M: Clone + Decode<Cfg = ()> + Encode + Send + Sync + 'static,
    O: Clone + Decode<Cfg = ()> + Encode + Send + Sync + 'static,
{
    fn new(
        storage: Arc<S>,
        partition_prefix: String,
        max_payload_bytes: usize,
        max_metadata_bytes: usize,
        max_output_bytes: usize,
    ) -> Self {
        Self {
            storage,
            partition_prefix: partition_prefix.into_bytes(),
            max_payload_bytes,
            max_metadata_bytes,
            max_output_bytes,
            _marker: std::marker::PhantomData,
        }
    }

    async fn ensure_initialized(&self) -> crate::Result<()> {
        let name = self.name(STORE_VERSION_NAME);
        let (blob, len) = self.storage.open(META_PARTITION, &name).await?;
        if len == 0 {
            blob.resize(1).await?;
            blob.write_at(0, Bytes::from_static(&[STORE_VERSION])).await?;
            blob.sync().await?;
        }
        Ok(())
    }

    async fn load_state(&self) -> crate::Result<ReplicaStoreState> {
        let next_sequence = match self.read_record::<ReplicaState, _, _>(META_PARTITION, self.name(STATE_NAME), ()).await? {
            Some(state) => state.next_sequence,
            None => self
                .last_applied_sequence()
                .await?
                .map(|sequence| BatchNumber(sequence.0.saturating_add(1)))
                .unwrap_or(BatchNumber(0)),
        };
        Ok(ReplicaStoreState {
            initialized: true,
            next_sequence,
        })
    }

    async fn write_state(&self, state: ReplicaState) -> crate::Result<()> {
        self.write_record(META_PARTITION, self.name(STATE_NAME), &state).await
    }

    async fn write_applied(&self, record: &AppliedRecord<M, O>) -> crate::Result<()> {
        self.write_record(APPLIED_PARTITION, self.name(&record.sequence.0.to_be_bytes()), record)
            .await
    }

    async fn load_applied(&self, sequence: BatchNumber) -> crate::Result<Option<AppliedRecord<M, O>>> {
        self.read_record(
            APPLIED_PARTITION,
            self.name(&sequence.0.to_be_bytes()),
            AppliedRecordCfg {
                max_metadata_bytes: self.max_metadata_bytes,
                max_output_bytes: self.max_output_bytes,
            },
        )
        .await
    }

    async fn last_applied_sequence(&self) -> crate::Result<Option<BatchNumber>> {
        let mut names = self.scan_partition(APPLIED_PARTITION).await?;
        names.sort();
        let Some(name) = names.last() else {
            return Ok(None);
        };
        let Some(key) = self.strip_name(name) else {
            return Ok(None);
        };
        if key.len() != 8 {
            return Err(Error::Decode("invalid replica applied key length".into()));
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(key);
        Ok(Some(BatchNumber(u64::from_be_bytes(bytes))))
    }

    async fn write_cache(&self, sequence: BatchNumber, payload: Bytes) -> crate::Result<()> {
        self.write_record(CACHE_PARTITION, self.name(&sequence.0.to_be_bytes()), &payload)
            .await
    }

    async fn load_cache(&self, sequence: BatchNumber) -> crate::Result<Option<Bytes>> {
        self.read_record(
            CACHE_PARTITION,
            self.name(&sequence.0.to_be_bytes()),
            (0..=self.max_payload_bytes).into(),
        )
        .await
    }

    fn name(&self, suffix: &[u8]) -> Vec<u8> {
        let mut name = Vec::with_capacity(self.partition_prefix.len() + 1 + suffix.len());
        name.extend_from_slice(&self.partition_prefix);
        name.push(0);
        name.extend_from_slice(suffix);
        name
    }

    fn strip_name<'a>(&self, name: &'a [u8]) -> Option<&'a [u8]> {
        let prefix_len = self.partition_prefix.len();
        if name.len() <= prefix_len || name[..prefix_len] != self.partition_prefix || name[prefix_len] != 0 {
            return None;
        }
        Some(&name[prefix_len + 1..])
    }

    async fn scan_partition(&self, partition: &str) -> crate::Result<Vec<Vec<u8>>> {
        match self.storage.scan(partition).await {
            Ok(names) => Ok(names),
            Err(commonware_runtime::Error::PartitionMissing(_)) => Ok(Vec::new()),
            Err(err) => Err(err.into()),
        }
    }

    async fn read_record<T, Cfg, N>(&self, partition: &str, name: N, cfg: Cfg) -> crate::Result<Option<T>>
    where
        T: Decode<Cfg = Cfg>,
        Cfg: Send,
        N: AsRef<[u8]>,
    {
        let (blob, len) = match self.storage.open(partition, name.as_ref()).await {
            Ok(result) => result,
            Err(commonware_runtime::Error::PartitionMissing(_)) => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        if len == 0 {
            return Ok(None);
        }
        let mut data = blob.read_at(0, len as usize).await?;
        let bytes = data.copy_to_bytes(len as usize);
        Ok(Some(T::decode_cfg(bytes, &cfg)?))
    }

    async fn write_record<T, N>(&self, partition: &str, name: N, record: &T) -> crate::Result<()>
    where
        T: Encode,
        N: AsRef<[u8]>,
    {
        let encoded = record.encode();
        let (blob, _) = self.storage.open(partition, name.as_ref()).await?;
        blob.resize(encoded.len() as u64).await?;
        blob.write_at(0, encoded).await?;
        blob.sync().await?;
        Ok(())
    }
}

impl Write for ReplicaState {
    fn write(&self, buf: &mut impl BufMut) {
        self.next_sequence.write(buf);
    }
}

impl FixedSize for ReplicaState {
    const SIZE: usize = BatchNumber::SIZE;
}

impl Read for ReplicaState {
    type Cfg = ();

    fn read_cfg(buf: &mut impl Buf, _: &()) -> std::result::Result<Self, commonware_codec::Error> {
        Ok(Self {
            next_sequence: BatchNumber::read(buf)?,
        })
    }
}

impl<M, O> Write for AppliedRecord<M, O>
where
    M: Encode,
    O: Encode,
{
    fn write(&self, buf: &mut impl BufMut) {
        self.sequence.write(buf);
        self.cursor.write(buf);
        self.payload_hash.write(buf);
        Bytes::from(self.metadata.encode()).write(buf);
        Bytes::from(self.output.encode()).write(buf);
    }
}

impl<M, O> EncodeSize for AppliedRecord<M, O>
where
    M: Encode,
    O: Encode,
{
    fn encode_size(&self) -> usize {
        self.sequence.encode_size()
            + self.cursor.encode_size()
            + self.payload_hash.encode_size()
            + Bytes::from(self.metadata.encode()).encode_size()
            + Bytes::from(self.output.encode()).encode_size()
    }
}

impl<M, O> Read for AppliedRecord<M, O>
where
    M: Decode<Cfg = ()>,
    O: Decode<Cfg = ()>,
{
    type Cfg = AppliedRecordCfg;

    fn read_cfg(buf: &mut impl Buf, cfg: &Self::Cfg) -> std::result::Result<Self, commonware_codec::Error> {
        let sequence = BatchNumber::read(buf)?;
        let cursor = BatchCursor::read(buf)?;
        let payload_hash = <[u8; 32]>::read(buf)?;
        let metadata = M::decode(Bytes::read_cfg(buf, &(0..=cfg.max_metadata_bytes).into())?)?;
        let output = O::decode(Bytes::read_cfg(buf, &(0..=cfg.max_output_bytes).into())?)?;
        Ok(Self {
            sequence,
            cursor,
            payload_hash,
            metadata,
            output,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc, time::Duration};

    use bytes::Bytes;
    use commonware_runtime::{deterministic, Runner};

    use super::*;
    use crate::{
        backend::MockBackend,
        config::{ChainConfig, ReaderConfig, VerificationMode},
        types::{compute_commitment, BlobCommitment, BlobRef, NamespaceId},
    };

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestMetadata(u64);

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestOutput(u64);

    impl Write for TestMetadata {
        fn write(&self, buf: &mut impl BufMut) {
            self.0.write(buf);
        }
    }

    impl FixedSize for TestMetadata {
        const SIZE: usize = u64::SIZE;
    }

    impl Read for TestMetadata {
        type Cfg = ();

        fn read_cfg(buf: &mut impl Buf, _: &()) -> std::result::Result<Self, commonware_codec::Error> {
            Ok(Self(u64::read(buf)?))
        }
    }

    impl Write for TestOutput {
        fn write(&self, buf: &mut impl BufMut) {
            self.0.write(buf);
        }
    }

    impl FixedSize for TestOutput {
        const SIZE: usize = u64::SIZE;
    }

    impl Read for TestOutput {
        type Cfg = ();

        fn read_cfg(buf: &mut impl Buf, _: &()) -> std::result::Result<Self, commonware_codec::Error> {
            Ok(Self(u64::read(buf)?))
        }
    }

    #[derive(Clone, Default)]
    struct RecordingApp {
        applied: Arc<Mutex<Vec<(BatchNumber, Bytes)>>>,
    }

    #[async_trait]
    impl ReplicaApplication for RecordingApp {
        type Metadata = TestMetadata;
        type Output = TestOutput;
        type Error = String;

        async fn apply_batch(
            &mut self,
            sequence: BatchNumber,
            payload: Bytes,
        ) -> std::result::Result<AppliedBatch<Self::Metadata, Self::Output>, Self::Error> {
            self.applied.lock().await.push((sequence, payload.clone()));
            Ok(AppliedBatch {
                metadata: TestMetadata(payload.len() as u64),
                output: TestOutput(sequence.0),
            })
        }
    }

    #[derive(Clone, Default)]
    struct MemorySource {
        head: Option<BatchNumber>,
        cursors: BTreeMap<BatchNumber, BatchCursor>,
        payloads: BTreeMap<BatchNumber, Bytes>,
    }

    #[async_trait]
    impl ReplicaSource for MemorySource {
        async fn head(&self) -> crate::Result<Option<BatchNumber>> {
            Ok(self.head)
        }

        async fn cursor(&self, sequence: BatchNumber) -> crate::Result<Option<BatchCursor>> {
            Ok(self.cursors.get(&sequence).cloned())
        }

        async fn payload(&self, sequence: BatchNumber) -> crate::Result<Option<Bytes>> {
            Ok(self.payloads.get(&sequence).cloned())
        }
    }

    fn namespace() -> NamespaceId {
        let mut ns = [0u8; 29];
        ns[19..].copy_from_slice(b"replica___");
        NamespaceId(ns)
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

    fn replica_config() -> ReplicaConfig {
        ReplicaConfig {
            partition: "replica".into(),
            max_payload_bytes: 1024,
            max_metadata_bytes: 64,
            max_output_bytes: 64,
        }
    }

    fn cursor(height: u64, sequence: u64, payload: &[u8]) -> BatchCursor {
        BatchCursor {
            sequence: BatchNumber(sequence),
            blob_ref: BlobRef {
                height,
                namespace: namespace(),
                commitment: compute_commitment(namespace(), payload).unwrap(),
            },
            payload_hash: payload_hash(payload),
        }
    }

    #[test]
    fn replica_state_codec_roundtrip() {
        let state = ReplicaState {
            next_sequence: BatchNumber(7),
        };
        let decoded = ReplicaState::decode(state.encode()).unwrap();
        assert_eq!(decoded, state);
    }

    #[test]
    fn apply_record_codec_roundtrip() {
        let record = AppliedRecord {
            sequence: BatchNumber(4),
            cursor: cursor(5, 4, b"payload"),
            payload_hash: payload_hash(b"payload"),
            metadata: TestMetadata(7),
            output: TestOutput(8),
        };
        let decoded = AppliedRecord::<TestMetadata, TestOutput>::decode_cfg(
            record.encode(),
            &AppliedRecordCfg {
                max_metadata_bytes: 64,
                max_output_bytes: 64,
            },
        )
        .unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn head_cursor_payload_source_behavior() {
        let source = MemorySource {
            head: Some(BatchNumber(2)),
            cursors: BTreeMap::from([(BatchNumber(0), cursor(1, 0, b"a"))]),
            payloads: BTreeMap::from([(BatchNumber(0), Bytes::from_static(b"a"))]),
        };
        let runner = deterministic::Runner::default();
        runner.start(|_| async move {
            assert_eq!(source.head().await.unwrap(), Some(BatchNumber(2)));
            assert!(source.cursor(BatchNumber(0)).await.unwrap().is_some());
            assert_eq!(source.payload(BatchNumber(0)).await.unwrap(), Some(Bytes::from_static(b"a")));
        });
    }

    #[test]
    fn missing_cursor_returns_noop_when_head_is_behind() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let app = RecordingApp::default();
            let replica = Replica::new(context, MockBackend::new(), reader_config(), replica_config(), app);
            let source = MemorySource { head: None, ..Default::default() };
            assert!(replica.sync_once(&source).await.unwrap().is_none());
        });
    }

    #[test]
    fn source_payload_is_preferred_over_da() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let app = RecordingApp::default();
            let applied = app.applied.clone();
            let backend = MockBackend::new();
            let expected = Bytes::from_static(b"payload");
            let batch_cursor = cursor(1, 0, expected.as_ref());
            backend.fail_next_gets(batch_cursor.blob_ref, 1);
            let replica = Replica::new(context, backend, reader_config(), replica_config(), app);
            let source = MemorySource {
                head: Some(BatchNumber(0)),
                cursors: BTreeMap::from([(BatchNumber(0), batch_cursor.clone())]),
                payloads: BTreeMap::from([(BatchNumber(0), expected.clone())]),
            };
            let batch = replica.sync_once(&source).await.unwrap().unwrap();
            assert_eq!(batch.payload, expected);
            assert_eq!(applied.lock().await.len(), 1);
        });
    }

    #[test]
    fn da_fallback_works_when_source_payload_missing() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let app = RecordingApp::default();
            let backend = MockBackend::new();
            let expected = Bytes::from_static(b"payload");
            let batch_cursor = cursor(1, 0, expected.as_ref());
            backend.insert_blob(batch_cursor.blob_ref, expected.clone());
            let replica = Replica::new(context, backend, reader_config(), replica_config(), app);
            let source = MemorySource {
                head: Some(BatchNumber(0)),
                cursors: BTreeMap::from([(BatchNumber(0), batch_cursor)]),
                payloads: BTreeMap::new(),
            };
            assert_eq!(replica.sync_once(&source).await.unwrap().unwrap().payload, expected);
        });
    }

    #[test]
    fn payload_hash_mismatch_is_rejected() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let app = RecordingApp::default();
            let replica = Replica::new(context, MockBackend::new(), reader_config(), replica_config(), app);
            let mut bad_cursor = cursor(1, 0, b"payload");
            bad_cursor.payload_hash = [9; 32];
            let source = MemorySource {
                head: Some(BatchNumber(0)),
                cursors: BTreeMap::from([(BatchNumber(0), bad_cursor)]),
                payloads: BTreeMap::from([(BatchNumber(0), Bytes::from_static(b"payload"))]),
            };
            let err = replica.sync_once(&source).await.unwrap_err();
            assert!(matches!(err, SequencerError::Core(Error::Decode(_))));
        });
    }

    #[test]
    fn duplicate_recovery_does_not_reapply() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let app = RecordingApp::default();
            let applied = app.applied.clone();
            let backend = MockBackend::new();
            let expected = Bytes::from_static(b"payload");
            let batch_cursor = cursor(1, 0, expected.as_ref());
            let replica = Replica::new(context, backend.clone(), reader_config(), replica_config(), app);
            backend.insert_blob(batch_cursor.blob_ref, expected.clone());
            let source = MemorySource {
                head: Some(BatchNumber(0)),
                cursors: BTreeMap::from([(BatchNumber(0), batch_cursor)]),
                payloads: BTreeMap::new(),
            };
            replica.sync_once(&source).await.unwrap();
            replica.recover().await.unwrap();
            replica.recover().await.unwrap();
            assert_eq!(applied.lock().await.len(), 1);
        });
    }

    #[test]
    fn restart_resumes_at_next_unapplied_sequence() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let context = Arc::new(context);
            let app = RecordingApp::default();
            let applied = app.applied.clone();
            let backend = MockBackend::new();
            let payload0 = Bytes::from_static(b"zero");
            let payload1 = Bytes::from_static(b"one_");
            let cursor0 = cursor(1, 0, payload0.as_ref());
            let cursor1 = cursor(2, 1, payload1.as_ref());
            backend.insert_blob(cursor0.blob_ref, payload0.clone());
            backend.insert_blob(cursor1.blob_ref, payload1.clone());
            let replica = Replica::from_shared(
                context.clone(),
                Reader::from_shared(context.clone(), backend.clone(), reader_config()),
                replica_config(),
                app,
            );
            let source = MemorySource {
                head: Some(BatchNumber(1)),
                cursors: BTreeMap::from([(BatchNumber(0), cursor0.clone()), (BatchNumber(1), cursor1.clone())]),
                payloads: BTreeMap::new(),
            };
            assert_eq!(replica.sync_once(&source).await.unwrap().unwrap().sequence, BatchNumber(0));

            let restarted = Replica::from_shared(
                context.clone(),
                Reader::from_shared(context, backend, reader_config()),
                replica_config(),
                RecordingApp { applied: applied.clone() },
            );
            let report = restarted.recover().await.unwrap();
            assert_eq!(report.next_sequence, BatchNumber(1));
        });
    }

    #[test]
    fn wrong_commitment_propagates() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let app = RecordingApp::default();
            let backend = MockBackend::new();
            let mut wrong = cursor(1, 0, b"payload");
            wrong.blob_ref.commitment = BlobCommitment([7; 32]);
            backend.insert_blob(wrong.blob_ref, Bytes::from_static(b"different"));
            let replica = Replica::new(context, backend, reader_config(), replica_config(), app);
            let source = MemorySource {
                head: Some(BatchNumber(0)),
                cursors: BTreeMap::from([(BatchNumber(0), wrong)]),
                payloads: BTreeMap::new(),
            };
            let err = replica.sync_once(&source).await.unwrap_err();
            assert!(matches!(err, SequencerError::Core(Error::CommitmentMismatch)));
        });
    }

    #[test]
    fn catch_up_replays_multiple_batches_in_order() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let app = RecordingApp::default();
            let applied = app.applied.clone();
            let backend = MockBackend::new();
            let payload0 = Bytes::from_static(b"zero");
            let payload1 = Bytes::from_static(b"one_");
            let cursor0 = cursor(1, 0, payload0.as_ref());
            let cursor1 = cursor(2, 1, payload1.as_ref());
            backend.insert_blob(cursor0.blob_ref, payload0.clone());
            backend.insert_blob(cursor1.blob_ref, payload1.clone());
            let replica = Replica::new(context, backend, reader_config(), replica_config(), app);
            let source = MemorySource {
                head: Some(BatchNumber(1)),
                cursors: BTreeMap::from([(BatchNumber(0), cursor0), (BatchNumber(1), cursor1)]),
                payloads: BTreeMap::new(),
            };
            let batches = replica.catch_up(&source).await.unwrap();
            assert_eq!(batches.len(), 2);
            let applied = applied.lock().await;
            assert_eq!(applied[0].0, BatchNumber(0));
            assert_eq!(applied[1].0, BatchNumber(1));
        });
    }

    #[test]
    fn repeated_recover_is_idempotent() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let replica = Replica::new(context, MockBackend::new(), reader_config(), replica_config(), RecordingApp::default());
            let first = replica.recover().await.unwrap();
            let second = replica.recover().await.unwrap();
            assert_eq!(first.next_sequence, BatchNumber(0));
            assert_eq!(second.next_sequence, BatchNumber(0));
        });
    }

    #[test]
    fn corrupted_cached_payload_is_rejected() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let context = Arc::new(context);
            let backend = MockBackend::new();
            let replica = Replica::from_shared(
                context.clone(),
                Reader::from_shared(context.clone(), backend, reader_config()),
                replica_config(),
                RecordingApp::default(),
            );
            replica.ensure_initialized().await.unwrap();
            replica.store.write_cache(BatchNumber(0), Bytes::from_static(b"bad")).await.unwrap();
            let expected = Bytes::from_static(b"payload");
            let batch_cursor = cursor(1, 0, expected.as_ref());
            let source = MemorySource {
                head: Some(BatchNumber(0)),
                cursors: BTreeMap::from([(BatchNumber(0), batch_cursor)]),
                payloads: BTreeMap::new(),
            };
            let err = replica.sync_once(&source).await.unwrap_err();
            assert!(matches!(err, SequencerError::Core(Error::Decode(_))));
        });
    }

    #[test]
    fn source_head_ahead_of_available_cursor_is_rejected() {
        let runner = deterministic::Runner::default();
        runner.start(|context| async move {
            let replica = Replica::new(context, MockBackend::new(), reader_config(), replica_config(), RecordingApp::default());
            let source = MemorySource {
                head: Some(BatchNumber(0)),
                cursors: BTreeMap::new(),
                payloads: BTreeMap::new(),
            };
            let err = replica.sync_once(&source).await.unwrap_err();
            assert!(matches!(err, SequencerError::Core(Error::Decode(_))));
        });
    }
}
