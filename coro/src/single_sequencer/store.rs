use std::sync::Arc;

use bytes::{Buf, Bytes};
use commonware_codec::{Decode, Encode};
use commonware_runtime::{Blob as RuntimeBlob, Storage};

use crate::{types::PayloadCfg, Error};

use super::{ArchiveRecordCfg, BatchCursor, BatchNumber};

const INGRESS_PARTITION: &str = "sequencer_ingress";
const ARCHIVE_PARTITION: &str = "sequencer_archive";
const REFS_PARTITION: &str = "sequencer_refs";
const META_PARTITION: &str = "sequencer_meta";
const STORE_VERSION_NAME: &[u8] = b"store-version";
const STORE_VERSION: u8 = 1;

#[derive(Clone, Debug, Default)]
pub(crate) struct StoreState {
    pub(crate) initialized: bool,
    pub(crate) next_ingress_position: u64,
    pub(crate) next_sequence: u64,
}

#[derive(Clone)]
pub(crate) struct SequencerStore<S, M> {
    storage: Arc<S>,
    partition_prefix: Vec<u8>,
    max_tx_bytes: usize,
    max_metadata_bytes: usize,
    max_payload_bytes: usize,
    _marker: std::marker::PhantomData<fn() -> M>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IngressRecord {
    pub(crate) accepted_at_ms: u64,
    pub(crate) tx: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ArchiveRecord<M> {
    pub(crate) sequence: BatchNumber,
    pub(crate) payload: Bytes,
    pub(crate) payload_hash: [u8; 32],
    pub(crate) metadata: M,
    pub(crate) blob_ref: Option<crate::BlobRef>,
    pub(crate) last_ingress_position: u64,
}

impl IngressRecord {
    pub(crate) fn new(accepted_at_ms: u64, tx: Bytes) -> Self {
        Self { accepted_at_ms, tx }
    }
}

impl<S, M> SequencerStore<S, M>
where
    S: Storage + Send + Sync + 'static,
    M: Clone + Decode<Cfg = ()> + Encode + Send + Sync + 'static,
{
    pub(crate) fn new(
        storage: Arc<S>,
        partition_prefix: String,
        max_tx_bytes: usize,
        max_metadata_bytes: usize,
        max_payload_bytes: usize,
    ) -> Self {
        Self {
            storage,
            partition_prefix: partition_prefix.into_bytes(),
            max_tx_bytes,
            max_metadata_bytes,
            max_payload_bytes,
            _marker: std::marker::PhantomData,
        }
    }

    pub(crate) async fn ensure_initialized(&self) -> crate::Result<()> {
        let name = self.name(STORE_VERSION_NAME);
        let (blob, len) = self.storage.open(META_PARTITION, &name).await?;
        if len == 0 {
            blob.resize(1).await?;
            blob.write_at(0, Bytes::from_static(&[STORE_VERSION])).await?;
            blob.sync().await?;
        }
        Ok(())
    }

    pub(crate) async fn load_state(&self) -> crate::Result<StoreState> {
        let next_ingress_position = self
            .ingress_records()
            .await?
            .last()
            .map(|(position, _)| position.saturating_add(1))
            .unwrap_or(0);
        let next_sequence = self.next_sequence_from_storage().await?.unwrap_or(0);
        Ok(StoreState {
            initialized: true,
            next_ingress_position,
            next_sequence,
        })
    }

    pub(crate) async fn next_sequence_from_storage(&self) -> crate::Result<Option<u64>> {
        Ok(self
            .archived_batches()
            .await?
            .last()
            .map(|record| record.sequence.0.saturating_add(1)))
    }

    pub(crate) async fn write_ingress(&self, position: u64, record: &IngressRecord) -> crate::Result<()> {
        self.write_record(INGRESS_PARTITION, self.name(&position.to_be_bytes()), record)
            .await
    }

    pub(crate) async fn ingress_records(&self) -> crate::Result<Vec<(u64, IngressRecord)>> {
        let mut names = self.scan_partition(INGRESS_PARTITION).await?;
        names.sort();
        let mut records = Vec::new();
        for name in names {
            let Some(key) = self.strip_name(&name) else {
                continue;
            };
            if key.len() != 8 {
                return Err(Error::Decode("invalid ingress key length".into()));
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(key);
            let position = u64::from_be_bytes(bytes);
            let record = self
                .read_record(
                    INGRESS_PARTITION,
                    self.name(&position.to_be_bytes()),
                    PayloadCfg {
                        max_payload_bytes: self.max_tx_bytes,
                    },
                )
                .await?
                .ok_or_else(|| Error::Decode("missing ingress record".into()))?;
            records.push((position, record));
        }
        Ok(records)
    }

    pub(crate) async fn delete_ingress_through(&self, position: u64) -> crate::Result<()> {
        for (existing, _) in self.ingress_records().await? {
            if existing > position {
                break;
            }
            self.storage
                .remove(INGRESS_PARTITION, Some(&self.name(&existing.to_be_bytes())))
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn write_archive(&self, record: &ArchiveRecord<M>) -> crate::Result<()> {
        self.write_record(ARCHIVE_PARTITION, self.name(&record.sequence.encode_key()), record)
            .await
    }

    pub(crate) async fn load_archive(&self, sequence: BatchNumber) -> crate::Result<Option<ArchiveRecord<M>>> {
        self.read_record(
            ARCHIVE_PARTITION,
            self.name(&sequence.encode_key()),
            ArchiveRecordCfg {
                max_metadata_bytes: self.max_metadata_bytes,
                max_payload_bytes: self.max_payload_bytes,
            },
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn delete_archive(&self, sequence: BatchNumber) -> crate::Result<()> {
        self.storage
            .remove(ARCHIVE_PARTITION, Some(&self.name(&sequence.encode_key())))
            .await?;
        Ok(())
    }

    pub(crate) async fn archived_batches(&self) -> crate::Result<Vec<ArchiveRecord<M>>> {
        let mut names = self.scan_partition(ARCHIVE_PARTITION).await?;
        names.sort();
        let mut records = Vec::new();
        for name in names {
            let Some(key) = self.strip_name(&name) else {
                continue;
            };
            if key.len() != 8 {
                return Err(Error::Decode("invalid archive key length".into()));
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(key);
            let sequence = BatchNumber(u64::from_be_bytes(bytes));
            let record = self
                .load_archive(sequence)
                .await?
                .ok_or_else(|| Error::Decode("missing archive record".into()))?;
            records.push(record);
        }
        Ok(records)
    }

    pub(crate) async fn archived_head(&self) -> crate::Result<Option<BatchNumber>> {
        Ok(self.archived_batches().await?.last().map(|record| record.sequence))
    }

    pub(crate) async fn published_head(&self) -> crate::Result<Option<BatchNumber>> {
        let archive_head = self
            .archived_batches()
            .await?
            .into_iter()
            .rev()
            .find(|record| record.blob_ref.is_some())
            .map(|record| record.sequence);
        let cursor_head = self.max_cursor_sequence().await?;
        Ok(match (archive_head, cursor_head) {
            (Some(a), Some(c)) => Some(std::cmp::max(a, c)),
            (Some(a), None) => Some(a),
            (None, Some(c)) => Some(c),
            (None, None) => None,
        })
    }

    pub(crate) async fn max_archived_ingress_position(&self) -> crate::Result<Option<u64>> {
        Ok(self
            .archived_batches()
            .await?
            .into_iter()
            .map(|record| record.last_ingress_position)
            .max())
    }

    pub(crate) async fn write_archive_blob_ref(
        &self,
        sequence: BatchNumber,
        blob_ref: crate::BlobRef,
    ) -> crate::Result<()> {
        let Some(mut record) = self.load_archive(sequence).await? else {
            return Err(Error::NotFound);
        };
        record.blob_ref = Some(blob_ref);
        self.write_archive(&record).await
    }

    pub(crate) async fn write_cursor(&self, cursor: &BatchCursor) -> crate::Result<()> {
        self.write_record(REFS_PARTITION, self.name(&cursor.sequence.encode_key()), cursor)
            .await
    }

    pub(crate) async fn load_cursor(&self, sequence: BatchNumber) -> crate::Result<Option<BatchCursor>> {
        self.read_record(REFS_PARTITION, self.name(&sequence.encode_key()), ())
            .await
    }

    async fn max_cursor_sequence(&self) -> crate::Result<Option<BatchNumber>> {
        let mut names = self.scan_partition(REFS_PARTITION).await?;
        names.sort();
        let Some(name) = names.last() else {
            return Ok(None);
        };
        let Some(key) = self.strip_name(name) else {
            return Ok(None);
        };
        if key.len() != 8 {
            return Err(Error::Decode("invalid cursor key length".into()));
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(key);
        Ok(Some(BatchNumber(u64::from_be_bytes(bytes))))
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

    async fn scan_partition(&self, partition: &str) -> crate::Result<Vec<Vec<u8>>> {
        match self.storage.scan(partition).await {
            Ok(names) => Ok(names),
            Err(commonware_runtime::Error::PartitionMissing(_)) => Ok(Vec::new()),
            Err(err) => Err(err.into()),
        }
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
