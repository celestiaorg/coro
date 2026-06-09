use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes};
use commonware_codec::{Decode, Encode, FixedSize, Read, ReadExt, Write};
use commonware_runtime::{Blob as RuntimeBlob, Storage};
use sha2::{Digest, Sha256};

use crate::{
    backend::BroadcastedSubmission,
    error::Result,
    types::{BlobRef, PublishReceipt, SubmissionId},
    Error,
};

const PENDING_PARTITION: &str = "pending";
const RECEIPTS_PARTITION: &str = "receipts";
const META_PARTITION: &str = "meta";
const STORE_VERSION_NAME: &[u8] = b"store-version";
const STORE_VERSION: u8 = 1;
const MAX_BROADCAST_TX_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct Store<S> {
    storage: Arc<S>,
    max_payload_bytes: usize,
    partition_prefix: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PendingRecord {
    pub id: SubmissionId,
    pub payload: Bytes,
    pub payload_hash: [u8; 32],
    pub stage: PendingStage,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PendingStage {
    AwaitingBroadcast,
    Broadcasted(BroadcastedSubmission),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReceiptRecord {
    pub id: SubmissionId,
    pub blob_ref: BlobRef,
    pub tx_hash: [u8; 32],
    pub payload_hash: [u8; 32],
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PendingRecordCfg {
    pub max_payload_bytes: usize,
    pub max_broadcast_tx_bytes: usize,
}

impl<S> Store<S>
where
    S: Storage + Send + Sync + 'static,
{
    pub(crate) fn new(storage: Arc<S>, max_payload_bytes: usize, partition_prefix: String) -> Self {
        Self {
            storage,
            max_payload_bytes,
            partition_prefix: partition_prefix.into_bytes(),
        }
    }

    pub(crate) async fn ensure_initialized(&self) -> Result<()> {
        let name = self.name(STORE_VERSION_NAME);
        let (blob, len) = self.storage.open(META_PARTITION, &name).await?;
        if len == 0 {
            blob.resize(1).await?;
            blob.write_at(0, Bytes::from_static(&[STORE_VERSION])).await?;
            blob.sync().await?;
        }
        Ok(())
    }

    pub(crate) async fn load_pending(&self, id: SubmissionId) -> Result<Option<PendingRecord>> {
        self.read_record(
            PENDING_PARTITION,
            self.name(&id.0),
            PendingRecordCfg {
                max_payload_bytes: self.max_payload_bytes,
                max_broadcast_tx_bytes: MAX_BROADCAST_TX_BYTES,
            },
        )
        .await
    }

    pub(crate) async fn write_pending(&self, record: &PendingRecord) -> Result<()> {
        self.write_record(PENDING_PARTITION, self.name(&record.id.0), record)
            .await
    }

    pub(crate) async fn delete_pending(&self, id: SubmissionId) -> Result<()> {
        let name = self.name(&id.0);
        self.storage.remove(PENDING_PARTITION, Some(&name)).await?;
        Ok(())
    }

    pub(crate) async fn pending_ids(&self) -> Result<Vec<SubmissionId>> {
        let mut names = self.storage.scan(PENDING_PARTITION).await?;
        names.sort();
        names.into_iter()
            .filter_map(|name| self.strip_name(&name).map(|stripped| stripped.to_vec()))
            .map(|name| {
                if name.len() != SubmissionId::SIZE {
                    return Err(Error::Decode("invalid pending record key length".into()));
                }
                let mut id = [0u8; SubmissionId::SIZE];
                id.copy_from_slice(&name);
                Ok(SubmissionId(id))
            })
            .collect()
    }

    pub(crate) async fn load_receipt(&self, id: SubmissionId) -> Result<Option<ReceiptRecord>> {
        self.read_record(RECEIPTS_PARTITION, self.name(&id.0), ()).await
    }

    pub(crate) async fn write_receipt(&self, record: &ReceiptRecord) -> Result<()> {
        self.write_record(RECEIPTS_PARTITION, self.name(&record.id.0), record)
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
        if name.len() <= prefix_len {
            return None;
        }
        if name[..prefix_len] != self.partition_prefix {
            return None;
        }
        if name[prefix_len] != 0 {
            return None;
        }
        Some(&name[prefix_len + 1..])
    }

    async fn read_record<T, Cfg, N>(
        &self,
        partition: &str,
        name: N,
        cfg: Cfg,
    ) -> Result<Option<T>>
    where
        T: Decode<Cfg = Cfg>,
        Cfg: Send,
        N: AsRef<[u8]>,
    {
        let (blob, len) = self.storage.open(partition, name.as_ref()).await?;
        if len == 0 {
            return Ok(None);
        }
        let mut data = blob.read_at(0, len as usize).await?;
        let bytes = data.copy_to_bytes(len as usize);
        Ok(Some(T::decode_cfg(bytes, &cfg)?))
    }

    async fn write_record<T, N>(&self, partition: &str, name: N, record: &T) -> Result<()>
    where
        T: Encode,
        N: AsRef<[u8]>,
    {
        let encoded = record.encode();
        let (blob, _len) = self.storage.open(partition, name.as_ref()).await?;
        blob.resize(encoded.len() as u64).await?;
        blob.write_at(0, encoded).await?;
        blob.sync().await?;
        Ok(())
    }
}

pub(crate) fn payload_hash(payload: &[u8]) -> [u8; 32] {
    let digest = Sha256::digest(payload);
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&digest);
    hash
}

impl PendingRecord {
    pub(crate) fn new(id: SubmissionId, payload: Bytes) -> Self {
        Self {
            id,
            payload_hash: payload_hash(payload.as_ref()),
            payload,
            stage: PendingStage::AwaitingBroadcast,
        }
    }
}

impl From<(PublishReceipt, [u8; 32])> for ReceiptRecord {
    fn from((receipt, payload_hash): (PublishReceipt, [u8; 32])) -> Self {
        Self {
            id: receipt.id,
            blob_ref: receipt.blob_ref,
            tx_hash: receipt.tx_hash,
            payload_hash,
        }
    }
}

impl From<ReceiptRecord> for PublishReceipt {
    fn from(value: ReceiptRecord) -> Self {
        Self {
            id: value.id,
            blob_ref: value.blob_ref,
            tx_hash: value.tx_hash,
        }
    }
}

impl Write for BroadcastedSubmission {
    fn write(&self, buf: &mut impl BufMut) {
        self.tx_hash.write(buf);
        self.tx_bytes.write(buf);
    }
}

impl commonware_codec::EncodeSize for BroadcastedSubmission {
    fn encode_size(&self) -> usize {
        self.tx_hash.encode_size() + self.tx_bytes.encode_size()
    }
}

impl Read for BroadcastedSubmission {
    type Cfg = usize;

    fn read_cfg(buf: &mut impl Buf, max_tx_bytes: &Self::Cfg) -> std::result::Result<Self, commonware_codec::Error> {
        Ok(Self {
            tx_hash: <[u8; 32]>::read(buf)?,
            tx_bytes: Bytes::read_cfg(buf, &(0..=*max_tx_bytes).into())?,
        })
    }
}

impl Write for PendingStage {
    fn write(&self, buf: &mut impl BufMut) {
        match self {
            Self::AwaitingBroadcast => 0u8.write(buf),
            Self::Broadcasted(submission) => {
                1u8.write(buf);
                submission.write(buf);
            }
        }
    }
}

impl commonware_codec::EncodeSize for PendingStage {
    fn encode_size(&self) -> usize {
        u8::SIZE
            + match self {
                Self::AwaitingBroadcast => 0,
                Self::Broadcasted(submission) => submission.encode_size(),
            }
    }
}

impl Read for PendingStage {
    type Cfg = usize;

    fn read_cfg(buf: &mut impl Buf, max_tx_bytes: &Self::Cfg) -> std::result::Result<Self, commonware_codec::Error> {
        match u8::read(buf)? {
            0 => Ok(Self::AwaitingBroadcast),
            1 => Ok(Self::Broadcasted(BroadcastedSubmission::read_cfg(
                buf,
                max_tx_bytes,
            )?)),
            other => Err(commonware_codec::Error::InvalidEnum(other)),
        }
    }
}

impl Write for PendingRecord {
    fn write(&self, buf: &mut impl BufMut) {
        self.id.write(buf);
        self.payload_hash.write(buf);
        self.payload.write(buf);
        self.stage.write(buf);
    }
}

impl commonware_codec::EncodeSize for PendingRecord {
    fn encode_size(&self) -> usize {
        self.id.encode_size()
            + self.payload_hash.encode_size()
            + self.payload.encode_size()
            + self.stage.encode_size()
    }
}

impl Read for PendingRecord {
    type Cfg = PendingRecordCfg;

    fn read_cfg(buf: &mut impl Buf, cfg: &Self::Cfg) -> std::result::Result<Self, commonware_codec::Error> {
        Ok(Self {
            id: SubmissionId::read(buf)?,
            payload_hash: <[u8; 32]>::read(buf)?,
            payload: Bytes::read_cfg(buf, &(0..=cfg.max_payload_bytes).into())?,
            stage: PendingStage::read_cfg(buf, &cfg.max_broadcast_tx_bytes)?,
        })
    }
}

impl Write for ReceiptRecord {
    fn write(&self, buf: &mut impl BufMut) {
        self.id.write(buf);
        self.blob_ref.write(buf);
        self.tx_hash.write(buf);
        self.payload_hash.write(buf);
    }
}

impl FixedSize for ReceiptRecord {
    const SIZE: usize = SubmissionId::SIZE + BlobRef::SIZE + <[u8; 32]>::SIZE + <[u8; 32]>::SIZE;
}

impl Read for ReceiptRecord {
    type Cfg = ();

    fn read_cfg(buf: &mut impl Buf, _: &()) -> std::result::Result<Self, commonware_codec::Error> {
        Ok(Self {
            id: SubmissionId::read(buf)?,
            blob_ref: BlobRef::read(buf)?,
            tx_hash: <[u8; 32]>::read(buf)?,
            payload_hash: <[u8; 32]>::read(buf)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use commonware_codec::{DecodeExt, Encode};

    use super::*;
    use crate::types::{BlobCommitment, NamespaceId};

    #[test]
    fn pending_record_roundtrip() {
        let record = PendingRecord {
            id: SubmissionId([7; 32]),
            payload: Bytes::from_static(b"payload"),
            payload_hash: payload_hash(b"payload"),
            stage: PendingStage::Broadcasted(BroadcastedSubmission {
                tx_hash: [3; 32],
                tx_bytes: Bytes::from_static(b"signed-tx"),
            }),
        };

        let bytes = record.encode();
        let decoded = PendingRecord::decode_cfg(
            bytes,
            &PendingRecordCfg {
                max_payload_bytes: 1024,
                max_broadcast_tx_bytes: MAX_BROADCAST_TX_BYTES,
            },
        )
        .unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn receipt_record_roundtrip() {
        let record = ReceiptRecord {
            id: SubmissionId([7; 32]),
            blob_ref: BlobRef {
                height: 9,
                namespace: NamespaceId([1; 29]),
                commitment: BlobCommitment([2; 32]),
            },
            tx_hash: [3; 32],
            payload_hash: [4; 32],
        };

        let bytes = record.encode();
        let decoded = ReceiptRecord::decode(bytes).unwrap();
        assert_eq!(decoded, record);
    }
}
