use std::fmt;

use bytes::{Buf, BufMut, Bytes};
use ::celestia_client::types::{Blob as CelestiaBlob, Commitment, nmt::Namespace};
use commonware_codec::{FixedSize, Read, ReadExt, Write};
use commonware_utils::sequence::Span;

use crate::{Error, Result};

pub const SUBMISSION_ID_LEN: usize = 32;
pub const NAMESPACE_ID_LEN: usize = 29;
pub const BLOB_COMMITMENT_LEN: usize = 32;
pub const BLOB_KEY_LEN: usize = 69;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubmissionId(pub [u8; SUBMISSION_ID_LEN]);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NamespaceId(pub [u8; NAMESPACE_ID_LEN]);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobCommitment(pub [u8; BLOB_COMMITMENT_LEN]);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobRef {
    pub height: u64,
    pub namespace: NamespaceId,
    pub commitment: BlobCommitment,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobKey(pub [u8; BLOB_KEY_LEN]);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishRequest {
    pub id: SubmissionId,
    pub payload: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishReceipt {
    pub id: SubmissionId,
    pub blob_ref: BlobRef,
    pub tx_hash: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verification {
    ProofVerified,
    RpcIncluded,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedBlob {
    pub blob_ref: BlobRef,
    pub payload: Bytes,
    pub verification: Verification,
}

impl NamespaceId {
    pub fn validate(&self) -> Result<()> {
        let namespace = Namespace::from_raw(&self.0).map_err(|_| Error::InvalidNamespace)?;
        if namespace.is_reserved() {
            return Err(Error::InvalidNamespace);
        }
        Ok(())
    }

    pub(crate) fn to_celestia(self) -> Result<Namespace> {
        self.validate()?;
        Namespace::from_raw(&self.0).map_err(|_| Error::InvalidNamespace)
    }
}

impl BlobRef {
    pub fn key(self) -> BlobKey {
        self.into()
    }
}

impl From<BlobRef> for BlobKey {
    fn from(value: BlobRef) -> Self {
        let mut bytes = [0u8; BLOB_KEY_LEN];
        bytes[..8].copy_from_slice(&value.height.to_be_bytes());
        bytes[8..37].copy_from_slice(&value.namespace.0);
        bytes[37..].copy_from_slice(&value.commitment.0);
        Self(bytes)
    }
}

impl From<BlobKey> for BlobRef {
    fn from(value: BlobKey) -> Self {
        let mut height = [0u8; 8];
        height.copy_from_slice(&value.0[..8]);

        let mut namespace = [0u8; NAMESPACE_ID_LEN];
        namespace.copy_from_slice(&value.0[8..37]);

        let mut commitment = [0u8; BLOB_COMMITMENT_LEN];
        commitment.copy_from_slice(&value.0[37..]);

        Self {
            height: u64::from_be_bytes(height),
            namespace: NamespaceId(namespace),
            commitment: BlobCommitment(commitment),
        }
    }
}

pub(crate) fn compute_commitment(namespace: NamespaceId, payload: &[u8]) -> Result<BlobCommitment> {
    let namespace = namespace.to_celestia()?;
    let blob = CelestiaBlob::new(namespace, payload.to_vec(), None)
        .map_err(|err| Error::Decode(err.to_string()))?;
    Ok(BlobCommitment(*blob.commitment.hash()))
}

pub(crate) fn validate_commitment(blob_ref: BlobRef, payload: &[u8]) -> Result<()> {
    let commitment = compute_commitment(blob_ref.namespace, payload)?;
    if commitment != blob_ref.commitment {
        return Err(Error::CommitmentMismatch);
    }
    Ok(())
}

pub(crate) fn celestia_commitment(commitment: BlobCommitment) -> Commitment {
    Commitment::new(commitment.0)
}

impl fmt::Display for BlobKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl Span for BlobKey {}

macro_rules! impl_fixed_codec {
    ($ty:ty, $size:expr, $field:tt) => {
        impl Write for $ty {
            fn write(&self, buf: &mut impl BufMut) {
                self.$field.write(buf);
            }
        }

        impl FixedSize for $ty {
            const SIZE: usize = $size;
        }

        impl Read for $ty {
            type Cfg = ();

            fn read_cfg(buf: &mut impl Buf, _: &()) -> std::result::Result<Self, commonware_codec::Error> {
                Ok(Self(<[u8; $size]>::read(buf)?))
            }
        }
    };
}

impl_fixed_codec!(SubmissionId, SUBMISSION_ID_LEN, 0);
impl_fixed_codec!(NamespaceId, NAMESPACE_ID_LEN, 0);
impl_fixed_codec!(BlobCommitment, BLOB_COMMITMENT_LEN, 0);
impl_fixed_codec!(BlobKey, BLOB_KEY_LEN, 0);

impl Write for BlobRef {
    fn write(&self, buf: &mut impl BufMut) {
        self.height.write(buf);
        self.namespace.write(buf);
        self.commitment.write(buf);
    }
}

impl FixedSize for BlobRef {
    const SIZE: usize = u64::SIZE + NamespaceId::SIZE + BlobCommitment::SIZE;
}

impl Read for BlobRef {
    type Cfg = ();

    fn read_cfg(buf: &mut impl Buf, _: &()) -> std::result::Result<Self, commonware_codec::Error> {
        Ok(Self {
            height: u64::read(buf)?,
            namespace: NamespaceId::read(buf)?,
            commitment: BlobCommitment::read(buf)?,
        })
    }
}

impl Write for PublishRequest {
    fn write(&self, buf: &mut impl BufMut) {
        self.id.write(buf);
        self.payload.write(buf);
    }
}

impl commonware_codec::EncodeSize for PublishRequest {
    fn encode_size(&self) -> usize {
        self.id.encode_size() + self.payload.encode_size()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PayloadCfg {
    pub max_payload_bytes: usize,
}

impl Read for PublishRequest {
    type Cfg = PayloadCfg;

    fn read_cfg(buf: &mut impl Buf, cfg: &Self::Cfg) -> std::result::Result<Self, commonware_codec::Error> {
        Ok(Self {
            id: SubmissionId::read(buf)?,
            payload: Bytes::read_cfg(buf, &(0..=cfg.max_payload_bytes).into())?,
        })
    }
}

impl Write for PublishReceipt {
    fn write(&self, buf: &mut impl BufMut) {
        self.id.write(buf);
        self.blob_ref.write(buf);
        self.tx_hash.write(buf);
    }
}

impl FixedSize for PublishReceipt {
    const SIZE: usize = SubmissionId::SIZE + BlobRef::SIZE + <[u8; 32]>::SIZE;
}

impl Read for PublishReceipt {
    type Cfg = ();

    fn read_cfg(buf: &mut impl Buf, _: &()) -> std::result::Result<Self, commonware_codec::Error> {
        Ok(Self {
            id: SubmissionId::read(buf)?,
            blob_ref: BlobRef::read(buf)?,
            tx_hash: <[u8; 32]>::read(buf)?,
        })
    }
}

impl Write for Verification {
    fn write(&self, buf: &mut impl BufMut) {
        let tag = match self {
            Self::ProofVerified => 0u8,
            Self::RpcIncluded => 1u8,
        };
        tag.write(buf);
    }
}

impl FixedSize for Verification {
    const SIZE: usize = u8::SIZE;
}

impl Read for Verification {
    type Cfg = ();

    fn read_cfg(buf: &mut impl Buf, _: &()) -> std::result::Result<Self, commonware_codec::Error> {
        match u8::read(buf)? {
            0 => Ok(Self::ProofVerified),
            1 => Ok(Self::RpcIncluded),
            other => Err(commonware_codec::Error::InvalidEnum(other)),
        }
    }
}

impl Write for VerifiedBlob {
    fn write(&self, buf: &mut impl BufMut) {
        self.blob_ref.write(buf);
        self.payload.write(buf);
        self.verification.write(buf);
    }
}

impl commonware_codec::EncodeSize for VerifiedBlob {
    fn encode_size(&self) -> usize {
        self.blob_ref.encode_size() + self.payload.encode_size() + self.verification.encode_size()
    }
}

impl Read for VerifiedBlob {
    type Cfg = PayloadCfg;

    fn read_cfg(buf: &mut impl Buf, cfg: &Self::Cfg) -> std::result::Result<Self, commonware_codec::Error> {
        Ok(Self {
            blob_ref: BlobRef::read(buf)?,
            payload: Bytes::read_cfg(buf, &(0..=cfg.max_payload_bytes).into())?,
            verification: Verification::read(buf)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_ref_key_roundtrip() {
        let blob_ref = BlobRef {
            height: 42,
            namespace: NamespaceId([1; NAMESPACE_ID_LEN]),
            commitment: BlobCommitment([2; BLOB_COMMITMENT_LEN]),
        };
        let key: BlobKey = blob_ref.into();
        assert_eq!(BlobRef::from(key), blob_ref);
    }

    #[test]
    fn namespace_validation_rejects_reserved() {
        assert!(NamespaceId([0; NAMESPACE_ID_LEN]).validate().is_err());
    }
}
