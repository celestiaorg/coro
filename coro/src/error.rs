use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid namespace")]
    InvalidNamespace,
    #[error("payload exceeds configured maximum")]
    PayloadTooLarge,
    #[error("submission id already exists with different payload")]
    SubmissionConflict,
    #[error("blob not found")]
    NotFound,
    #[error("blob commitment mismatch")]
    CommitmentMismatch,
    #[error("inclusion verification failed")]
    InclusionVerificationFailed,
    #[error("unsupported verification mode")]
    UnsupportedVerificationMode,
    #[error("celestia submit error: {0}")]
    CelestiaSubmit(String),
    #[error("celestia read error: {0}")]
    CelestiaRead(String),
    #[error("decode error: {0}")]
    Decode(String),
    #[error("storage error: {0}")]
    Storage(commonware_runtime::Error),
    #[error("timeout")]
    Timeout,
}

impl From<commonware_codec::Error> for Error {
    fn from(value: commonware_codec::Error) -> Self {
        Self::Decode(value.to_string())
    }
}

impl From<commonware_runtime::Error> for Error {
    fn from(value: commonware_runtime::Error) -> Self {
        match value {
            commonware_runtime::Error::Timeout => Self::Timeout,
            other => Self::Storage(other),
        }
    }
}
