use std::time::Duration;

use ::celestia_client::tx::TxConfig;

use crate::{error::Result, types::NamespaceId, Error};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainConfig {
    pub namespace: NamespaceId,
    pub max_payload_bytes: usize,
}

impl ChainConfig {
    pub fn validate(&self) -> Result<()> {
        self.namespace.validate()?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryConfig {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub max_attempts: Option<u32>,
}

impl RetryConfig {
    pub fn validate(&self) -> Result<()> {
        if self.initial_backoff.is_zero() || self.max_backoff.is_zero() {
            return Err(Error::Decode("retry backoff must be non-zero".into()));
        }
        if self.initial_backoff > self.max_backoff {
            return Err(Error::Decode(
                "initial backoff must be <= max backoff".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VerificationMode {
    ProofRequired,
    RpcIncluded,
}

#[derive(Clone, Debug)]
pub struct PublisherConfig {
    pub chain: ChainConfig,
    pub partition: String,
    pub retry: RetryConfig,
    pub readback_timeout: Duration,
    pub tx: TxConfig,
}

impl PublisherConfig {
    pub fn validate(&self) -> Result<()> {
        self.chain.validate()?;
        self.retry.validate()?;
        if self.partition.is_empty() {
            return Err(Error::Decode("publisher partition cannot be empty".into()));
        }
        if self.readback_timeout.is_zero() {
            return Err(Error::Decode("readback timeout must be non-zero".into()));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ReaderConfig {
    pub chain: ChainConfig,
    pub verification: VerificationMode,
    pub read_timeout: Duration,
}

impl ReaderConfig {
    pub fn validate(&self) -> Result<()> {
        self.chain.validate()?;
        if self.read_timeout.is_zero() {
            return Err(Error::Decode("read timeout must be non-zero".into()));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ResolverConfig {
    pub retry_interval: Duration,
    pub max_inflight: usize,
}

impl ResolverConfig {
    pub fn validate(&self) -> Result<()> {
        if self.retry_interval.is_zero() {
            return Err(Error::Decode("resolver retry interval must be non-zero".into()));
        }
        if self.max_inflight == 0 {
            return Err(Error::Decode("resolver max_inflight must be > 0".into()));
        }
        Ok(())
    }
}
