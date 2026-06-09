//! Commonware-native Celestia data availability integration.
//!
//! This crate publishes opaque payload bytes to Celestia DA, reads them back by
//! exact `height + namespace + commitment`, and exposes a Commonware resolver
//! keyed by fixed-width blob references.
//!
//! The crate is intentionally DA-only:
//! - it does not encode sequencer or host-chain semantics
//! - it does not provide archival indexing or namespace scans
//! - it does not replace the host chain's own historical storage
//!
//! **Trust Model**
//!
//! - [`VerificationMode::RpcIncluded`] verifies that Celestia returned payload bytes
//!   whose local blob commitment matches the trusted [`BlobRef`].
//! - [`VerificationMode::RpcIncluded`] does not locally prove that those shares were
//!   included under the block DA root; it trusts the node for inclusion at the given
//!   `height + namespace + commitment`.
//! - [`VerificationMode::ProofRequired`] locally verifies the returned share range
//!   against the block DA root and checks the blob commitment again locally.
//! - [`VerificationMode::ProofRequired`] still depends on the backend's header source
//!   for canonical-chain trust. This crate validates the returned header and its commit
//!   locally, but it does not maintain its own long-lived Celestia light-client trust
//!   store or application-specific trusted checkpoint.
//!
//! **Host Integration Pattern**
//!
//! - Host chains must keep their own archive and historical state.
//! - Host chains should store the canonical mapping from local chain history to
//!   [`BlobRef`].
//! - That local archive should be the primary source of truth; Celestia DA should be
//!   used as a recovery and availability path, not as the host chain's archive.
//!
//! **Single Sequencer**
//!
//! - [`single_sequencer`] builds a reusable higher layer on top of the DA-only
//!   publisher/reader/resolver stack in this crate.
//! - It provides deterministic ingress ordering, local batch archiving, durable
//!   `sequence -> BlobRef` mapping, and restart recovery for a single active writer.
//! - It does not add consensus, multi-writer coordination, validator-set logic, or
//!   chain-specific validation semantics.
//! - `submit()` records durable local acceptance only.
//! - Archived batches are a local soft-confirmation stage.
//! - Published batches with canonical [`BlobRef`]s are the replica-visible stage.
//!
//! **Trust Model**
//!
//! - The single sequencer layer is intentionally centralized: one operator is trusted
//!   to accept and order transactions.
//! - The local archive remains primary. Celestia DA is a fallback and recovery path.
//! - This crate can prove that bytes match a chosen [`BlobRef`] under the selected
//!   verification mode.
//! - Host applications are still responsible for validating that archived or DA-fetched
//!   bytes are a valid batch for their own state machine.
//!
//! **Replica Nodes**
//!
//! - Replica nodes trust the sequencer-defined canonical batch order.
//! - Replicas re-execute opaque batch payloads through host application logic.
//! - Local archive or trusted source payloads are primary.
//! - Celestia exact-ref reads are fallback and recovery only.
//! - This is not Celestia-as-consensus.
//!
//! This crate supports both [`VerificationMode::RpcIncluded`] and
//! [`VerificationMode::ProofRequired`] on the live backend, with the trust assumptions
//! above stated explicitly.

pub mod backend;
pub mod config;
pub mod error;
pub mod publisher;
pub mod reader;
pub mod resolver;
pub mod single_sequencer;
pub mod store;
pub mod types;

pub use config::{
    ChainConfig, PublisherConfig, ReaderConfig, ResolverConfig, RetryConfig, VerificationMode,
};
pub use error::{Error, Result};
pub use publisher::Publisher;
pub use reader::Reader;
pub use resolver::CelestiaResolver;
pub use single_sequencer::{
    Application as SingleSequencerApplication, ArchivedBatch, BatchCursor, BatchNumber,
    BatchPolicy, BatchStatus, ExecutedBatch, RecoveryReport, SequencerConfig, SequencerError,
    SingleSequencer,
    AppliedBatch as ReplicaAppliedBatch, Replica, ReplicaApplication, ReplicaBatch,
    ReplicaConfig, ReplicaRecoveryReport, ReplicaSource, ReplicaState,
};
pub use types::{
    BlobCommitment, BlobKey, BlobRef, NamespaceId, PublishReceipt, PublishRequest, SubmissionId,
    Verification, VerifiedBlob,
};
