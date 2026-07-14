> [!IMPORTANT] 
> This codebase has not been audited and is not intended for use in production, use at your own risk

# coro

`coro` is a Commonware-native Celestia data availability library with a minimal
single-sequencer stack built on top.

It is designed for applications that want to:

- publish opaque batch bytes to Celestia DA
- read them back by exact `height + namespace + commitment`
- run a single trusted sequencer that archives locally before DA publication
- run replicas that trust sequencer-defined canonical order and replay batches
- use local archive/history as primary, with Celestia as fallback and recovery

It is not:

- Celestia-as-consensus
- a namespace-scanning restore tool
- a sovereign rollup consensus core
- a host-chain state machine or account/signature framework

## Workspace Layout

- `coro/`: core library crate
- `coro-demo/`: minimal HTTP demo control plane for replica history fetches

## Core Layers

### DA Layer

The DA layer stays generic and payload-opaque:

- `publisher`
- `reader`
- `resolver`
- `store`
- `backend/*`

It only deals with opaque bytes and exact references:

- namespace
- blob commitment
- Celestia height

### Single Sequencer Layer

The `single_sequencer` module adds a reusable higher layer for:

- durable ingress acceptance
- deterministic batching
- host-defined execution
- local batch archive
- DA publication through the existing `Publisher`
- durable `BatchNumber -> BlobRef` mapping
- restart recovery for unpublished and partially finalized work

### Replica Layer

The replica layer lets applications replay canonical history from a trusted
source.

Replicas:

- trust sequencer-defined canonical order
- fetch canonical `BatchCursor`s from a `ReplicaSource`
- prefer locally served payloads or cache
- fall back to Celestia exact-ref reads through `Reader`
- verify payload hashes against canonical history
- replay through a host-provided `ReplicaApplication`
- persist apply progress and avoid duplicate application on restart

## Sequencer Semantics

`coro` now exposes the sequencing stages explicitly.

### Acceptance

`SingleSequencer::submit()` is a durable local acceptance boundary.

That means:

- ingress is durably stored locally
- the transaction is not yet batch-numbered public history
- it is not yet replica-visible canonical history

### Archived

After batching and host execution, a batch is archived locally before DA
publication.

This is a local soft-confirmation stage:

- it has a `BatchNumber`
- it is durably archived locally
- it may still have no published `BlobRef`
- replicas must not treat it as canonical head yet

### Published

After DA publication succeeds and the canonical `BatchCursor` is durable, the
batch is replica-visible canonical history.

Public APIs:

- `SingleSequencer::archive_ready()`
- `SingleSequencer::flush_archive()`
- `SingleSequencer::archived_head()`
- `SingleSequencer::published_head()`
- `SingleSequencer::status(sequence)`
- `SingleSequencer::unpublished_archived_batches()`
- `SingleSequencer::head()`

`head()` means published head.

`BatchStatus` is intentionally small:

- `Archived`
- `Published(BatchCursor)`

`SingleSequencer::process_ready()` and `SingleSequencer::flush()` keep the
existing compatibility behavior: they archive a batch and publish it before
returning a `BatchCursor`.

Hosts that want soft confirmations without blocking on DA can use the split
flow:

```rust
sequencer.submit(tx).await?;
let archived = sequencer.archive_ready().await?;
```

Then a background publisher can materialize canonical history with either:

- `SingleSequencer::publish_archived_sequence(sequence)` when Coro should use
  its configured `Publisher`, or
- `SingleSequencer::record_published_cursor(cursor)` when the host published
  the archived payload with its own DA submission strategy.

## HTTP Demo Control Plane

`coro-demo` provides a minimal HTTP `ReplicaSource` implementation and a tiny
history server for demos and example deployments.

Endpoints:

- `GET /healthz`
- `GET /head`
- `GET /archived-head`
- `GET /cursor/:sequence`
- `GET /status/:sequence`
- `GET /payload/:sequence`

Semantics:

- `/head` serves published head semantics only
- `/archived-head` is operator-facing soft-confirmation state
- `/cursor/:sequence` serves canonical published cursor metadata
- `/payload/:sequence` is convenience for local/archive serving and may return `404`
- replicas still fall back to DA exact-ref reads if payload bytes are unavailable

## Trust Model

### DA Verification

- `VerificationMode::RpcIncluded` checks that returned bytes match the trusted
  `BlobRef` commitment, but trusts the node for inclusion at the requested
  height/reference
- `VerificationMode::ProofRequired` additionally verifies inclusion against the
  Celestia DA root locally, while still depending on the backend header source
  for canonical-chain trust

### Sequencer Trust

- one operator is trusted to accept and order transactions
- local archive/history is primary
- Celestia is used for availability and recovery, not archival truth

### Replica Trust

- replicas trust sequencer-defined canonical order from their `ReplicaSource`
- replicas do not derive fork choice or order from Celestia scans
- host applications still validate whether batch bytes are valid for their own
  state machine

## Development

Run tests from the workspace root:

```bash
cargo test
```

Ignored live tests in `coro/tests/live_backend.rs` require local Celestia RPC and
gRPC endpoints plus signer environment variables.
