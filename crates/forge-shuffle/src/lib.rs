//! Shuffle subsystem for the Forge engine.
//!
//! A distributed query is split into *stages* at exchange boundaries. Each
//! stage is executed as a set of independent tasks (one per input partition).
//! The output of a task is written to local disk as a set of Arrow IPC
//! streams, one per *output partition*, by [`ShuffleWriterExec`]. Downstream
//! stages read those partitions — locally or from remote executors over gRPC —
//! using [`ShuffleReaderExec`].
//!
//! [`UnresolvedShuffleExec`] is a placeholder inserted by the planner that the
//! scheduler replaces with a `ShuffleReaderExec` once the producing stage has
//! completed and the partition locations are known.

pub mod client;
pub mod codec;
pub mod reader;
pub mod storage;
pub mod unresolved;
pub mod writer;

pub use client::ShuffleClient;
pub use codec::ForgeCodec;
pub use reader::ShuffleReaderExec;
pub use storage::ShuffleStorage;
pub use unresolved::UnresolvedShuffleExec;
pub use writer::ShuffleWriterExec;

use std::sync::OnceLock;

/// The gRPC address (`host:port`) of the executor this process is running as.
/// Used by the reader to short-circuit local reads.
static LOCAL_EXECUTOR_ADDR: OnceLock<String> = OnceLock::new();

pub fn set_local_executor_addr(addr: impl Into<String>) {
    let _ = LOCAL_EXECUTOR_ADDR.set(addr.into());
}

pub fn local_executor_addr() -> Option<&'static str> {
    LOCAL_EXECUTOR_ADDR.get().map(|s| s.as_str())
}
