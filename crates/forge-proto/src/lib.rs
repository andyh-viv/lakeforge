//! Generated gRPC/protobuf types for the Forge distributed compute protocol.

pub mod forge {
    tonic::include_proto!("forge");
}

pub use forge::*;

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/s{}/p{}/a{}",
            self.job_id, self.stage_id, self.partition_id, self.attempt
        )
    }
}
