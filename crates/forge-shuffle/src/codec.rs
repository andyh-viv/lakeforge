//! `PhysicalExtensionCodec` that (de)serialises Forge's custom operators so
//! stage plans can be shipped from the driver to executors.

use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::{
    AsExecutionPlan, DefaultPhysicalProtoConverter, PhysicalExtensionCodec,
};
use datafusion_proto::protobuf;
use forge_proto::forge_exec_node::Node;
use forge_proto::{
    ForgeExecNode, PartitionLocations, ShuffleReaderExecNode, ShuffleWriterExecNode,
    UnresolvedShuffleExecNode,
};
use prost::Message;

use crate::{ShuffleReaderExec, ShuffleWriterExec, UnresolvedShuffleExec};

#[derive(Debug, Default, Clone)]
pub struct ForgeCodec;

impl ForgeCodec {
    /// Encode a whole plan to bytes.
    pub fn encode_plan(plan: Arc<dyn ExecutionPlan>) -> DFResult<Vec<u8>> {
        let node = protobuf::PhysicalPlanNode::try_from_physical_plan(plan, &ForgeCodec)?;
        Ok(node.encode_to_vec())
    }

    /// Decode a plan previously produced by [`Self::encode_plan`].
    pub fn decode_plan(bytes: &[u8], ctx: &TaskContext) -> DFResult<Arc<dyn ExecutionPlan>> {
        let node = protobuf::PhysicalPlanNode::decode(bytes)
            .map_err(|e| DataFusionError::Internal(format!("decode plan: {e}")))?;
        node.try_into_physical_plan(ctx, &ForgeCodec)
    }

    pub fn encode_schema(schema: &Schema) -> DFResult<Vec<u8>> {
        let s: protobuf::Schema = schema.try_into()?;
        Ok(s.encode_to_vec())
    }

    pub fn decode_schema(bytes: &[u8]) -> DFResult<SchemaRef> {
        let s = protobuf::Schema::decode(bytes)
            .map_err(|e| DataFusionError::Internal(format!("decode schema: {e}")))?;
        let schema: Schema = (&s).try_into()?;
        Ok(Arc::new(schema))
    }

    pub fn encode_partitioning(p: &Partitioning) -> DFResult<Vec<u8>> {
        let proto = serialize_partitioning(p, &ForgeCodec, &DefaultPhysicalProtoConverter {})?;
        Ok(proto.encode_to_vec())
    }

    pub fn decode_partitioning(
        bytes: &[u8],
        ctx: &TaskContext,
        input_schema: &Schema,
    ) -> DFResult<Option<Partitioning>> {
        if bytes.is_empty() {
            return Ok(None);
        }
        let proto = protobuf::Partitioning::decode(bytes)
            .map_err(|e| DataFusionError::Internal(format!("decode partitioning: {e}")))?;
        parse_protobuf_partitioning(
            Some(&proto),
            ctx,
            input_schema,
            &ForgeCodec,
            &DefaultPhysicalProtoConverter {},
        )
    }
}

impl PhysicalExtensionCodec for ForgeCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let node = ForgeExecNode::decode(buf)
            .map_err(|e| DataFusionError::Internal(format!("decode ForgeExecNode: {e}")))?;
        match node.node {
            Some(Node::Writer(w)) => {
                let input = inputs.first().cloned().ok_or_else(|| {
                    DataFusionError::Internal("ShuffleWriterExec requires an input".into())
                })?;
                let partitioning =
                    ForgeCodec::decode_partitioning(&w.partitioning, ctx, &input.schema())?;
                Ok(Arc::new(ShuffleWriterExec::new(
                    w.job_id,
                    w.stage_id,
                    input,
                    partitioning,
                    w.work_dir,
                )))
            }
            Some(Node::Reader(r)) => {
                let schema = ForgeCodec::decode_schema(&r.schema)?;
                let partitions = r.partitions.into_iter().map(|p| p.locations).collect();
                Ok(Arc::new(ShuffleReaderExec::new(r.job_id, r.stage_id, schema, partitions)))
            }
            Some(Node::Unresolved(u)) => {
                let schema = ForgeCodec::decode_schema(&u.schema)?;
                Ok(Arc::new(UnresolvedShuffleExec::new(
                    u.stage_id,
                    schema,
                    u.output_partition_count as usize,
                )))
            }
            None => Err(DataFusionError::Internal("empty ForgeExecNode".into())),
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> DFResult<()> {
        let any = node.as_any();
        let msg = if let Some(w) = any.downcast_ref::<ShuffleWriterExec>() {
            let partitioning = match w.shuffle_partitioning() {
                Some(p) => ForgeCodec::encode_partitioning(p)?,
                None => Vec::new(),
            };
            ForgeExecNode {
                node: Some(Node::Writer(ShuffleWriterExecNode {
                    job_id: w.job_id().to_string(),
                    stage_id: w.stage_id(),
                    partitioning,
                    work_dir: w.work_dir().to_string(),
                })),
            }
        } else if let Some(r) = any.downcast_ref::<ShuffleReaderExec>() {
            ForgeExecNode {
                node: Some(Node::Reader(ShuffleReaderExecNode {
                    job_id: r.job_id().to_string(),
                    stage_id: r.stage_id(),
                    schema: ForgeCodec::encode_schema(&r.schema())?,
                    partitions: r
                        .partitions()
                        .iter()
                        .map(|locs| PartitionLocations { locations: locs.clone() })
                        .collect(),
                })),
            }
        } else if let Some(u) = any.downcast_ref::<UnresolvedShuffleExec>() {
            ForgeExecNode {
                node: Some(Node::Unresolved(UnresolvedShuffleExecNode {
                    stage_id: u.stage_id(),
                    schema: ForgeCodec::encode_schema(&u.schema())?,
                    output_partition_count: u.output_partition_count() as u32,
                })),
            }
        } else {
            return Err(DataFusionError::NotImplemented(format!(
                "ForgeCodec cannot encode {}",
                node.name()
            )));
        };
        msg.encode(buf)
            .map_err(|e| DataFusionError::Internal(format!("encode ForgeExecNode: {e}")))
    }
}
