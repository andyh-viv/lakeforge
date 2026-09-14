//! Local on-disk layout for shuffle output.
//!
//! ```text
//! {work_dir}/{job_id}/stage-{stage_id}/map-{map_partition}/part-{output_partition}.arrow
//! ```

use std::path::{Path, PathBuf};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use forge_common::{ForgeError, Result};

#[derive(Debug, Clone)]
pub struct ShuffleStorage {
    root: PathBuf,
}

impl ShuffleStorage {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn job_dir(&self, job_id: &str) -> PathBuf {
        self.root.join(sanitize(job_id))
    }

    pub fn map_dir(&self, job_id: &str, stage_id: u32, map_partition: u32) -> PathBuf {
        self.job_dir(job_id)
            .join(format!("stage-{stage_id}"))
            .join(format!("map-{map_partition}"))
    }

    pub fn partition_path(
        &self,
        job_id: &str,
        stage_id: u32,
        map_partition: u32,
        output_partition: u32,
    ) -> PathBuf {
        self.map_dir(job_id, stage_id, map_partition)
            .join(format!("part-{output_partition}.arrow"))
    }

    /// Remove all data for a job, returning bytes freed.
    pub fn remove_job(&self, job_id: &str) -> Result<u64> {
        let dir = self.job_dir(job_id);
        if !dir.exists() {
            return Ok(0);
        }
        let bytes = dir_size(&dir);
        std::fs::remove_dir_all(&dir)?;
        Ok(bytes)
    }

    /// Open a partition file as an Arrow IPC stream reader.
    pub fn open(&self, path: &Path) -> Result<StreamReader<std::io::BufReader<std::fs::File>>> {
        let file = std::fs::File::open(path)
            .map_err(|e| ForgeError::Shuffle(format!("open {}: {e}", path.display())))?;
        StreamReader::try_new(std::io::BufReader::new(file), None)
            .map_err(|e| ForgeError::Shuffle(format!("read {}: {e}", path.display())))
    }
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
}

fn dir_size(path: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                total += dir_size(&p);
            } else if let Ok(md) = e.metadata() {
                total += md.len();
            }
        }
    }
    total
}

/// Buffered Arrow IPC writer for one output partition.
pub struct PartitionWriter {
    path: PathBuf,
    writer: Option<StreamWriter<std::io::BufWriter<std::fs::File>>>,
    schema: SchemaRef,
    pub num_rows: u64,
    pub num_batches: u32,
}

impl PartitionWriter {
    pub fn new(path: PathBuf, schema: SchemaRef) -> Self {
        Self { path, writer: None, schema, num_rows: 0, num_batches: 0 }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if self.writer.is_none() {
            if let Some(parent) = self.path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let file = std::fs::File::create(&self.path)?;
            let w = StreamWriter::try_new(std::io::BufWriter::new(file), &self.schema)?;
            self.writer = Some(w);
        }
        self.writer.as_mut().unwrap().write(batch)?;
        self.num_rows += batch.num_rows() as u64;
        self.num_batches += 1;
        Ok(())
    }

    /// Finish the stream and return bytes written (0 if no rows were written and
    /// therefore no file created).
    pub fn finish(mut self) -> Result<u64> {
        if let Some(mut w) = self.writer.take() {
            w.finish()?;
            let inner = w.into_inner()?;
            drop(inner);
            let len = std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0);
            Ok(len)
        } else {
            Ok(0)
        }
    }
}
