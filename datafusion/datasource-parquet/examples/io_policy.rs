// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Local-file I/O policy experiment. Run with a row-group count (default 64).
//! An optional second argument names a directory for sharing generated input files.
//! Existing files are reused and checked against the generated expected results.
//! Round zero warms each configuration. No artificial latency is injected.
//! Reader calls/ranges exclude internal footer reads; bytes_scanned includes them.
//! The pool peak covers prefetch reservations, not total scan memory.
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use datafusion_common::{Result, instant::Instant};
use datafusion_datasource::{
    PartitionedFile, file_groups::FileGroup, file_scan_config::FileScanConfigBuilder,
    source::DataSourceExec,
};
use datafusion_datasource_parquet::{
    DefaultParquetFileReaderFactory, ParquetFileReaderFactory, source::ParquetSource,
};
use datafusion_execution::{
    TaskContext,
    config::SessionConfig,
    memory_pool::{GreedyMemoryPool, MemoryPool, PeakRecordingPool},
    object_store::ObjectStoreUrl,
};
use datafusion_expr::Operator;
use datafusion_physical_expr::{
    PhysicalExpr,
    expressions::{BinaryExpr, Column, lit},
};
use datafusion_physical_plan::{
    ExecutionPlan, filter::FilterExec, metrics::ExecutionPlanMetricsSet,
    projection::ProjectionExec,
};
use futures::{StreamExt, future::BoxFuture};
use parquet::{
    arrow::{
        ArrowWriter, arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader,
    },
    basic::Compression,
    file::{
        metadata::ParquetMetaData,
        properties::{EnabledStatistics, WriterProperties},
        reader::{FileReader, SerializedFileReader},
    },
};
use std::{
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Debug, Default)]
struct Reads {
    calls: AtomicUsize,
    ranges: AtomicUsize,
}

struct Reader {
    inner: Box<dyn AsyncFileReader + Send>,
    reads: Arc<Reads>,
}
impl AsyncFileReader for Reader {
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.reads.calls.fetch_add(1, Ordering::Relaxed);
        self.reads.ranges.fetch_add(1, Ordering::Relaxed);
        self.inner.get_bytes(range)
    }
    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        self.reads.calls.fetch_add(1, Ordering::Relaxed);
        self.reads.ranges.fetch_add(ranges.len(), Ordering::Relaxed);
        self.inner.get_byte_ranges(ranges)
    }
    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        self.inner.get_metadata(options)
    }
}
#[derive(Debug)]
struct Factory {
    inner: DefaultParquetFileReaderFactory,
    reads: Arc<Reads>,
}
impl ParquetFileReaderFactory for Factory {
    fn create_reader(
        &self,
        partition: usize,
        file: PartitionedFile,
        hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        Ok(Box::new(Reader {
            inner: self.inner.create_reader(partition, file, hint, metrics)?,
            reads: Arc::clone(&self.reads),
        }))
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let groups: usize = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "64".into())
        .parse()?;
    assert!(groups > 0);
    let group_rows = 131_072;
    let schema = Arc::new(Schema::new(
        (0..8)
            .map(|i| Field::new(format!("c{i}"), DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let directory = tempfile::tempdir()?;
    let data_dir = std::env::args()
        .nth(2)
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| directory.path().to_path_buf());
    std::fs::create_dir_all(&data_dir)?;
    println!(
        "distribution,indexes,projection,mode,prefetch,round,file_bytes,elapsed_ms,rows,checksum,bytes_scanned,reader_calls,reader_ranges,page_rows_pruned,prefetch_peak,prefetch_bytes,prefetch_row_groups"
    );
    for clustered in [false, true] {
        for indexed in [false, true] {
            let path =
                data_dir.join(format!("scan-{clustered}-{indexed}-{groups}.parquet"));
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_dictionary_enabled(false)
                .set_max_row_group_row_count(Some(group_rows))
                .set_data_page_row_count_limit(1024)
                .set_statistics_enabled(if indexed {
                    EnabledStatistics::Page
                } else {
                    EnabledStatistics::Chunk
                })
                .set_offset_index_disabled(!indexed)
                .build();
            let mut writer = if path.exists() {
                None
            } else {
                Some(ArrowWriter::try_new(
                    std::fs::File::create(&path)?,
                    Arc::clone(&schema),
                    Some(props),
                )?)
            };
            let mut random = 42u64;
            let mut expected_rows = 0;
            let mut expected_wide = 0i64;
            let mut expected_narrow = 0i64;
            for group in 0..groups {
                let mut columns = Vec::new();
                for column in 0..8 {
                    columns.push(
                        (0..group_rows)
                            .map(|row| {
                                random ^= random << 13;
                                random ^= random >> 7;
                                random ^= random << 17;
                                if column == 0 {
                                    (group * group_rows + row) as i64
                                } else if column == 1 && clustered {
                                    (row * 1_000_000 / group_rows) as i64
                                } else {
                                    (random % 1_000_000) as i64
                                }
                            })
                            .collect::<Vec<_>>(),
                    );
                }
                for row in 0..group_rows {
                    if columns[1][row] < 10_000 {
                        expected_rows += 1;
                        expected_narrow = expected_narrow.wrapping_add(columns[0][row]);
                        for (i, column) in columns.iter().enumerate() {
                            if i != 1 {
                                expected_wide = expected_wide.wrapping_add(column[row]);
                            }
                        }
                    }
                }
                if let Some(writer) = &mut writer {
                    writer.write(&RecordBatch::try_new(
                        Arc::clone(&schema),
                        columns
                            .into_iter()
                            .map(|v| Arc::new(Int64Array::from(v)) as _)
                            .collect(),
                    )?)?;
                    writer.flush()?;
                }
            }
            let metadata = if let Some(writer) = writer {
                writer.close()?
            } else {
                SerializedFileReader::new(std::fs::File::open(&path)?)?
                    .metadata()
                    .clone()
            };
            assert_eq!(metadata.num_row_groups(), groups);
            for group in metadata.row_groups() {
                for column in group.columns() {
                    assert_eq!(column.column_index_offset().is_some(), indexed);
                    assert_eq!(column.offset_index_offset().is_some(), indexed);
                }
            }
            let size = std::fs::metadata(&path)?.len();
            for narrow in [false, true] {
                let output: Vec<usize> = if narrow {
                    vec![0]
                } else {
                    vec![0, 2, 3, 4, 5, 6, 7]
                };
                let mut expected_bytes = None;
                for round in 0..4 {
                    // Rotate all six configurations to reduce ordering bias.
                    for offset in 0..6 {
                        let case = (offset + round) % 6;
                        let mode = case / 2;
                        let budget = if case % 2 == 0 { 0 } else { 16 << 20 };
                        let recording = Arc::new(PeakRecordingPool::new(Arc::new(
                            GreedyMemoryPool::new(16 << 20),
                        )));
                        let pool: Arc<dyn MemoryPool> = Arc::clone(&recording) as _;
                        let reads = Arc::new(Reads::default());
                        let predicate = Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("c1", 1)),
                            Operator::Lt,
                            lit(10_000i64),
                        ))
                            as Arc<dyn PhysicalExpr>;
                        let source = ParquetSource::new(Arc::clone(&schema))
                            .with_row_group_prefetch(budget, Arc::clone(&pool))
                            .with_progressive_io(mode != 2)
                            .with_pushdown_filters(mode != 0)
                            .with_predicate(Arc::clone(&predicate))
                            .with_parquet_file_reader_factory(Arc::new(Factory {
                                inner: DefaultParquetFileReaderFactory::new(Arc::new(
                                    object_store::local::LocalFileSystem::new(),
                                )),
                                reads: Arc::clone(&reads),
                            }));
                        let scan_columns = if mode == 0 && narrow {
                            vec![0, 1]
                        } else if mode == 0 {
                            (0..8).collect()
                        } else {
                            output.clone()
                        };
                        let config = FileScanConfigBuilder::new(
                            ObjectStoreUrl::local_filesystem(),
                            Arc::new(source),
                        )
                        .with_file_group(FileGroup::new(vec![PartitionedFile::new(
                            path.to_str().unwrap(),
                            size,
                        )]))
                        .with_projection_indices(Some(scan_columns))?
                        .build();
                        let scan = Arc::new(DataSourceExec::new(Arc::new(config)));
                        let mut plan: Arc<dyn ExecutionPlan> = Arc::clone(&scan) as _;
                        if mode == 0 {
                            plan = Arc::new(FilterExec::try_new(predicate, plan)?);
                            plan = Arc::new(ProjectionExec::try_new(
                                output.iter().map(|&i| {
                                    (
                                        Arc::new(Column::new(&format!("c{i}"), i))
                                            as Arc<dyn PhysicalExpr>,
                                        format!("c{i}"),
                                    )
                                }),
                                plan,
                            )?);
                        }
                        let task = TaskContext::default().with_session_config(
                            SessionConfig::new().with_batch_size(8192),
                        );
                        let start = Instant::now();
                        let mut stream = plan.execute(0, Arc::new(task))?;
                        let (mut rows, mut checksum) = (0usize, 0i64);
                        while let Some(batch) = stream.next().await {
                            let batch = batch?;
                            rows += batch.num_rows();
                            for column in batch.columns() {
                                for value in column
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .unwrap()
                                    .values()
                                {
                                    checksum = checksum.wrapping_add(*value);
                                }
                            }
                        }
                        drop(stream);
                        let peak = recording.peak_reserved();
                        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                        assert_eq!(
                            (rows, checksum),
                            (
                                expected_rows,
                                if narrow {
                                    expected_narrow
                                } else {
                                    expected_wide
                                }
                            )
                        );
                        assert_eq!(pool.reserved(), 0);
                        let metrics = scan.metrics().unwrap();
                        let metric = |name| {
                            metrics.sum_by_name(name).map(|v| match v {
                                datafusion_physical_plan::metrics::MetricValue::PruningMetrics { pruning_metrics, .. } => pruning_metrics.pruned(),
                                v => v.as_usize(),
                            }).unwrap_or(0)
                        };
                        let bytes = metric("bytes_scanned");
                        assert_eq!(
                            bytes,
                            *expected_bytes.get_or_insert(bytes),
                            "I/O policy must preserve this fixture's page pruning without duplicate reads"
                        );
                        println!(
                            "{},{},{},{},{},{round},{size},{elapsed:.3},{rows},{checksum},{},{},{},{},{peak},{},{}",
                            if clustered { "clustered" } else { "random" },
                            indexed,
                            if narrow { "narrow" } else { "wide" },
                            ["off", "progressive", "upfront"][mode],
                            budget != 0,
                            metric("bytes_scanned"),
                            reads.calls.load(Ordering::Relaxed),
                            reads.ranges.load(Ordering::Relaxed),
                            metric("page_index_rows_pruned"),
                            metric("prefetch_bytes"),
                            metric("prefetch_row_groups")
                        );
                    }
                }
            }
        }
    }
    Ok(())
}
