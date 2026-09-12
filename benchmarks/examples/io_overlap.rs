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

//! Profile a complete filtered aggregate over an existing io_policy fixture.
//! Compare the same input and configuration with prefetch disabled and enabled.
//! The query runs on a Tokio worker so synchronous decoding exercises the same
//! scheduler as a query task. Optional events record runtime parking and reader
//! future lifetimes, which include scheduling delay and are not disk I/O times.
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::Int64Array;
use bytes::Bytes;
use clap::Parser;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::{
    DefaultParquetFileReaderFactory, ParquetFileReaderFactory,
};
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::*;
use datafusion_common::instant::Instant;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common_runtime::SpawnedTask;
use futures::future::BoxFuture;
use parquet::arrow::{arrow_reader::ArrowReaderOptions, async_reader::AsyncFileReader};
use parquet::file::metadata::ParquetMetaData;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
struct Options {
    #[arg(long)]
    path: String,
    #[arg(long, value_parser = ["off", "progressive", "upfront"])]
    mode: String,
    #[arg(long, default_value_t = 0)]
    prefetch_bytes: usize,
    #[arg(long, default_value_t = 8)]
    partitions: usize,
    #[arg(long, default_value_t = 8)]
    iterations: usize,
    #[arg(long, default_value_t = 0)]
    start_delay_ms: u64,
    #[arg(long)]
    trace: Option<String>,
    #[arg(long)]
    narrow: bool,
    #[arg(long)]
    expected_rows: i64,
    #[arg(long)]
    expected_checksum: i64,
}

fn timestamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

#[derive(Debug, Default)]
struct Trace(Mutex<Vec<serde_json::Value>>);
impl Trace {
    fn record(&self, name: &str, start: u128, duration: u128, partition: Option<usize>) {
        self.0.lock().unwrap().push(serde_json::json!({
            "name": name, "start_ns": start, "duration_ns": duration,
            "thread": std::thread::current().name().unwrap_or("main"), "partition": partition,
        }));
    }
}

struct Reader {
    inner: Box<dyn AsyncFileReader + Send>,
    trace: Arc<Trace>,
    partition: usize,
}
impl AsyncFileReader for Reader {
    fn get_bytes(
        &mut self,
        range: Range<u64>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(async move {
            let start = timestamp();
            let result = self.inner.get_bytes(range).await;
            self.trace
                .record("read", start, timestamp() - start, Some(self.partition));
            result
        })
    }
    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        Box::pin(async move {
            let start = timestamp();
            let result = self.inner.get_byte_ranges(ranges).await;
            self.trace
                .record("read", start, timestamp() - start, Some(self.partition));
            result
        })
    }
    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let start = timestamp();
            let result = self.inner.get_metadata(options).await;
            self.trace.record(
                "metadata",
                start,
                timestamp() - start,
                Some(self.partition),
            );
            result
        })
    }
}

#[derive(Debug)]
struct Factory {
    inner: DefaultParquetFileReaderFactory,
    trace: Arc<Trace>,
}
impl ParquetFileReaderFactory for Factory {
    fn create_reader(
        &self,
        partition: usize,
        file: PartitionedFile,
        hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion::error::Result<Box<dyn AsyncFileReader + Send>> {
        Ok(Box::new(Reader {
            inner: self.inner.create_reader(partition, file, hint, metrics)?,
            trace: Arc::clone(&self.trace),
            partition,
        }))
    }
}

fn main() -> datafusion::error::Result<()> {
    let opt = Options::parse();
    std::thread::sleep(std::time::Duration::from_millis(opt.start_delay_ms));
    let trace = opt.trace.as_ref().map(|_| Arc::new(Trace::default()));
    let path = opt.trace.clone();
    let thread_number = AtomicUsize::new(0);
    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    runtime
        .worker_threads(8)
        .thread_name_fn(move || {
            format!(
                "df-thread-{}",
                thread_number.fetch_add(1, Ordering::Relaxed)
            )
        })
        .enable_all();
    if let Some(trace) = &trace {
        let parked = Arc::clone(trace);
        let active = Arc::clone(trace);
        runtime.on_thread_park(move || parked.record("park", timestamp(), 0, None));
        runtime.on_thread_unpark(move || active.record("unpark", timestamp(), 0, None));
    }
    let runtime = runtime.build()?;
    let task_trace = trace.clone();
    let result = runtime.block_on(async {
        SpawnedTask::spawn(run(opt, task_trace))
            .join_unwind()
            .await
            .unwrap()
    });
    drop(runtime);
    if let (Some(trace), Some(path)) = (trace, path) {
        std::fs::write(path, serde_json::to_vec(&*trace.0.lock().unwrap()).unwrap())?;
    }
    result
}

async fn run(opt: Options, trace: Option<Arc<Trace>>) -> datafusion::error::Result<()> {
    assert!(opt.partitions > 0 && opt.iterations > 0);
    let mut config = SessionConfig::new()
        .with_target_partitions(opt.partitions)
        .with_batch_size(8192);
    config.options_mut().execution.parquet.pushdown_filters = opt.mode != "off";
    config.options_mut().execution.parquet.progressive_io = opt.mode != "upfront";
    let ctx = SessionContext::new_with_config(config);
    ctx.register_parquet("input", &opt.path, ParquetReadOptions::default())
        .await?;
    let runtime_metrics = tokio::runtime::Handle::current().metrics();
    let busy = || {
        (0..runtime_metrics.num_workers())
            .map(|i| runtime_metrics.worker_total_busy_duration(i))
            .collect::<Vec<_>>()
    };
    for iteration in 0..opt.iterations {
        let busy_before = busy();
        let start_ns = timestamp();
        let start = Instant::now();
        let projection = if opt.narrow {
            "c0"
        } else {
            "c0 + c2 + c3 + c4 + c5 + c6 + c7"
        };
        let dataframe = ctx.sql(&format!(
            "SELECT COUNT(*) AS rows, SUM({projection}) AS checksum FROM input WHERE c1 < 10000"
        )).await?;
        let task = Arc::new(dataframe.task_ctx());
        let mut plan = dataframe.create_physical_plan().await?;
        if opt.prefetch_bytes > 0 || trace.is_some() {
            plan = plan
                .transform_up(|plan| {
                    if let Some(exec) = plan.downcast_ref::<DataSourceExec>()
                        && let Some((config, source)) =
                            exec.downcast_to_file_source::<ParquetSource>()
                    {
                        let mut source = source.clone().with_row_group_prefetch(
                            opt.prefetch_bytes,
                            Arc::clone(&ctx.runtime_env().memory_pool),
                        );
                        if let Some(trace) = &trace {
                            source = source.with_parquet_file_reader_factory(Arc::new(
                                Factory {
                                    inner: DefaultParquetFileReaderFactory::new(
                                        Arc::new(
                                            object_store::local::LocalFileSystem::new(),
                                        ),
                                    ),
                                    trace: Arc::clone(trace),
                                },
                            ));
                        }
                        let config = FileScanConfigBuilder::from(config.clone())
                            .with_source(Arc::new(source))
                            .build();
                        return Ok(Transformed::yes(Arc::new(
                            exec.clone().with_data_source(Arc::new(config)),
                        )));
                    }
                    Ok(Transformed::no(plan))
                })?
                .data;
        }
        let batches = collect(Arc::clone(&plan), task).await?;
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        let worker_busy_ms = busy()
            .iter()
            .zip(&busy_before)
            .map(|(after, before)| {
                after.checked_sub(*before).unwrap().as_secs_f64() * 1000.0
            })
            .collect::<Vec<_>>();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let value = |i| {
            batches[0]
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert_eq!(
            (value(0), value(1)),
            (opt.expected_rows, opt.expected_checksum)
        );
        let mut counters = serde_json::Map::new();
        for name in [
            "bytes_scanned",
            "prefetch_bytes",
            "prefetch_row_groups",
            "prefetch_budget_skips",
            "prefetch_wait_time",
        ] {
            let mut total = 0;
            plan.apply(|plan| {
                if let Some(metric) =
                    plan.metrics().and_then(|metrics| metrics.sum_by_name(name))
                {
                    total += metric.as_usize();
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            counters.insert(name.into(), total.into());
        }
        println!(
            "{}",
            serde_json::json!({
                "pid": std::process::id(), "iteration": iteration, "start_ns": start_ns, "elapsed_ms": elapsed_ms,
                "rows": value(0), "checksum": value(1), "counters": counters, "worker_busy_ms": worker_busy_ms,
            })
        );
        if iteration == 0 {
            eprintln!("{}", displayable(plan.as_ref()).indent(true));
        }
    }
    Ok(())
}
