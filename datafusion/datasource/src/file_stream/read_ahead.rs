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

//! Experimental shared queue for preparing upcoming file-range scan jobs.

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_common_runtime::JoinSet;
use datafusion_execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion_physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder,
};
use futures::stream::BoxStream;
use futures::task::{ArcWake, waker_ref};
use futures::{FutureExt, StreamExt};
use parking_lot::Mutex;

use crate::PartitionedFile;
use crate::morsel::Morselizer;

const MIN_JOB_BYTES: usize = 16 * 1024 * 1024;

/// Execution-local scan backlog budget. This prototype is opt-in and not serialized.
#[derive(Debug)]
pub struct ReadAheadBudget {
    max_jobs: usize,
    max_bytes: usize,
    governed: bool,
    pool: Arc<dyn MemoryPool>,
    used: Mutex<usize>,
    peak_bytes: Gauge,
    peak_jobs: Gauge,
    admitted: Count,
    initial_bytes: Count,
    denied: Count,
    pub(crate) fallbacks: Count,
}

impl ReadAheadBudget {
    /// Construct a byte-bounded backlog. Governed mode additionally retains at
    /// most one sixteenth of pool headroom, leaving room for downstream growth.
    pub fn new(
        max_jobs: usize,
        max_bytes: usize,
        governed: bool,
        pool: Arc<dyn MemoryPool>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Arc<Self> {
        Arc::new(Self {
            max_jobs,
            max_bytes,
            governed,
            pool,
            used: Mutex::new(0),
            peak_bytes: MetricBuilder::new(metrics)
                .global_gauge("scan_read_ahead_peak_bytes"),
            peak_jobs: MetricBuilder::new(metrics)
                .global_gauge("scan_read_ahead_peak_jobs"),
            admitted: MetricBuilder::new(metrics).global_counter("scan_read_ahead_jobs"),
            initial_bytes: MetricBuilder::new(metrics)
                .global_counter("scan_read_ahead_initial_bytes"),
            denied: MetricBuilder::new(metrics)
                .global_counter("scan_read_ahead_budget_denials"),
            fallbacks: MetricBuilder::new(metrics)
                .global_counter("scan_read_ahead_demand_fallbacks"),
        })
    }

    fn limit(&self, used: usize) -> usize {
        if self.governed
            && let MemoryLimit::Finite(limit) = self.pool.memory_limit()
        {
            // ponytail: pool headroom is a coarse signal; use per-consumer pressure
            // when the memory-pool API exposes it.
            // Add our own reservation back when computing headroom so backlog
            // does not mistake its own allocation for downstream pressure.
            self.max_bytes.min(
                limit
                    .saturating_sub(self.pool.reserved())
                    .saturating_add(used)
                    / 16,
            )
        } else {
            self.max_bytes
        }
    }

    fn reserve(self: &Arc<Self>) -> Option<Arc<ReadAheadReservation>> {
        let reservation = Arc::new(ReadAheadReservation {
            budget: Arc::clone(self),
            reservation: MemoryConsumer::new("Scan read-ahead backlog")
                .register(&self.pool),
        });
        reservation.try_resize(MIN_JOB_BYTES).then_some(reservation)
    }
}

/// Reservation carried in a file extension through planning and initial data I/O.
/// It is released when the first row group's reader takes ownership of the bytes.
#[derive(Debug)]
pub struct ReadAheadReservation {
    budget: Arc<ReadAheadBudget>,
    reservation: MemoryReservation,
}

impl ReadAheadReservation {
    /// Record successfully prefetched initial payload bytes.
    pub fn record_read(&self, bytes: usize) {
        self.budget.initial_bytes.add(bytes);
    }

    /// Account for projected compressed bytes before reading them. Failure leaves
    /// the old reservation intact; the caller falls back to ordinary demand I/O.
    pub fn try_resize(&self, bytes: usize) -> bool {
        let bytes = bytes.max(MIN_JOB_BYTES);
        let mut used = self.budget.used.lock();
        let Some(target) = used
            .checked_sub(self.reservation.size())
            .and_then(|n| n.checked_add(bytes))
        else {
            self.budget.denied.add(1);
            return false;
        };
        if target > self.budget.limit(*used)
            || self.reservation.try_resize(bytes).is_err()
        {
            self.budget.denied.add(1);
            return false;
        }
        *used = target;
        self.budget.peak_bytes.set_max(target);
        true
    }
}

impl Drop for ReadAheadReservation {
    fn drop(&mut self) {
        let mut used = self.budget.used.lock();
        *used -= self.reservation.free();
    }
}

#[derive(Default)]
struct Waiters(Mutex<Vec<Waker>>);

impl ArcWake for Waiters {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let waiters = std::mem::take(&mut *arc_self.0.lock());
        for waker in waiters {
            waker.wake();
        }
    }
}

/// Completed tasks in JoinSet form the ready queue. Dropping the queue aborts
/// outstanding preparation and releases their reservations when cancellation runs.
pub(super) struct ReadAheadQueue {
    budget: Arc<ReadAheadBudget>,
    jobs: Mutex<JoinSet<Result<BoxStream<'static, Result<RecordBatch>>>>>,
    waiters: Arc<Waiters>,
}

impl fmt::Debug for ReadAheadQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadAheadQueue")
            .field("budget", &self.budget)
            .finish_non_exhaustive()
    }
}

impl ReadAheadQueue {
    pub(super) fn new(budget: Arc<ReadAheadBudget>) -> Self {
        Self {
            budget,
            jobs: Mutex::new(JoinSet::new()),
            waiters: Arc::default(),
        }
    }

    pub(super) fn fill(
        &self,
        files: &Mutex<VecDeque<PartitionedFile>>,
        morselizer: &Arc<dyn Morselizer>,
    ) {
        let mut jobs = self.jobs.lock();
        while jobs.len() < self.budget.max_jobs {
            if files.lock().is_empty() {
                break;
            }
            let Some(reservation) = self.budget.reserve() else {
                break;
            };
            let Some(mut file) = files.lock().pop_front() else {
                break;
            };
            file.extensions.insert_arc(reservation);
            let morselizer = Arc::clone(morselizer);
            jobs.spawn(async move { prepare_file(morselizer, file).await });
            self.budget.admitted.add(1);
            self.budget.peak_jobs.set_max(jobs.len());
        }
    }

    pub(super) fn demand_fallback(&self) {
        self.budget.fallbacks.add(1);
    }

    pub(super) fn poll_ready(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<BoxStream<'static, Result<RecordBatch>>>>> {
        {
            let mut waiters = self.waiters.0.lock();
            if !waiters.iter().any(|w| w.will_wake(cx.waker())) {
                waiters.push(cx.waker().clone());
            }
        }
        let waker = waker_ref(&self.waiters);
        let mut context = Context::from_waker(&waker);
        let mut jobs = self.jobs.lock();
        std::pin::pin!(jobs.join_next())
            .poll_unpin(&mut context)
            .map(|result| {
                result.map(|joined| {
                    joined.unwrap_or_else(|error| {
                        Err(DataFusionError::External(Box::new(error)))
                    })
                })
            })
    }
}

async fn prepare_file(
    morselizer: Arc<dyn Morselizer>,
    file: PartitionedFile,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let mut planners = VecDeque::from([morselizer.plan_file(file)?]);
    let mut morsels = Vec::new();
    while let Some(planner) = planners.pop_front() {
        if let Some(mut plan) = planner.plan()? {
            morsels.extend(plan.take_morsels());
            let mut children = plan.take_ready_planners();
            if let Some(pending) = plan.take_pending_planner() {
                children.push(pending.await?);
            }
            for child in children.into_iter().rev() {
                planners.push_front(child);
            }
        }
        tokio::task::yield_now().await;
    }
    Ok(futures::stream::iter(morsels)
        .flat_map(|m| m.into_stream())
        .boxed())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::morsel::{Morsel, MorselPlan, MorselPlanner};
    use datafusion_execution::memory_pool::GreedyMemoryPool;
    use tokio::sync::oneshot;

    #[test]
    fn budget_caps_backlog_and_releases_reservations() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(128 << 20));
        let budget = ReadAheadBudget::new(
            2,
            32 << 20,
            false,
            Arc::clone(&pool),
            &ExecutionPlanMetricsSet::new(),
        );
        let first = budget.reserve().unwrap();
        let second = budget.reserve().unwrap();
        assert!(budget.reserve().is_none());
        assert!(!first.try_resize(32 << 20));
        assert_eq!(pool.reserved(), 32 << 20);
        drop(second);
        assert!(first.try_resize(32 << 20));
        drop(first);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(*budget.used.lock(), 0);
    }

    #[test]
    fn budget_reacts_to_downstream_memory_and_releases_reservations() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 << 20));
        let budget = ReadAheadBudget::new(
            32,
            512 << 20,
            true,
            Arc::clone(&pool),
            &ExecutionPlanMetricsSet::new(),
        );
        let mut reservations: Vec<_> =
            (0..4).map(|_| budget.reserve().unwrap()).collect();
        assert_eq!(pool.reserved(), 64 << 20);
        assert!(budget.reserve().is_none());
        let downstream = MemoryConsumer::new("aggregation").register(&pool);
        downstream.try_grow(512 << 20).unwrap();
        assert!(budget.reserve().is_none());
        reservations.truncate(1);
        let additional = budget.reserve().unwrap();
        assert!(budget.reserve().is_none());
        drop((additional, reservations, downstream));
        assert_eq!(pool.reserved(), 0);
        assert_eq!(*budget.used.lock(), 0);
        assert!(budget.reserve().is_some());
    }

    #[derive(Debug)]
    struct GatedMorselizer(Mutex<VecDeque<oneshot::Receiver<()>>>);

    impl Morselizer for GatedMorselizer {
        fn plan_file(&self, file: PartitionedFile) -> Result<Box<dyn MorselPlanner>> {
            Ok(Box::new(GatedPlanner {
                file,
                gate: self.0.lock().pop_front(),
            }))
        }
    }

    #[derive(Debug)]
    struct GatedPlanner {
        file: PartitionedFile,
        gate: Option<oneshot::Receiver<()>>,
    }

    impl MorselPlanner for GatedPlanner {
        fn plan(self: Box<Self>) -> Result<Option<MorselPlan>> {
            let Self { file, gate } = *self;
            if let Some(gate) = gate {
                Ok(Some(MorselPlan::new().with_pending_planner(async move {
                    gate.await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    Ok(Box::new(Self { file, gate: None }) as Box<dyn MorselPlanner>)
                })))
            } else {
                Ok(Some(
                    MorselPlan::new().with_morsels(vec![Box::new(ReadyFile(file))]),
                ))
            }
        }
    }

    #[derive(Debug)]
    struct ReadyFile(PartitionedFile);

    impl Morsel for ReadyFile {
        fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>> {
            futures::stream::once(async move {
                let batch =
                    RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty()));
                drop(self.0);
                Ok(batch)
            })
            .boxed()
        }
    }

    #[tokio::test]
    async fn queue_returns_ready_work_and_cancels_remaining_io() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(128 << 20));
        let budget = ReadAheadBudget::new(
            2,
            32 << 20,
            false,
            Arc::clone(&pool),
            &ExecutionPlanMetricsSet::new(),
        );
        let queue = ReadAheadQueue::new(Arc::clone(&budget));
        let (first_tx, first_rx) = oneshot::channel();
        let (second_tx, second_rx) = oneshot::channel();
        let morselizer: Arc<dyn Morselizer> =
            Arc::new(GatedMorselizer(Mutex::new(VecDeque::from([
                first_rx, second_rx,
            ]))));
        let files = Mutex::new(VecDeque::from([
            PartitionedFile::new("first", 1),
            PartitionedFile::new("second", 1),
        ]));
        queue.fill(&files, &morselizer);
        assert!(files.lock().is_empty());
        assert_eq!(pool.reserved(), 32 << 20);
        second_tx.send(()).unwrap();
        let mut ready = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            futures::future::poll_fn(|cx| queue.poll_ready(cx)),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert!(ready.next().await.unwrap().is_ok());
        drop(ready);
        assert_eq!(pool.reserved(), 16 << 20);
        assert!(
            futures::future::poll_fn(|cx| queue.poll_ready(cx))
                .now_or_never()
                .is_none()
        );
        drop(queue);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while pool.reserved() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(first_tx.send(()).is_err());
    }
}
