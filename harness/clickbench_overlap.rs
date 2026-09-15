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

//! Run the existing ClickBench driver on eight Tokio workers with optional park events.
use clap::Parser;
use datafusion::error::Result;
use datafusion_benchmarks::clickbench::RunOpt;
use datafusion_common_runtime::SpawnedTask;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
struct Options {
    #[command(flatten)]
    run: RunOpt,
    #[arg(long)]
    trace: Option<String>,
    #[arg(long, default_value_t = 0)]
    start_delay_ms: u64,
}
fn main() -> Result<()> {
    let opt = Options::parse();
    std::thread::sleep(std::time::Duration::from_millis(opt.start_delay_ms));
    let events = Arc::new(Mutex::new(Vec::new()));
    let index = AtomicUsize::new(0);
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .worker_threads(8)
        .enable_all()
        .thread_name_fn(move || format!("df-thread-{}", index.fetch_add(1, Ordering::Relaxed)));
    if opt.trace.is_some() {
        let parked = Arc::clone(&events);
        let active = Arc::clone(&events);
        builder.on_thread_park(move || record(&parked, "park"));
        builder.on_thread_unpark(move || record(&active, "unpark"));
    }
    let runtime = builder.build()?;
    let result = runtime.block_on(async {
        SpawnedTask::spawn(opt.run.run())
            .join_unwind()
            .await
            .unwrap()
    });
    drop(runtime);
    if let Some(path) = opt.trace {
        std::fs::write(path, serde_json::to_vec(&*events.lock().unwrap()).unwrap())?;
    }
    result
}
fn record(events: &Mutex<Vec<serde_json::Value>>, name: &str) {
    events.lock().unwrap().push(serde_json::json!({"name":name,"start_ns":SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),"duration_ns":0,"thread":std::thread::current().name().unwrap_or("main")}));
}
