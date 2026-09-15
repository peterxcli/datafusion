#!/usr/bin/env bash
set -euo pipefail
ulimit -n 65536
root=/work/peterxcli/datafusion-io-stack-20260916
cd "$root"
exec > logs/benchmark-pipeline.log 2>&1
trap 'echo $? > benchmark-pipeline.exit' EXIT
python3 capture_environment.py
"$root/perf-package/usr/bin/numactl" --physcpubind=96-111 --membind=1 python3 prime_cache.py
python3 -u - <<'PY'
import json
from pathlib import Path
from run_bench import run, ROOT
from native_profiles import summarize
for stage in [0, 4]:
    run(stage, 20, 0, profile=True)
    path = ROOT / 'results' / f'profile-q20-s{stage}-lat0-12G-r0.json'
    record = json.loads(path.read_text())
    profile = summarize(record, path.with_suffix('.perf.txt'))
    cpu_ms = sum(thread['cpu_ms'] for thread in profile['threads'])
    ticks_ms = record['measured'][0]['cpu_ticks'] * 1000 / record['ticks_per_second']
    assert abs(cpu_ms - ticks_ms) <= max(30, ticks_ms * 0.05), (stage, cpu_ms, ticks_ms)
    print('Native profile validated:', stage, 'CPU ms', round(cpu_ms, 2), 'process ticks ms', ticks_ms, flush=True)
PY
python3 -u run_bench.py > logs/benchmark.log 2>&1
python3 analyze.py > logs/analysis.log 2>&1
plot-env/bin/python plot_report.py > logs/plots.log 2>&1
echo 'Benchmark, result checks, and all per-query charts completed'
