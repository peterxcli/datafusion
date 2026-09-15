"""Serial full-dataset comparisons for the cumulative Parquet PR stack."""
import json
import os
from pathlib import Path
import resource
import statistics
import subprocess
import time

ROOT = Path('/work/peterxcli/datafusion-io-stack-20260916')
OUT = ROOT / 'results'
OUT.mkdir(exist_ok=True)
(ROOT / 'spill').mkdir(exist_ok=True)
STAGES = list(range(6))
NAMES = ['baseline', 'policy', 'prefetch', 'early-prefetch', 'shared-queue', 'governor']

def evict():
    files = sorted((ROOT / 'input').glob('*.parquet'))
    assert len(files) == 100
    for path in files:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)

def cpu_ticks():
    return list(map(int, Path('/proc/stat').read_text().splitlines()[0].split()[1:9]))

def selected_cpu_ticks():
    rows = [line.split() for line in Path('/proc/stat').read_text().splitlines()]
    return {row[0]: list(map(int, row[1:9])) for row in rows if row[0] in {f'cpu{n}' for n in range(96, 112)}}

def clock_offset():
    samples = []
    for _ in range(9):
        begin = time.monotonic_ns()
        real = time.time_ns()
        end = time.monotonic_ns()
        samples.append(dict(offset_ns=real - (begin + end) // 2, uncertainty_ns=(end - begin) // 2))
    return min(samples, key=lambda sample: sample['uncertainty_ns'])

def arguments(stage, query, iterations, latency, memory):
    args = [str(ROOT / 'perf-package/usr/bin/numactl'), '--physcpubind=96-111', '--membind=1', str(ROOT / 'bin' / f'stage{stage}'),
            '--path', str(ROOT / 'input'), '--queries-path', str(ROOT / 'repo/benchmarks/queries/clickbench/queries'),
            '--query', str(query), '--partitions', '8', '--iterations', str(iterations),
            '--memory-limit', memory, '--mem-pool-type', 'fair', '--batch-size', '8192', '--pushdown', '--io-latency-ms', str(latency)]
    if stage >= 1:
        args += ['-c', 'datafusion.execution.parquet.progressive_io=false']
    if stage >= 2:
        args += ['--prefetch-bytes', '67108864']
    if stage >= 4:
        args += ['--scan-read-ahead-jobs', '32', '--scan-read-ahead-bytes', '536870912']
    if stage >= 5:
        args += ['--scan-read-ahead-governed']
    return args

def run(stage, query, repeat, cache='warm', latency=0, memory='12G', profile=False):
    prefix = ("profile-cold" if cache == "cold" else "profile") if profile else cache
    name = f'{prefix}-q{query:02d}-s{stage}-lat{latency}-{memory}-r{repeat}'
    path = OUT / f'{name}.json'
    if path.exists():
        return
    if cache == 'cold':
        evict()
    iterations = 1 if cache == 'cold' else (2 if profile else 4)
    args = arguments(stage, query, iterations, latency, memory)
    args += ['--output', str(OUT / f'{name}-benchmark.json')]
    clock_before = clock_offset()
    before_cpu = cpu_ticks()
    selected_before = selected_cpu_ticks()
    before_usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    start = time.monotonic()
    log_path = OUT / f'{name}.log'
    env = {k: v for k, v in os.environ.items() if not k.startswith('DATAFUSION_')}
    for key in ['DEBUG', 'SIMULATE_LATENCY', 'SORT_SPILL_RESERVATION_BYTES']:
        env.pop(key, None)
    env['TMPDIR'] = str(ROOT / 'spill')
    command = args
    if profile:
        env['LD_LIBRARY_PATH'] = str(ROOT / 'perf-package/usr/lib/x86_64-linux-gnu')
        command = [str(ROOT / 'perf-package/usr/bin/perf'), 'record', '--switch-events', '--sample-cpu',
                   '--clockid', 'CLOCK_MONOTONIC', '-e', 'dummy:u', '-m', '1024',
                   '-o', str(OUT / f'{name}.perf.data'), '--', *args]
    with log_path.open('w') as log:
        subprocess.run(['/usr/bin/time', '-f', '%M', '-o', str(OUT / f'{name}-rss.txt'), *command],
                       stdout=log, stderr=subprocess.STDOUT, env=env, cwd=ROOT, check=True, timeout=1800)
    clock_after = clock_offset()
    if profile:
        assert abs(clock_after['offset_ns'] - clock_before['offset_ns']) < 1_000_000
        with (OUT / f'{name}.perf.txt').open('w') as export:
            subprocess.run([str(ROOT / 'perf-package/usr/bin/perf'), 'script', '-i', str(OUT / f'{name}.perf.data'),
                            '--show-switch-events', '--show-task-events', '--ns', '-F', 'comm,pid,tid,cpu,time,event'],
                           stdout=export, env=env, check=True)
    after_usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    runs = [json.loads(line.removeprefix('PROFILE_QUERY ')) for line in log_path.read_text().splitlines()
            if line.startswith('PROFILE_QUERY ')]
    assert len(runs) == iterations, (name, len(runs), iterations)
    assert all(row['rows'] == len(row['values']) for row in runs)
    if query == 0:
        assert all(row['values'] == [['99997497']] for row in runs)
    summary = json.loads((OUT / f'{name}-benchmark.json').read_text())
    assert all(query['success'] for query in summary['queries']), name
    delta = [a - b for a, b in zip(cpu_ticks(), before_cpu)]
    selected_after = selected_cpu_ticks()
    selected_delta = {cpu: [a - b for a, b in zip(selected_after[cpu], ticks)] for cpu, ticks in selected_before.items()}
    measured = runs if cache == 'cold' else runs[1:]
    record = dict(stage=stage, name=NAMES[stage], query_id=query, repeat=repeat, cache=cache,
                  latency_ms=latency, memory_limit=memory, profile=profile, runs=runs, measured=measured,
                  command=args, process_seconds=time.monotonic() - start,
                  process_cpu_seconds=after_usage.ru_utime + after_usage.ru_stime - before_usage.ru_utime - before_usage.ru_stime,
                  peak_rss_kib=int((OUT / f'{name}-rss.txt').read_text()),
                  pool_peak_bytes=summary['queries'][0].get('pool_peak_bytes'),
                  selected_cpu_ticks=selected_delta, host_cpu_ticks=delta, host_steal_percent=100 * delta[7] / sum(delta),
                  ticks_per_second=os.sysconf('SC_CLK_TCK'), clock_before=clock_before, clock_after=clock_after)
    path.write_text(json.dumps(record) + '\n')
    (ROOT / 'benchmark.phase').write_text(name + '\n')
    print(name, round(statistics.median(r['elapsed_ms'] for r in measured), 2), 'ms', flush=True)

def main():
    assert (ROOT / 'dataset.ready').exists()
    prs = json.loads((ROOT / 'prs.json').read_text())
    assert len(prs) == 5, 'Open the requested PR stack before benchmarking'
    assert all((ROOT / f'stage{s}-validation.exit').read_text().strip() == '0' for s in STAGES[1:])
    for repeat in range(2):
        for query in (range(43) if repeat == 0 else reversed(range(43))):
            for stage in (STAGES if repeat == 0 else reversed(STAGES)):
                run(stage, query, repeat)
    (ROOT / 'warm.done').write_text('43 queries, six stages, two reversed-order passes\n')
    for repeat in range(3):
        for query in (range(43) if repeat % 2 == 0 else reversed(range(43))):
            for stage in (STAGES if repeat % 2 == 0 else reversed(STAGES)):
                run(stage, query, repeat, cache='cold')
    (ROOT / 'cold.done').write_text('43 queries, six stages, three evicted-cache runs\n')
    for repeat in range(2):
        for stage in (STAGES if repeat == 0 else reversed(STAGES)):
            for query in [20, 28]:
                run(stage, query, repeat, latency=8)
            run(stage, 34, repeat, memory='20G')
    for query in range(43):
        for stage in (STAGES if query % 2 == 0 else reversed(STAGES)):
            run(stage, query, 0, profile=True)
    (ROOT / 'benchmark.done').write_text('All timing and native CPU profile cases completed\n')

if __name__ == '__main__':
    main()
