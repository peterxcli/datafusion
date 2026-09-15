"""Read per-process PERF_RECORD_SWITCH events; never infer CPU activity from Tokio polls."""
from collections import defaultdict
import json
from pathlib import Path
import re

LINE = re.compile(r'^\s*(.*?)\s+(\d+)/(\d+)\s+\[(-?\d+)\]\s+(\d+)\.(\d+):\s+(.*)$')
COMM = re.compile(r'PERF_RECORD_COMM(?: exec)?: (.*):(\d+)/(\d+)$')

def merge(intervals):
    result = []
    for begin, end in sorted(intervals):
        if result and begin <= result[-1][1]:
            result[-1][1] = max(result[-1][1], end)
        else:
            result.append([begin, end])
    return result

def parse(path, pid):
    names, running, first = {}, {}, {}
    intervals = defaultdict(list)
    last = 0
    for line in Path(path).read_text().splitlines():
        if 'PERF_RECORD_LOST' in line:
            raise ValueError(f'Lost profiling records in {path}')
        match = LINE.match(line)
        if not match:
            continue
        name, event_pid, tid, cpu, seconds, fraction, event = match.groups()
        tid = int(tid)
        timestamp = int(seconds) * 1_000_000_000 + int(fraction.ljust(9, '0'))
        comm = COMM.search(event)
        if comm and int(comm[2]) == pid:
            names[int(comm[3])] = comm[1]
            first.setdefault(int(comm[3]), timestamp)
        if int(event_pid) != pid:
            continue
        last = max(last, timestamp)
        names.setdefault(tid, name)
        first.setdefault(tid, timestamp)
        if event.startswith('PERF_RECORD_SWITCH IN'):
            running[tid] = timestamp
        elif event.startswith('PERF_RECORD_SWITCH OUT'):
            begin = running.pop(tid, first[tid])
            if begin < timestamp:
                intervals[tid].append((begin, timestamp))
            first[tid] = timestamp
        elif event.startswith('PERF_RECORD_EXIT') and tid in running:
            intervals[tid].append((running.pop(tid), timestamp))
    for tid, begin in running.items():
        if begin < last:
            intervals[tid].append((begin, last))
    return names, intervals

def summarize(record, path):
    run = record['measured'][0]
    offset = record['clock_before']['offset_ns']
    begin = int(run['start_ns']) - offset
    end = begin + round(run['elapsed_ms'] * 1_000_000)
    names, intervals = parse(path, run['pid'])
    threads = []
    for tid, name in names.items():
        pieces = [[max(start, begin) - begin, min(stop, end) - begin]
                  for start, stop in intervals[tid] if start < end and stop > begin]
        if name.startswith('df-thread-'):
            index = int(name.removeprefix('df-thread-'))
            role = 'worker' if index < 8 else 'io'
        elif tid == run['pid']:
            index, role = -1, 'main'
        else:
            index, role = tid, 'other'
        threads.append(dict(tid=tid, name=name, role=role, index=index, intervals_ns=pieces,
                            cpu_ms=sum(stop - start for start, stop in pieces) / 1e6))
    workers = [thread for thread in threads if thread['role'] == 'worker']
    assert len(workers) == 8, (path, [thread['name'] for thread in threads])
    union = merge([piece for worker in workers for piece in worker['intervals_ns']])
    worker_cpu = sum(thread['cpu_ms'] for thread in workers)
    return dict(query_id=record['query_id'], stage=record['stage'], cache=record['cache'], elapsed_ms=run['elapsed_ms'],
                worker_cpu_ms=worker_cpu,
                worker_off_cpu_ms=8 * run['elapsed_ms'] - worker_cpu,
                all_workers_off_cpu_ms=run['elapsed_ms'] - sum(stop - start for start, stop in union) / 1e6,
                threads=sorted(threads, key=lambda thread: (thread['role'] != 'main', thread['index'])))

if __name__ == '__main__':
    import tempfile
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / 'trace.txt'
        path.write_text(''' worker 10/11 [096] 1.000000000: PERF_RECORD_COMM: df-thread-0:10/11
 worker 10/11 [096] 1.010000000: PERF_RECORD_SWITCH IN
 worker 10/11 [096] 1.030000000: PERF_RECORD_SWITCH OUT
 worker 10/11 [096] 1.050000000: PERF_RECORD_SWITCH IN
 worker 10/11 [096] 1.060000000: PERF_RECORD_EXIT(10:11):(1:1)
''')
        names, intervals = parse(path, 10)
        assert names[11] == 'df-thread-0'
        assert intervals[11] == [(1010000000, 1030000000), (1050000000, 1060000000)]
        assert merge([[0, 2], [1, 3], [4, 5]]) == [[0, 3], [4, 5]]
    print('Native context-switch parser checks passed')
