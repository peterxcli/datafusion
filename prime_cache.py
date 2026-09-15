from run_bench import ROOT, evict

evict()
buffer = bytearray(8 << 20)
total = 0
for path in sorted((ROOT / 'input').glob('*.parquet')):
    with path.open('rb', buffering=0) as source:
        while count := source.readinto(buffer):
            total += count
assert total == 14737666736, total
print('Primed full dataset on benchmark NUMA node:', total, 'bytes', flush=True)
