import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path('/work/peterxcli/datafusion-io-stack-20260916')

def output(*args):
    return subprocess.check_output(args, text=True).strip()

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

prs = json.loads((ROOT / 'prs.json').read_text())
assert len(prs) == 5 and (ROOT / 'remaining-stages.exit').read_text().strip() == '0'
assert (ROOT / 'harness-ready.exit').read_text().strip() == '0'
manifest = dict(baseline='c14976481ea53d7e1806eb9dbc1cff803c6eba31', prs=prs,
                host=output('hostname'), kernel=output('uname', '-a'), cpu=output('lscpu'),
                filesystem=output('findmnt', '-T', str(ROOT)), nvme_numa_node=Path('/sys/block/nvme0n1/device/numa_node').read_text().strip(),
                memory=Path('/proc/meminfo').read_text(), rust=output(str(ROOT / 'toolchain/bin/rustc'), '-Vv'),
                benchmark_cpus='96-111', numa_memory_node=1, workers=8, partitions=8,
                memory_pool='fair', memory_limit='12G', batch_size=8192, allocator='mimalloc', spill_directory=str(ROOT / 'spill'), prefetch_bytes=64<<20,
                queue_jobs=32, queue_bytes=512<<20, governor='min(hard cap, (pool limit - reserved + own bytes) / 16)',
                dataset_manifest_sha256=sha(ROOT / 'dataset-manifest.json'),
                validation_environment='private PID namespace, CPUs 96-111, process file limit 65536',
                binaries={str(stage): sha(ROOT / f'bin/stage{stage}') for stage in range(6)},
                validated_patch_sha256={str(stage): sha(ROOT / f'stage{stage}-validated.patch') for stage in range(1,6)},
                harness={str(stage): (ROOT / f'harness/stage{stage}-sha256.txt').read_text() for stage in range(6)})
(ROOT / 'environment.json').write_text(json.dumps(manifest, indent=2) + '\n')
print('Recorded environment, commits, dataset identity, and binary/harness hashes.')
