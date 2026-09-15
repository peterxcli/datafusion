#!/usr/bin/env bash
set -euo pipefail
ulimit -n 65536
root=/work/peterxcli/datafusion-io-stack-20260916
cd "$root"
exec > logs/cold-profile-pipeline.log 2>&1
trap 'echo $? > "$root/cold-profile-pipeline.exit"' EXIT
while [[ ! -f benchmark-pipeline.exit ]]; do sleep 15; done
test "$(cat benchmark-pipeline.exit)" = 0
python3 -u cold_profiles.py
python3 analyze.py > logs/analysis.log 2>&1
plot-env/bin/python plot_report.py > logs/plots.log 2>&1
echo 'Complete: all timing runs, warm CPU profiles, targeted evicted-cache CPU profiles, and charts'
