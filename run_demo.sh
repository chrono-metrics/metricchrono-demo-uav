#!/usr/bin/env bash
# Reproducible MetricChrono change-code replay on a public two-UAV recording.
#   bash run_demo.sh        # prove reproducibility live + render the alignment headline
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; cd "$HERE"
export MPLBACKEND=Agg
PY="${PYTHON:-python3}"
MODE="${1:-fast}"
mkdir -p out

case "$MODE" in
  fast)
    echo "▶ metricchrono-demo-uav — analysis reproducibility + alignment headline"
    "$PY" -m swarm_eval.impairment_analysis \
        --events data/events_impaired_circle.parquet --out out --progress
    "$PY" summarize.py
    ;;
  full)
    echo "The end-to-end path (rosbags -> MetricChrono engine -> seeded impairment -> baseline vs MC)"
    echo "requires the proprietary MetricChrono engine and the full ~15 GB CTU-MRS dataset — not in"
    echo "this repo. This public repo ships the engine's OUTPUT on the public recording plus the"
    echo "pure-pandas analysis. See README.md. Open-core primitive: github.com/chrono-metrics/metricchrono"
    exit 0
    ;;
  *) echo "usage: bash run_demo.sh [fast]" >&2; exit 2 ;;
esac
