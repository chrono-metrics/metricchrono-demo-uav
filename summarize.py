#!/usr/bin/env python3
"""Summarize the deterministic-replay result + the alignment headline.

- The **fast** path (`run_demo.sh`) proves the change-code *analysis* is bit-reproducible
  on the bundled MetricChrono output (`out/determinism.json`, recomputed live), and
  fingerprints the shipped MC correction column. It does NOT re-execute the encoder.
- The alignment **headline** numbers are read from the committed FULL two-flight run
  (`data/metrics_table.csv`); the bundled `data/events_impaired_circle.parquet` is a
  single-flight (circle) subset (internal decision signals dropped) used for the live
  determinism proof + figure rendering.

Pure stdlib + numpy/pandas/matplotlib. The MetricChrono engine that produced the
corrections is proprietary and is not in this repo (see README / NOTICE).
"""
from __future__ import annotations

import hashlib
import json
import pathlib
import sys

import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = pathlib.Path(__file__).resolve().parent
OUT = ROOT / "out"
IMPAIRED = ROOT / "data" / "events_impaired_circle.parquet"
METRICS_TABLE = ROOT / "data" / "metrics_table.csv"
MEETING = ROOT / "data" / "meeting_summary.csv"
CAMERA = "topic:/mv_25003659/image_raw/compressed"
SLAM = "topic:/uav12/slam/odom"
MC_CODE_COLS = ["mc_correction_ns_effective"]   # the only correction column shipped


def _pct(base: float, mc: float) -> float:
    return (base - mc) / base * 100.0 if base else float("nan")


def _short(s: str) -> str:
    t = s.replace("topic:", "").strip("/")
    if "image_raw" in t:
        return "camera image_raw"
    p = t.split("/")
    return f"{p[0]} {'/'.join(p[-2:])}" if len(p) >= 2 else t


def _mc_code_fingerprint() -> tuple[str, list[str]]:
    import pyarrow.parquet as pq
    have = [c for c in MC_CODE_COLS if c in pq.ParquetFile(IMPAIRED).schema.names]
    if not have:
        return "(no MC code columns)", []
    df = pd.read_parquet(IMPAIRED, columns=have)
    h = hashlib.sha256()
    for c in have:
        h.update(c.encode())
        h.update(np.ascontiguousarray(pd.to_numeric(df[c], errors="coerce").to_numpy(np.float64)).tobytes())
    return h.hexdigest(), have


def main() -> int:
    det_path = OUT / "determinism.json"
    if not det_path.exists():
        print(f"!! {det_path} not found — run `bash run_demo.sh` first.", file=sys.stderr)
        return 2
    det = json.loads(det_path.read_text())
    hm = det.get("hash_match", {})
    n_match = sum(1 for v in hm.values() if v); n_tot = len(hm)
    shuf = det.get("shuffle", {})
    det_ok = n_match == n_tot and n_tot > 0
    shuffle_moves = bool(shuf.get("out_of_order_changed")) or float(shuf.get("alignment_delta", 0) or 0) > 0
    fp, _ = _mc_code_fingerprint()

    t = pd.read_csv(METRICS_TABLE)

    def cell(section, metric):
        r = t[(t["section"] == section) & (t["metric"] == metric)]
        return (float(r.iloc[0]["impaired_baseline"]), float(r.iloc[0]["impaired_mc"])) if not r.empty else (float("nan"), float("nan"))

    ov_mean = cell("overall", "alignment_error_ms_mean")
    ov_p99 = cell("overall", "alignment_error_ms_p99")
    ov_p95 = cell("overall", "alignment_error_ms_p95")
    cam_p50 = cell(CAMERA, "alignment_error_ms_p50")
    slam_p50 = cell(SLAM, "alignment_error_ms_p50")
    bw = float("nan")
    if MEETING.exists():
        m = pd.read_csv(MEETING); r = m[m["metric_name"] == "bandwidth_overhead_pct"]
        if not r.empty:
            bw = float(r.iloc[0]["mc_value"])

    tp = t[(t["metric"] == "alignment_error_ms_p50") & (t["section"].str.startswith("topic:"))].copy()
    tp["base"] = tp["impaired_baseline"].astype(float); tp["mc"] = tp["impaired_mc"].astype(float)
    tp = tp[np.isfinite(tp["base"]) & np.isfinite(tp["mc"])].sort_values("base", ascending=False).head(10)

    OUT.mkdir(parents=True, exist_ok=True)
    fig, (axA, axB) = plt.subplots(1, 2, figsize=(12, 4.6), gridspec_kw={"width_ratios": [1, 1.5]})
    x = np.arange(3); bv = [ov_mean[0], ov_p95[0], ov_p99[0]]; mv = [ov_mean[1], ov_p95[1], ov_p99[1]]
    axA.bar(x - 0.2, bv, 0.4, label="Baseline (naive impaired local clock)", color="#c0392b")
    axA.bar(x + 0.2, mv, 0.4, label="MetricChrono (enterprise ref)", color="#2471a3")
    for i, (b, m) in enumerate(zip(bv, mv)):
        axA.text(i, max(b, m) + 1.5, f"−{_pct(b, m):.0f}%", ha="center", fontsize=9, fontweight="bold")
    axA.set_xticks(x); axA.set_xticklabels(["all-stream\nmean", "all-stream\np95", "all-stream\np99"])
    axA.set_ylabel("Time-alignment error to synced reference (ms)"); axA.set_title("Overall — lower is better")
    axA.legend(fontsize=8, loc="upper left"); axA.grid(axis="y", alpha=0.25)
    y = np.arange(len(tp))[::-1]
    axB.barh(y + 0.2, tp["base"], 0.4, label="Baseline", color="#c0392b")
    axB.barh(y - 0.2, tp["mc"], 0.4, label="MetricChrono", color="#2471a3")
    axB.set_yticks(y); axB.set_yticklabels([_short(s) for s in tp["section"]], fontsize=8)
    axB.set_xlabel("Alignment error p50 (ms)"); axB.set_title("Worst streams (p50)")
    axB.legend(fontsize=8, loc="lower right"); axB.grid(axis="x", alpha=0.25)
    fig.suptitle("MetricChrono on a public two-UAV recording under SEEDED synthetic "
                 "clock-drift / out-of-order / dropout", fontsize=11)
    fig.tight_layout(rect=(0, 0, 1, 0.95)); fig.savefig(OUT / "hero_alignment.png", dpi=160); plt.close(fig)

    L = []; P = L.append
    P("# Results\n")
    P("_Reproducible change-code replay on a **public** two-UAV ROS 2/MAVROS recording "
      "(CTU-MRS, arXiv:2306.17544) under **seeded** clock-drift, out-of-order delivery and dropout. "
      "Corrections were produced by the MetricChrono **enterprise** engine (not in this repo); this "
      "repo ships that output + a pure-pandas analysis._\n")
    P("## 1. Reproducible (live, this run — on the bundled circle subset)")
    P(f"- **{n_match}/{n_tot} analysis outputs byte-identical** across two runs (SHA-256). "
      f"{'OK' if det_ok else 'MISMATCH'}  _(analysis reproducibility on the bundled MC output — not a "
      "re-execution of the proprietary encoder.)_")
    P(f"- **MC change-code fingerprint** (SHA-256 of `mc_correction_ns_effective`): `{fp[:16]}…`")
    P(f"- **Shuffle control:** perturb arrival order within {float(shuf.get('bin_ns',0))/1e9:.1f}s bins → "
      f"output changes (Δ out-of-order {float(shuf.get('out_of_order_delta',float('nan')))/1e6:.0f} ms): "
      f"the analysis is genuinely order-sensitive. {'OK' if shuffle_moves else 'WEAK'}\n")
    P("## 2. Time-alignment error to the synchronized reference (committed FULL two-flight run)")
    P("_Per-event `|corrected − synced-reference|`, pooled across streams; from `data/metrics_table.csv`. "
      "Baseline = naive impaired local clock (`t_obs`). The bundled parquet is the circle subset only._\n")
    P("| metric | baseline | MetricChrono | improvement |")
    P("|---|--:|--:|--:|")
    P(f"| all-stream mean | {ov_mean[0]:.1f} ms | {ov_mean[1]:.1f} ms | **−{_pct(*ov_mean):.0f}%** |")
    P(f"| all-stream p99 | {ov_p99[0]:.1f} ms | {ov_p99[1]:.1f} ms | −{_pct(*ov_p99):.0f}% |")
    P(f"| camera stream p50 | {cam_p50[0]:.1f} ms | {cam_p50[1]:.1f} ms | **−{_pct(*cam_p50):.0f}%** |")
    P(f"| uav12 SLAM-odom p50 | {slam_p50[0]:.1f} ms | {slam_p50[1]:.1f} ms | −{_pct(*slam_p50):.0f}% |\n")
    P(f"## 3. Cost\n- Reconciliation **bandwidth overhead ≈ {bw:.2f}%** of payload.\n")
    P("## Caveats\n- **In-sample** (calibration fit on the reference timeline, holdout off) and the "
      "baseline is a naive impaired clock — not a tuned aligner. Out-of-sample + header-stamp-baseline "
      "validation needs the full engine.\n- The corrections come from the **enterprise** engine, not the "
      "Apache-2.0 open core (which ships the comparator/ladder primitive).\n")
    P("> MetricChrono is a measurement/evidence layer (deterministic time-alignment & change-coding). "
      "It is not an autonomy, targeting, navigation, or weapons system and does not certify safety or "
      "mission assurance.")
    (ROOT / "RESULTS.md").write_text("\n".join(L) + "\n")

    bar = "=" * 64
    print(bar); print("metricchrono-demo-uav — reproducible change-code replay"); print(bar)
    print(f"[1] REPRODUCIBLE : {n_match}/{n_tot} analysis outputs byte-identical (SHA-256)  "
          f"{'OK' if det_ok else 'MISMATCH'}   (analysis on bundled MC output)")
    print(f"    MC-code fp   : {fp[:16]}…   shuffle-control {'OK' if shuffle_moves else 'WEAK'}")
    print(f"[2] ALIGNMENT    : all-stream mean {ov_mean[0]:.1f} -> {ov_mean[1]:.1f} ms (-{_pct(*ov_mean):.0f}%)"
          f" ; camera p50 {cam_p50[0]:.1f} -> {cam_p50[1]:.1f} ms (-{_pct(*cam_p50):.0f}%)   [committed full run]")
    print(f"[3] OVERHEAD     : ~{bw:.2f}% bandwidth")
    print(f"    figure       : out/hero_alignment.png  ·  RESULTS.md")
    print(bar)
    return 0 if det_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
