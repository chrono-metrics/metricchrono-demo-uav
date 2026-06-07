# Results

_Reproducible change-code replay on a **public** two-UAV ROS 2/MAVROS recording (CTU-MRS, arXiv:2306.17544) under **seeded** clock-drift, out-of-order delivery and dropout. Corrections were produced by the MetricChrono **enterprise** engine (not in this repo); this repo ships that output + a pure-pandas analysis._

## 1. Reproducible (live, this run — on the bundled circle subset)
- **12/12 analysis outputs byte-identical** across two runs (SHA-256). OK  _(analysis reproducibility on the bundled MC output — not a re-execution of the proprietary encoder.)_
- **MC change-code fingerprint** (SHA-256 of `mc_correction_ns_effective`): `e2f0eca69c9a84c4…`
- **Shuffle control:** perturb arrival order within 0.2s bins → output changes (Δ out-of-order 251 ms): the analysis is genuinely order-sensitive. OK

## 2. Time-alignment error to the synchronized reference (committed FULL two-flight run)
_Per-event `|corrected − synced-reference|`, pooled across streams; from `data/metrics_table.csv`. Baseline = naive impaired local clock (`t_obs`). The bundled parquet is the circle subset only._

| metric | baseline | MetricChrono | improvement |
|---|--:|--:|--:|
| all-stream mean | 30.9 ms | 23.0 ms | **−25%** |
| all-stream p99 | 75.0 ms | 63.7 ms | −15% |
| camera stream p50 | 29.7 ms | 19.0 ms | **−36%** |
| uav12 SLAM-odom p50 | 47.4 ms | 39.6 ms | −17% |

## 3. Cost
- Reconciliation **bandwidth overhead ≈ 0.47%** of payload.

## Caveats
- **In-sample** (calibration fit on the reference timeline, holdout off) and the baseline is a naive impaired clock — not a tuned aligner. Out-of-sample + header-stamp-baseline validation needs the full engine.
- The corrections come from the **enterprise** engine, not the Apache-2.0 open core (which ships the comparator/ladder primitive).

> MetricChrono is a measurement/evidence layer (deterministic time-alignment & change-coding). It is not an autonomy, targeting, navigation, or weapons system and does not certify safety or mission assurance.
