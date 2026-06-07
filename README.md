# metricchrono-demo-uav

**Reproducible MetricChrono change-code replay on a public two-UAV recording.**

MetricChrono turns the distance between successive states of a multi-sensor stream into a
**deterministic, reproducible change-code**, so you can replay *what changed* under
disruption. This repo demonstrates that on a **public** recording of **two cooperative
UAVs** (LiDAR-equipped primary + camera/VIO secondary), replayed under **seeded** synthetic
clock-drift, out-of-order delivery, and packet dropout — the conditions that break naive
timestamp alignment.

> **Dataset:** *Heterogeneous UAV dataset for relative localization and cooperative flight*,
> Pritzl, Vrba, Štěpán & Saska, CTU-MRS, **[arXiv:2306.17544](https://arxiv.org/abs/2306.17544)**
> (public, chrony-synchronized). This is **real data with reproducible synthetic disruption**
> (seed `424242`: 5–200 ms network delay, 15% out-of-order, 5 burst-loss windows, a 3 s
> partition delay).

---

![MetricChrono on a public two-UAV recording, under seeded clock-drift / out-of-order / dropout](out/hero_alignment.png)

## Quickstart

```bash
pip install -r requirements.txt
bash run_demo.sh
```

~30 s, pure `numpy/pandas/pyarrow/matplotlib`. Outputs: `RESULTS.md`, `out/hero_alignment.png`,
`out/determinism.json`.

---

## ⚠️ What this repo proves — and what it does NOT

- **Reproducibility (live):** the bundled `data/events_impaired_circle.parquet` (the circle
  flight) contains the MetricChrono engine's **output** (corrected timestamps). `run_demo.sh`
  re-runs the change-code **analysis** twice and SHA-256-hashes the outputs → **12/12
  byte-identical**, plus a **shuffle control** (perturbing arrival order changes the output, so
  it's genuinely order-sensitive), and a **fingerprint** of the shipped MC correction column.
  This proves the **analysis is bit-reproducible** on the committed codes — it is **not** a
  re-execution of the encoder.
- **Alignment headline (committed full run):** the **−25% / −36%** numbers are read from
  `data/metrics_table.csv` — the result of the **full two-flight** pipeline (circle + figure-
  eight). The bundled parquet is the **circle subset only** (internal decision signals removed),
  used for the live reproducibility proof + the figure.

| metric | baseline (naive impaired local clock `t_obs`) | MetricChrono | improvement |
|---|--:|--:|--:|
| all-stream mean | 30.9 ms | 23.0 ms | **−25 %** |
| all-stream p99 | 75.0 ms | 63.7 ms | −15 % |
| camera stream p50 | 29.7 ms | 19.0 ms | **−36 %** |
| uav12 SLAM-odom p50 | 47.4 ms | 39.6 ms | −17 % |

Reconciliation **bandwidth overhead ≈ 0.47 %**.

---

## Open core vs. enterprise — be precise

The corrections in `data/*.parquet` were produced by the MetricChrono **enterprise engine**
(stateful fleet-timing + gap/OOD/robustness/reorder), which is **proprietary and not in this
repo**. The **Apache-2.0 open core** —
**[github.com/chrono-metrics/metricchrono](https://github.com/chrono-metrics/metricchrono)** —
ships the *primitive* (the epsilon-delta-p comparator, the multiscale ladder, base metrics).
This repo ships the engine's **output on a public dataset** + a pure-pandas analysis, so the
reproducibility claim above is verifiable without the engine. It is **in-sample** (calibration
fit on the reference timeline, holdout off) against a naive baseline; out-of-sample and
header-stamp-baseline validation require the full engine.

## Scope

> MetricChrono is a **measurement/evidence layer** (deterministic time-alignment &
> change-coding). It is **not** an autonomy, targeting, navigation, or weapons system and does
> not certify safety or mission assurance.

## License

- **Code** (`*.py`, `*.sh`): **Apache-2.0** — see [`LICENSE`](LICENSE).
- **Data** (`data/`): **derived timing metadata** from the public CTU-MRS recording — no raw
  sensor payloads. We claim **no rights over the source recording**; honor CTU-MRS's terms and
  cite [arXiv:2306.17544](https://arxiv.org/abs/2306.17544). Details in [`NOTICE`](NOTICE) /
  [`DATA-LICENSE.md`](DATA-LICENSE.md).
