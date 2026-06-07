# Data terms

The files under `data/` (`events_impaired_circle.parquet`, `metrics_table.csv`,
`meeting_summary.csv`) are **derived timing metadata** computed from the public dataset
*Heterogeneous UAV dataset for relative localization and cooperative flight* (Pritzl, Vrba,
Štěpán, Saska, CTU-MRS, [arXiv:2306.17544](https://arxiv.org/abs/2306.17544)).

They contain only **(a)** per-event timing metadata (timestamps, drop flags, payload sizes) and
**(b)** the MetricChrono engine's output (corrected timestamps + alignment errors) under a seeded
synthetic impairment (seed `424242`). They do **not** contain the original dataset's raw sensor
payloads — no images, point clouds, or raw LiDAR.

**Rights.** Chrono-Metrics claims **no copyright over the underlying CTU-MRS recording**. We
release our own contribution — the derived metadata and the MetricChrono corrections — for free
reuse, and make **no representation** about rights in the underlying source data.

**Your obligations.** If you redistribute or build on `data/`, you must **honor the upstream
CTU-MRS dataset's terms** and **cite** the source:

> "MetricChrono demo data, derived from the CTU-MRS cooperative-UAV dataset
> (Pritzl et al., arXiv:2306.17544)."
