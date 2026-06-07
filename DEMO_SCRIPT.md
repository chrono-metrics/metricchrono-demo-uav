# 90-second narration (Loom / live)

**Goal:** land "bit-reproducible change-coding that quantifies real sensor-fusion disruption,"
on real two-UAV data, in 90 seconds. (Precise: corrections come from the enterprise engine; the
fast path proves *analysis* reproducibility, not encoder re-execution.)

**0:00 — Frame it (10s).** "Public recording of two cooperative drones — LiDAR primary, camera/VIO
secondary. I've replayed it under seeded clock-drift, dropped packets, and out-of-order delivery:
the stuff that wrecks sensor fusion."

**0:10 — One command (10s).** `bash run_demo.sh`. "It re-runs the change-code analysis and proves
it's bit-reproducible."

**0:25 — Reproducibility (25s).** Show `REPRODUCIBLE: 12/12 analysis outputs byte-identical (SHA-256)`
and the MC-code fingerprint. "Bit-reproducible analysis on the committed MetricChrono output; the
fingerprint is the correction column. The shuffle control proves it genuinely responds to input
order, not a constant."

**0:50 — Alignment (25s).** Open `out/hero_alignment.png`. "Red is the naive impaired-clock baseline;
blue is MetricChrono. Time-alignment error to the synced reference drops ~25% overall, ~36% on the
relayed camera stream — for ~0.5% bandwidth. (Headline from the committed full two-flight run; the
bundled data is the circle subset for the live proof.)"

**1:15 — Close (15s).** "Measurement/evidence layer — not autonomy or safety. These corrections come
from our enterprise engine; the Apache-2.0 core (github.com/chrono-metrics/metricchrono) is the
underlying primitive. Worth 20 minutes on a bag of yours?"

**Fallback:** show committed `RESULTS.md` + `out/hero_alignment.png` and read the reproducibility line
from `out/determinism.json`.
