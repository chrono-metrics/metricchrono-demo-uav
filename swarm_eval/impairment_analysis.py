"""Impairment-focused metrics and reporting utilities."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .io import read_parquet
from .progress import log_status, maybe_tqdm


ARRIVAL_FALLBACK_COLUMNS = ("t_arrival_ns", "t_record_ns")
BASELINE_COLUMNS = (
    "t_est_baseline_ns",
    "baseline_correction_ns",
    "baseline_time_ns",
    "t_baseline_ns",
)
MC_COLUMNS = (
    "t_est_mc_ns",
    "mc_correction_ns_effective",
    "mc_correction_ns",
    "mc_time_ns",
    "t_mc_ns",
)
BYTES_COLUMNS = ("bytes", "payload_size_bytes", "payload_bytes", "size_bytes")


@dataclass(slots=True)
class StandardizedEvents:
    frame: pd.DataFrame
    baseline_mode: str
    arrival_source: str
    obs_source: str | None
    baseline_source: str
    mc_source: str
    bytes_source: str | None


@dataclass(slots=True)
class AlignmentPairConfig:
    name: str
    source_id: str
    topic_a: str
    topic_b: str
    window_ns: int


@dataclass(slots=True)
class AnalysisConfig:
    alignment_window_ns: int = int(0.5e9)
    reconverge_threshold_ms: float = 50.0
    reconverge_window_s: float = 5.0
    reconverge_stability_s: float = 10.0
    reconverge_windows_s: Tuple[Tuple[float, float], ...] | None = None
    shuffle_bin_ns: int = int(0.2e9)
    alignment_tolerance_ns: int = int(5e6)
    show_progress: bool = False


@dataclass(slots=True)
class AnalysisOutputs:
    topic_inventory: pd.DataFrame
    out_of_order: pd.DataFrame
    out_of_order_worst: pd.DataFrame
    gaps_transport: pd.DataFrame
    gaps_transport_worst: pd.DataFrame
    gaps_sampling: pd.DataFrame
    gaps_sampling_worst: pd.DataFrame
    drift_header: pd.DataFrame
    drift_obs: pd.DataFrame
    alignment_pairs: pd.DataFrame
    alignment_worst: pd.DataFrame
    reconverge: pd.DataFrame
    rolling_reconverge: pd.DataFrame
    alignment_matches: Dict[str, pd.DataFrame]


def _pick_column(df: pd.DataFrame, candidates: Sequence[str]) -> str | None:
    for name in candidates:
        if name in df.columns:
            return name
    return None


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _percentiles(values: np.ndarray, percentiles: Sequence[float]) -> List[float]:
    if values.size == 0:
        return [float("nan") for _ in percentiles]
    return [float(val) for val in np.percentile(values, percentiles)]


def _progress_groups(groups: Iterable[Tuple[Tuple[object, ...], pd.DataFrame]], *, enabled: bool, desc: str) -> Iterable:
    total = None
    if hasattr(groups, "ngroups"):
        total = int(getattr(groups, "ngroups"))
    return maybe_tqdm(groups, enabled=enabled, desc=desc, total=total)


def standardize_events(df: pd.DataFrame, *, obs_time_col: str | None = None) -> StandardizedEvents:
    if "topic" not in df:
        raise KeyError("Missing required column: topic")

    if "drop" in df.columns:
        df = df[~df["drop"].fillna(False)].copy()

    source_col = _pick_column(df, ("source_id", "agent_id", "uav", "bag"))
    if source_col is None:
        source_series = pd.Series(["unknown"] * len(df), index=df.index)
    elif source_col in {"uav", "bag"} and "topic" in df.columns:
        from .stream_key import stream_key_parts

        parts = [
            stream_key_parts(uav, bag, topic)
            for uav, bag, topic in zip(df.get("uav"), df.get("bag"), df["topic"])
        ]
        uav_ids, bag_ids, _ = zip(*parts) if parts else ([], [], [])
        source_series = pd.Series([f"{uav}::{bag}" for uav, bag in zip(uav_ids, bag_ids)], index=df.index)
    else:
        source_series = df[source_col].astype(str)

    arrival_col = _pick_column(df, ARRIVAL_FALLBACK_COLUMNS)
    if arrival_col is None:
        raise KeyError("Missing required arrival column t_arrival_ns or t_record_ns")

    header_col = "t_header_ns" if "t_header_ns" in df.columns else None
    obs_candidates = [col for col in [obs_time_col, "t_obs_ns"] if col]
    obs_col = _pick_column(df, obs_candidates)

    baseline_col = _pick_column(df, BASELINE_COLUMNS)
    mc_col = _pick_column(df, MC_COLUMNS)
    bytes_col = _pick_column(df, BYTES_COLUMNS)

    if mc_col is None:
        raise KeyError("Missing required MC estimate column (t_est_mc_ns)")

    frame = pd.DataFrame(
        {
            "topic": df["topic"].astype(str),
            "source_id": source_series,
            "t_arrival_ns": _to_numeric(df[arrival_col]),
            "t_header_ns": _to_numeric(df[header_col]) if header_col else np.nan,
            "t_obs_ns": _to_numeric(df[obs_col]) if obs_col else np.nan,
            "t_est_baseline_ns": _to_numeric(df[baseline_col]) if baseline_col else np.nan,
            "t_est_mc_ns": _to_numeric(df[mc_col]),
            "bytes": _to_numeric(df[bytes_col]) if bytes_col else np.nan,
        }
    )

    if frame["t_arrival_ns"].isna().all():
        raise ValueError("t_arrival_ns is present but contains no valid values")

    baseline_mode = "standard"
    baseline_source = baseline_col or "fallback"
    if baseline_col is None:
        if obs_col is not None:
            frame["t_est_baseline_ns"] = frame["t_obs_ns"]
            baseline_source = obs_col
        else:
            frame["t_est_baseline_ns"] = frame["t_arrival_ns"]
            baseline_source = arrival_col
        baseline_mode = "degraded"

    return StandardizedEvents(
        frame=frame,
        baseline_mode=baseline_mode,
        arrival_source=arrival_col,
        obs_source=obs_col,
        baseline_source=baseline_source,
        mc_source=mc_col,
        bytes_source=bytes_col,
    )


def compute_topic_inventory(frame: pd.DataFrame, *, show_progress: bool = False) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    groups = frame.groupby(["source_id", "topic"], dropna=False)
    for (source_id, topic), group in _progress_groups(groups, enabled=show_progress, desc="topic inventory"):
        arrivals = group["t_arrival_ns"].to_numpy(dtype=np.float64)
        arrivals = arrivals[np.isfinite(arrivals)]
        if arrivals.size == 0:
            continue
        ordered = np.sort(arrivals)
        dts = np.diff(ordered)
        duration_ns = float(ordered[-1] - ordered[0])
        mean_hz = float((ordered.size - 1) / (duration_ns / 1e9)) if duration_ns > 0 else float("nan")
        dt_stats = _percentiles(dts, [50, 95, 99])
        dt_max = float(dts.max()) if dts.size else float("nan")

        ordered_group = group.sort_values("t_arrival_ns")
        bytes_all = ordered_group["bytes"].to_numpy(dtype=np.float64)
        bytes_mask = np.isfinite(bytes_all)
        bytes_vals = bytes_all[bytes_mask]
        bytes_mean = float(np.mean(bytes_vals)) if bytes_vals.size else float("nan")
        bytes_p95 = float(np.percentile(bytes_vals, 95)) if bytes_vals.size else float("nan")

        bitrate_mean = float("nan")
        bitrate_p95 = float("nan")
        if dts.size:
            dt_seconds = dts / 1e9
            rate_bytes = bytes_all[1:]
            valid = np.isfinite(rate_bytes) & (dt_seconds > 0)
            if valid.any():
                bitrate_vals = rate_bytes[valid] / dt_seconds[valid]
                bitrate_mean = float(np.mean(bitrate_vals)) if bitrate_vals.size else float("nan")
                bitrate_p95 = float(np.percentile(bitrate_vals, 95)) if bitrate_vals.size else float("nan")

        rows.append(
            {
                "source_id": source_id,
                "topic": topic,
                "count": int(ordered.size),
                "duration_ns": duration_ns,
                "mean_hz": mean_hz,
                "dt_arrival_p50_ns": dt_stats[0],
                "dt_arrival_p95_ns": dt_stats[1],
                "dt_arrival_p99_ns": dt_stats[2],
                "dt_arrival_max_ns": dt_max,
                "bytes_mean": bytes_mean,
                "bytes_p95": bytes_p95,
                "bitrate_mean": bitrate_mean,
                "bitrate_p95": bitrate_p95,
            }
        )
    return pd.DataFrame(rows)


def compute_out_of_order(frame: pd.DataFrame, *, show_progress: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rows: List[Dict[str, object]] = []
    worst_rows: List[Dict[str, object]] = []

    groups = frame.groupby(["source_id", "topic"], dropna=False)
    for (source_id, topic), group in _progress_groups(groups, enabled=show_progress, desc="out-of-order"):
        stamp_col = "t_header_ns" if group["t_header_ns"].notna().any() else "t_obs_ns"
        stamps = group[stamp_col].to_numpy(dtype=np.float64)
        arrivals = group["t_arrival_ns"].to_numpy(dtype=np.float64)
        mask = np.isfinite(stamps) & np.isfinite(arrivals)
        stamps = stamps[mask]
        arrivals = arrivals[mask]
        if stamps.size == 0:
            continue
        order = np.argsort(arrivals)
        stamps = stamps[order]
        arrivals = arrivals[order]

        lateness_vals: List[float] = []
        neg_dstamp = 0
        comparisons = max(len(stamps) - 1, 0)
        max_stamp = stamps[0]
        prev_stamp = stamps[0]
        for stamp, arrival in zip(stamps, arrivals):
            lateness = max(0.0, max_stamp - stamp)
            if lateness > 0:
                lateness_vals.append(lateness)
                worst_rows.append(
                    {
                        "source_id": source_id,
                        "topic": topic,
                        "t_arrival_ns": float(arrival),
                        "stamp_ns": float(stamp),
                        "max_stamp_so_far_ns": float(max_stamp),
                        "lateness_ns": float(lateness),
                    }
                )
            max_stamp = max(max_stamp, stamp)
            if stamp - prev_stamp < 0:
                neg_dstamp += 1
            prev_stamp = stamp

        lateness_arr = np.array(lateness_vals, dtype=np.float64)
        lateness_rate = float(len(lateness_vals) / comparisons) if comparisons > 0 else float("nan")
        lateness_stats = _percentiles(lateness_arr, [50, 95, 99])
        lateness_max = float(lateness_arr.max()) if lateness_arr.size else 0.0
        neg_rate = float(neg_dstamp / comparisons) if comparisons > 0 else float("nan")

        rows.append(
            {
                "source_id": source_id,
                "topic": topic,
                "stamp_col": stamp_col,
                "event_count": int(len(stamps)),
                "lateness_rate": lateness_rate,
                "lateness_p50_ns": lateness_stats[0],
                "lateness_p95_ns": lateness_stats[1],
                "lateness_p99_ns": lateness_stats[2],
                "lateness_max_ns": lateness_max,
                "neg_dstamp_rate": neg_rate,
            }
        )

    worst_df = pd.DataFrame(worst_rows)
    if not worst_df.empty:
        worst_df.sort_values("lateness_ns", ascending=False, inplace=True)
        worst_df = worst_df.head(20)
    return pd.DataFrame(rows), worst_df


def _compute_nominal_dt(dts: np.ndarray) -> float:
    if dts.size == 0:
        return float("nan")
    p5, p95 = np.percentile(dts, [5, 95])
    trimmed = dts[(dts >= p5) & (dts <= p95)]
    if trimmed.size == 0:
        trimmed = dts
    return float(np.median(trimmed))


def compute_gaps(
    frame: pd.DataFrame,
    *,
    stamp_col: str,
    show_progress: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    summary_rows: List[Dict[str, object]] = []
    worst_rows: List[Dict[str, object]] = []

    groups = frame.groupby(["source_id", "topic"], dropna=False)
    for (source_id, topic), group in _progress_groups(groups, enabled=show_progress, desc=f"gaps:{stamp_col}"):
        stamps = group[stamp_col].to_numpy(dtype=np.float64)
        stamps = stamps[np.isfinite(stamps)]
        if stamps.size < 2:
            summary_rows.append(
                {
                    "source_id": source_id,
                    "topic": topic,
                    "stamp_col": stamp_col,
                    "event_count": int(stamps.size),
                    "nominal_dt_ns": float("nan"),
                    "gap_count": 0,
                    "gap_p50_ns": 0.0,
                    "gap_p95_ns": 0.0,
                    "gap_p99_ns": 0.0,
                    "gap_max_ns": 0.0,
                }
            )
            continue

        ordered = np.sort(stamps)
        dts = np.diff(ordered)
        nominal = _compute_nominal_dt(dts)
        threshold = max(3 * nominal, nominal + 200_000_000.0)
        gap_mask = dts > threshold
        gap_dts = dts[gap_mask]

        summary_rows.append(
            {
                "source_id": source_id,
                "topic": topic,
                "stamp_col": stamp_col,
                "event_count": int(stamps.size),
                "nominal_dt_ns": float(nominal),
                "gap_count": int(gap_dts.size),
                "gap_p50_ns": float(np.percentile(gap_dts, 50)) if gap_dts.size else 0.0,
                "gap_p95_ns": float(np.percentile(gap_dts, 95)) if gap_dts.size else 0.0,
                "gap_p99_ns": float(np.percentile(gap_dts, 99)) if gap_dts.size else 0.0,
                "gap_max_ns": float(gap_dts.max()) if gap_dts.size else 0.0,
            }
        )

        if gap_dts.size:
            indices = np.where(gap_mask)[0]
            for idx, gap_dt in zip(indices, gap_dts):
                worst_rows.append(
                    {
                        "source_id": source_id,
                        "topic": topic,
                        "stamp_col": stamp_col,
                        "gap_start_ns": float(ordered[idx]),
                        "gap_end_ns": float(ordered[idx + 1]),
                        "gap_dt_ns": float(gap_dt),
                        "nominal_dt_ns": float(nominal),
                    }
                )

    worst_df = pd.DataFrame(worst_rows)
    if not worst_df.empty:
        worst_df.sort_values("gap_dt_ns", ascending=False, inplace=True)
        worst_df = worst_df.head(20)
    return pd.DataFrame(summary_rows), worst_df


def _ols_fit(x: np.ndarray, y: np.ndarray) -> Tuple[float, float]:
    x_mean = float(x.mean())
    y_mean = float(y.mean())
    x_centered = x - x_mean
    y_centered = y - y_mean
    denom = float(np.dot(x_centered, x_centered))
    slope = float(np.dot(x_centered, y_centered) / denom) if denom > 0 else 1.0
    intercept = y_mean - slope * x_mean
    return slope, intercept


def _drift_rows(frame: pd.DataFrame, *, stamp_col: str, show_progress: bool = False) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    groups = frame.groupby(["source_id", "topic"], dropna=False)
    for (source_id, topic), group in _progress_groups(groups, enabled=show_progress, desc=f"drift:{stamp_col}"):
        stamps = group[stamp_col].to_numpy(dtype=np.float64)
        arrivals = group["t_arrival_ns"].to_numpy(dtype=np.float64)
        mask = np.isfinite(stamps) & np.isfinite(arrivals)
        stamps = stamps[mask]
        arrivals = arrivals[mask]
        if stamps.size < 2:
            rows.append(
                {
                    "source_id": source_id,
                    "topic": topic,
                    "stamp_col": stamp_col,
                    "sample_count": int(stamps.size),
                    "drift_ppm": float("nan"),
                    "offset_start_ns": float("nan"),
                    "offset_end_ns": float("nan"),
                    "offset_range_ns": float("nan"),
                    "residual_p50_ns": float("nan"),
                    "residual_p95_ns": float("nan"),
                    "residual_p99_ns": float("nan"),
                    "residual_max_ns": float("nan"),
                }
            )
            continue

        order = np.argsort(stamps)
        stamps = stamps[order]
        arrivals = arrivals[order]

        slope, intercept = _ols_fit(stamps, arrivals)
        fitted = intercept + slope * stamps
        residual = arrivals - fitted
        abs_residual = np.abs(residual)
        residual_stats = _percentiles(abs_residual, [50, 95, 99])

        offsets = arrivals - stamps
        offset_start = float(offsets[0])
        offset_end = float(offsets[-1])
        offset_range = float(offset_end - offset_start)

        rows.append(
            {
                "source_id": source_id,
                "topic": topic,
                "stamp_col": stamp_col,
                "sample_count": int(stamps.size),
                "drift_ppm": float((slope - 1.0) * 1e6),
                "offset_start_ns": offset_start,
                "offset_end_ns": offset_end,
                "offset_range_ns": offset_range,
                "residual_p50_ns": residual_stats[0],
                "residual_p95_ns": residual_stats[1],
                "residual_p99_ns": residual_stats[2],
                "residual_max_ns": float(abs_residual.max()),
            }
        )
    return pd.DataFrame(rows)


def compute_drift(frame: pd.DataFrame, *, show_progress: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame]:
    header_df = _drift_rows(
        frame[frame["t_header_ns"].notna()],
        stamp_col="t_header_ns",
        show_progress=show_progress,
    )
    obs_df = pd.DataFrame()
    if frame["t_obs_ns"].notna().any():
        obs_df = _drift_rows(
            frame[frame["t_obs_ns"].notna()],
            stamp_col="t_obs_ns",
            show_progress=show_progress,
        )
        jump_threshold = 2 * 3600 * 1e9
        if not obs_df.empty:
            offset_range = obs_df["offset_range_ns"].abs()
            offset_abs = obs_df[["offset_start_ns", "offset_end_ns"]].abs().max(axis=1)
            obs_df["non_physical"] = (offset_range > jump_threshold) | (offset_abs > jump_threshold)
    return header_df, obs_df


def select_alignment_pairs(frame: pd.DataFrame, *, window_ns: int) -> List[AlignmentPairConfig]:
    configs: List[AlignmentPairConfig] = []
    for source_id, group in frame.groupby("source_id", dropna=False):
        topics = group["topic"].dropna().astype(str)
        if topics.empty:
            continue
        counts = topics.value_counts()
        if len(counts) < 2:
            continue
        topic_a = str(counts.index[0])
        topic_b = str(counts.index[1])
        name = f"{source_id}:{topic_a}_vs_{topic_b}"
        configs.append(
            AlignmentPairConfig(
                name=name,
                source_id=str(source_id),
                topic_a=topic_a,
                topic_b=topic_b,
                window_ns=window_ns,
            )
        )
    return configs


def _match_nearest(times_a: np.ndarray, times_b: np.ndarray, window_ns: int) -> List[Tuple[int, int]]:
    matches: List[Tuple[int, int]] = []
    if times_a.size == 0 or times_b.size == 0:
        return matches
    for idx_a, time_a in enumerate(times_a):
        pos = int(np.searchsorted(times_b, time_a))
        candidates = []
        if pos < len(times_b):
            candidates.append(pos)
        if pos > 0:
            candidates.append(pos - 1)
        best_idx = None
        best_dt = None
        for idx_b in candidates:
            dt = abs(times_b[idx_b] - time_a)
            if dt <= window_ns and (best_dt is None or dt < best_dt):
                best_dt = dt
                best_idx = idx_b
        if best_idx is not None:
            matches.append((idx_a, best_idx))
    return matches


def _pair_indices(
    frame: pd.DataFrame, cfg: AlignmentPairConfig
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    group = frame[frame["source_id"] == cfg.source_id]
    df_a = group[group["topic"] == cfg.topic_a].sort_values("t_arrival_ns").reset_index(drop=True)
    df_b = group[group["topic"] == cfg.topic_b].sort_values("t_arrival_ns").reset_index(drop=True)
    if df_a.empty or df_b.empty:
        return pd.DataFrame(), df_a, df_b

    arr_a = df_a["t_arrival_ns"].to_numpy(dtype=np.float64)
    arr_b = df_b["t_arrival_ns"].to_numpy(dtype=np.float64)
    matches = _match_nearest(arr_a, arr_b, cfg.window_ns)
    if not matches:
        return pd.DataFrame(), df_a, df_b

    stream_a = f"{cfg.source_id}:{cfg.topic_a}"
    stream_b = f"{cfg.source_id}:{cfg.topic_b}"
    rows: List[Dict[str, object]] = []
    for pair_idx, (idx_a, idx_b) in enumerate(matches):
        rows.append(
            {
                "pair": cfg.name,
                "pair_id": f"{cfg.name}:{pair_idx}",
                "source_id": cfg.source_id,
                "topic_a": cfg.topic_a,
                "topic_b": cfg.topic_b,
                "stream_a": stream_a,
                "stream_b": stream_b,
                "i_a": int(idx_a),
                "i_b": int(idx_b),
                "t_arrival_a_ns": float(arr_a[idx_a]),
                "t_arrival_b_ns": float(arr_b[idx_b]),
            }
        )
    return pd.DataFrame(rows), df_a, df_b


def compute_alignment_pairs(
    frame: pd.DataFrame,
    pair_configs: Sequence[AlignmentPairConfig],
    *,
    show_progress: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, pd.DataFrame]]:
    metrics_rows: List[Dict[str, object]] = []
    worst_rows: List[Dict[str, object]] = []
    match_tables: Dict[str, pd.DataFrame] = {}

    iterator = maybe_tqdm(
        pair_configs,
        enabled=show_progress,
        desc="alignment pairs",
        total=len(pair_configs),
    )
    for cfg in iterator:
        pairs_df, df_a, df_b = _pair_indices(frame, cfg)
        if pairs_df.empty:
            continue
        if pairs_df["pair_id"].duplicated().any():
            raise ValueError(f"Duplicate pair_id detected for pair {cfg.name}")
        if pairs_df[["i_a", "i_b"]].isna().any().any():
            raise ValueError(f"Missing pair indices for pair {cfg.name}")
        idx_a = pairs_df["i_a"].to_numpy(dtype=np.int64)
        idx_b = pairs_df["i_b"].to_numpy(dtype=np.int64)
        baseline_a = df_a["t_est_baseline_ns"].to_numpy(dtype=np.float64)
        baseline_b = df_b["t_est_baseline_ns"].to_numpy(dtype=np.float64)
        mc_a = df_a["t_est_mc_ns"].to_numpy(dtype=np.float64)
        mc_b = df_b["t_est_mc_ns"].to_numpy(dtype=np.float64)

        t_est_baseline_a = baseline_a[idx_a]
        t_est_baseline_b = baseline_b[idx_b]
        t_est_mc_a = mc_a[idx_a]
        t_est_mc_b = mc_b[idx_b]

        pairs_df = pairs_df.copy()
        pairs_df["t_est_baseline_a_ns"] = t_est_baseline_a
        pairs_df["t_est_baseline_b_ns"] = t_est_baseline_b
        pairs_df["t_est_mc_a_ns"] = t_est_mc_a
        pairs_df["t_est_mc_b_ns"] = t_est_mc_b
        match_tables[cfg.name] = pairs_df

        err_baseline_full = np.abs(t_est_baseline_a - t_est_baseline_b)
        err_mc_full = np.abs(t_est_mc_a - t_est_mc_b)
        valid_mask = (
            np.isfinite(t_est_baseline_a)
            & np.isfinite(t_est_baseline_b)
            & np.isfinite(t_est_mc_a)
            & np.isfinite(t_est_mc_b)
        )

        err_baseline = err_baseline_full[valid_mask]
        err_mc = err_mc_full[valid_mask]
        base_stats = _percentiles(err_baseline, [50, 95, 99])
        mc_stats = _percentiles(err_mc, [50, 95, 99])

        denom = len(frame[(frame["source_id"] == cfg.source_id) & (frame["topic"] == cfg.topic_a)])
        match_count = int(valid_mask.sum())
        match_rate = float(match_count / denom) if denom else float("nan")
        metrics_rows.append(
            {
                "pair": cfg.name,
                "source_id": cfg.source_id,
                "topic_a": cfg.topic_a,
                "topic_b": cfg.topic_b,
                "match_count": match_count,
                "match_count_total": int(len(pairs_df)),
                "match_rate": match_rate,
                "err_baseline_p50_ns": base_stats[0],
                "err_baseline_p95_ns": base_stats[1],
                "err_baseline_p99_ns": base_stats[2],
                "err_baseline_max_ns": float(err_baseline.max()) if err_baseline.size else float("nan"),
                "err_mc_p50_ns": mc_stats[0],
                "err_mc_p95_ns": mc_stats[1],
                "err_mc_p99_ns": mc_stats[2],
                "err_mc_max_ns": float(err_mc.max()) if err_mc.size else float("nan"),
                "tail_delta_p95_ns": mc_stats[1] - base_stats[1],
                "tail_delta_p99_ns": mc_stats[2] - base_stats[2],
            }
        )
        pairs_df = pairs_df.copy()
        pairs_df["err_baseline_ns"] = err_baseline_full
        pairs_df["err_mc_ns"] = err_mc_full
        if valid_mask.any():
            worst_rows.extend(
                pairs_df.loc[valid_mask]
                .assign(worst_err_ns=np.maximum(err_baseline_full, err_mc_full)[valid_mask])
                .sort_values("worst_err_ns", ascending=False)
                .head(20)
                .drop(columns=["worst_err_ns"])
                .to_dict("records")
            )

    worst_df = pd.DataFrame(worst_rows)
    if not worst_df.empty:
        err_stack = np.vstack([worst_df["err_baseline_ns"], worst_df["err_mc_ns"]])
        finite_mask = np.isfinite(err_stack).any(axis=0)
        if finite_mask.any():
            worst_df = worst_df.loc[finite_mask].copy()
            worst_df["worst_err_ns"] = np.nanmax(err_stack[:, finite_mask], axis=0)
            worst_df.sort_values("worst_err_ns", ascending=False, inplace=True)
            worst_df = worst_df.head(20).drop(columns=["worst_err_ns"])
        else:
            worst_df = pd.DataFrame()
    return pd.DataFrame(metrics_rows), worst_df, match_tables


def _rolling_percentile(times: np.ndarray, values: np.ndarray, *, window_ns: int, percentile: float) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    start_idx = 0
    for idx, t_now in enumerate(times):
        while times[start_idx] < t_now - window_ns:
            start_idx += 1
        window_vals = values[start_idx : idx + 1]
        if window_vals.size:
            p_val = float(np.percentile(window_vals, percentile))
        else:
            p_val = float("nan")
        rows.append({"t_arrival_ns": float(t_now), "value": p_val})
    return pd.DataFrame(rows)


def _select_reconverge_topic(out_of_order_df: pd.DataFrame) -> str | None:
    if out_of_order_df.empty:
        return None
    sorted_df = out_of_order_df.copy()
    sorted_df["lateness_p99_ns"] = sorted_df["lateness_p99_ns"].fillna(-1.0)
    sorted_df = sorted_df.sort_values("lateness_p99_ns", ascending=False)
    return str(sorted_df.iloc[0]["topic"])


def _default_reconverge_windows(frame: pd.DataFrame) -> Tuple[Tuple[float, float], ...]:
    arrivals = frame["t_arrival_ns"].to_numpy(dtype=np.float64)
    arrivals = arrivals[np.isfinite(arrivals)]
    if arrivals.size < 2:
        return ((0.0, 0.0),)
    duration_s = float((arrivals.max() - arrivals.min()) / 1e9)
    start_s = duration_s * 0.4
    end_s = duration_s * 0.5
    if end_s <= start_s:
        end_s = start_s + max(5.0, duration_s * 0.1)
    return ((start_s, end_s),)


def compute_reconverge(
    frame: pd.DataFrame,
    *,
    pair_config: AlignmentPairConfig,
    target_topic: str,
    windows_s: Sequence[Tuple[float, float]],
    threshold_ms: float,
    window_s: float,
    stability_s: float,
    show_progress: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rows: List[Dict[str, object]] = []
    rolling_rows: List[Dict[str, object]] = []
    start_ns = float(frame["t_arrival_ns"].min())
    iterator = maybe_tqdm(
        list(windows_s),
        enabled=show_progress,
        desc="reconverge windows",
        total=len(windows_s),
    )
    for start_s, end_s in iterator:
        window_start_ns = start_ns + start_s * 1e9
        window_end_ns = start_ns + end_s * 1e9
        mask_drop = (frame["topic"] == target_topic) & (
            (frame["t_arrival_ns"] >= window_start_ns) & (frame["t_arrival_ns"] < window_end_ns)
        )
        filtered = frame.loc[~mask_drop].copy()
        pairs_df, df_a, df_b = _pair_indices(filtered, pair_config)
        if pairs_df.empty:
            continue
        idx_a = pairs_df["i_a"].to_numpy(dtype=np.int64)
        idx_b = pairs_df["i_b"].to_numpy(dtype=np.int64)
        baseline_a = df_a["t_est_baseline_ns"].to_numpy(dtype=np.float64)
        baseline_b = df_b["t_est_baseline_ns"].to_numpy(dtype=np.float64)
        mc_a = df_a["t_est_mc_ns"].to_numpy(dtype=np.float64)
        mc_b = df_b["t_est_mc_ns"].to_numpy(dtype=np.float64)

        matches_df = pairs_df.copy()
        matches_df["t_est_baseline_a_ns"] = baseline_a[idx_a]
        matches_df["t_est_baseline_b_ns"] = baseline_b[idx_b]
        matches_df["t_est_mc_a_ns"] = mc_a[idx_a]
        matches_df["t_est_mc_b_ns"] = mc_b[idx_b]
        if matches_df.empty:
            continue

        arrivals = matches_df["t_arrival_a_ns"].to_numpy(dtype=np.float64)
        err_baseline = np.abs(
            matches_df["t_est_baseline_a_ns"].to_numpy(dtype=np.float64)
            - matches_df["t_est_baseline_b_ns"].to_numpy(dtype=np.float64)
        ) / 1e6
        err_mc = np.abs(
            matches_df["t_est_mc_a_ns"].to_numpy(dtype=np.float64)
            - matches_df["t_est_mc_b_ns"].to_numpy(dtype=np.float64)
        ) / 1e6

        mask = np.isfinite(arrivals) & np.isfinite(err_baseline) & np.isfinite(err_mc)
        arrivals = arrivals[mask]
        err_baseline = err_baseline[mask]
        err_mc = err_mc[mask]
        if arrivals.size == 0:
            continue

        order = np.argsort(arrivals)
        arrivals = arrivals[order]
        err_baseline = err_baseline[order]
        err_mc = err_mc[order]

        for label, errors in [("baseline", err_baseline), ("mc", err_mc)]:
            rolling = _rolling_percentile(
                arrivals,
                np.abs(errors),
                window_ns=int(window_s * 1e9),
                percentile=95,
            )
            rolling["time_since_window_end_s"] = (rolling["t_arrival_ns"] - window_end_ns) / 1e9
            rolling["method"] = label
            rolling["window_start_s"] = start_s
            rolling["window_end_s"] = end_s
            rolling_rows.extend(rolling.to_dict("records"))

            recon_start = None
            recon_time_s = float("nan")
            stability_ns = stability_s * 1e9
            for t_arr, value in zip(rolling["t_arrival_ns"], rolling["value"]):
                if t_arr < window_end_ns:
                    continue
                if value <= threshold_ms:
                    recon_start = t_arr if recon_start is None else recon_start
                    if t_arr - recon_start >= stability_ns:
                        recon_time_s = float((t_arr - window_end_ns) / 1e9)
                        break
                else:
                    recon_start = None

            rows.append(
                {
                    "method": label,
                    "pair": pair_config.name,
                    "target_topic": target_topic,
                    "window_start_s": float(start_s),
                    "window_end_s": float(end_s),
                    "reconverge_time_s": recon_time_s,
                }
            )

    return pd.DataFrame(rows), pd.DataFrame(rolling_rows)


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def _hash_csvs(out_dir: Path) -> Dict[str, str]:
    return {path.name: _hash_file(path) for path in sorted(out_dir.glob("*.csv"))}


def _shuffle_arrival(frame: pd.DataFrame, *, bin_ns: int, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    shuffled = frame.copy()
    arrivals = shuffled["t_arrival_ns"].to_numpy(dtype=np.float64)
    min_arrival = np.nanmin(arrivals)
    bin_index = np.floor((arrivals - min_arrival) / bin_ns)
    shuffled_vals = arrivals.copy()
    for bin_id in np.unique(bin_index):
        mask = bin_index == bin_id
        indices = np.where(mask)[0]
        if indices.size > 1:
            rng.shuffle(indices)
            shuffled_vals[mask] = arrivals[indices]
    shuffled["t_arrival_ns"] = shuffled_vals
    return shuffled


def compute_metrics(
    frame: pd.DataFrame,
    *,
    config: AnalysisConfig,
    pair_configs: Sequence[AlignmentPairConfig] | None = None,
    target_topic: str | None = None,
) -> AnalysisOutputs:
    if config.show_progress:
        log_status("Impairment analysis: topic inventory")
    topic_inventory = compute_topic_inventory(frame, show_progress=config.show_progress)
    if config.show_progress:
        log_status("Impairment analysis: out-of-order")
    out_of_order, out_of_order_worst = compute_out_of_order(frame, show_progress=config.show_progress)

    if config.show_progress:
        log_status("Impairment analysis: transport gaps")
    gaps_transport, gaps_transport_worst = compute_gaps(
        frame,
        stamp_col="t_arrival_ns",
        show_progress=config.show_progress,
    )
    gaps_sampling = pd.DataFrame()
    gaps_sampling_worst = pd.DataFrame()
    if frame["t_header_ns"].notna().any():
        if config.show_progress:
            log_status("Impairment analysis: sampling gaps")
        gaps_sampling, gaps_sampling_worst = compute_gaps(
            frame,
            stamp_col="t_header_ns",
            show_progress=config.show_progress,
        )

    if config.show_progress:
        log_status("Impairment analysis: drift")
    drift_header, drift_obs = compute_drift(frame, show_progress=config.show_progress)

    pair_configs = list(pair_configs) if pair_configs else select_alignment_pairs(
        frame, window_ns=config.alignment_window_ns
    )
    if config.show_progress:
        log_status("Impairment analysis: alignment pairs")
    alignment_pairs, alignment_worst, matches = compute_alignment_pairs(
        frame,
        pair_configs,
        show_progress=config.show_progress,
    )

    target_topic = target_topic or _select_reconverge_topic(out_of_order)
    windows_s = config.reconverge_windows_s or _default_reconverge_windows(frame)
    reconverge = pd.DataFrame()
    rolling = pd.DataFrame()
    if pair_configs and target_topic:
        if config.show_progress:
            log_status("Impairment analysis: reconverge")
        reconverge, rolling = compute_reconverge(
            frame,
            pair_config=pair_configs[0],
            target_topic=target_topic,
            windows_s=windows_s,
            threshold_ms=config.reconverge_threshold_ms,
            window_s=config.reconverge_window_s,
            stability_s=config.reconverge_stability_s,
            show_progress=config.show_progress,
        )

    return AnalysisOutputs(
        topic_inventory=topic_inventory,
        out_of_order=out_of_order,
        out_of_order_worst=out_of_order_worst,
        gaps_transport=gaps_transport,
        gaps_transport_worst=gaps_transport_worst,
        gaps_sampling=gaps_sampling,
        gaps_sampling_worst=gaps_sampling_worst,
        drift_header=drift_header,
        drift_obs=drift_obs,
        alignment_pairs=alignment_pairs,
        alignment_worst=alignment_worst,
        reconverge=reconverge,
        rolling_reconverge=rolling,
        alignment_matches=matches,
    )


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _slugify(text: str) -> str:
    return text.replace("/", "_").replace(":", "_")


def _plot_alignment_tails(matches: Dict[str, pd.DataFrame], out_dir: Path) -> List[str]:
    figure_dir = out_dir / "figures"
    _ensure_dir(figure_dir)
    outputs: List[str] = []
    for name, df in matches.items():
        if df.empty:
            continue
        err_baseline = np.abs(df["t_est_baseline_a_ns"] - df["t_est_baseline_b_ns"]) / 1e6
        err_mc = np.abs(df["t_est_mc_a_ns"] - df["t_est_mc_b_ns"]) / 1e6
        err_baseline = err_baseline[np.isfinite(err_baseline)]
        err_mc = err_mc[np.isfinite(err_mc)]
        if err_baseline.size == 0 or err_mc.size == 0:
            continue

        fig, ax = plt.subplots(figsize=(6, 4))
        for label, series in [("Baseline", err_baseline), ("MC", err_mc)]:
            sorted_vals = np.sort(series)
            cdf = np.linspace(0, 1, len(sorted_vals))
            ax.plot(sorted_vals, cdf, label=label)
        ax.set_xlabel("Absolute alignment error (ms)")
        ax.set_ylabel("CDF")
        ax.set_title(name)
        ax.legend()
        cdf_path = figure_dir / f"alignment_cdf_{_slugify(name)}.png"
        fig.savefig(cdf_path, dpi=200, bbox_inches="tight")
        plt.close(fig)
        outputs.append(str(cdf_path))

        fig, ax = plt.subplots(figsize=(6, 4))
        for label, series in [("Baseline", err_baseline), ("MC", err_mc)]:
            sorted_vals = np.sort(series)
            ccdf = 1 - np.linspace(0, 1, len(sorted_vals))
            ax.plot(sorted_vals, ccdf, label=label)
        ax.set_xlabel("Absolute alignment error (ms)")
        ax.set_ylabel("CCDF")
        ax.set_title(name)
        ax.legend()
        ccdf_path = figure_dir / f"alignment_ccdf_{_slugify(name)}.png"
        fig.savefig(ccdf_path, dpi=200, bbox_inches="tight")
        plt.close(fig)
        outputs.append(str(ccdf_path))
    return outputs


def _plot_reconverge(rolling: pd.DataFrame, out_dir: Path) -> Optional[str]:
    if rolling.empty:
        return None
    fig, ax = plt.subplots(figsize=(7, 4))
    for label, group in rolling.groupby("method"):
        ax.plot(group["time_since_window_end_s"], group["value"], label=label)
    ax.set_xlabel("Time since window end (s)")
    ax.set_ylabel("Rolling p95 error (ms)")
    ax.set_title("Reconverge rolling p95")
    ax.legend()
    path = out_dir / "reconverge_rolling_p95.png"
    fig.savefig(path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    return str(path)


def write_outputs(out_dir: Path, outputs: AnalysisOutputs) -> None:
    _ensure_dir(out_dir)
    outputs.topic_inventory.to_csv(out_dir / "topic_inventory.csv", index=False)
    outputs.out_of_order.to_csv(out_dir / "out_of_order.csv", index=False)
    outputs.out_of_order_worst.to_csv(out_dir / "out_of_order_worst.csv", index=False)
    outputs.gaps_transport.to_csv(out_dir / "gaps_transport.csv", index=False)
    outputs.gaps_transport_worst.to_csv(out_dir / "gaps_transport_worst.csv", index=False)
    outputs.gaps_sampling.to_csv(out_dir / "gaps_sampling.csv", index=False)
    outputs.gaps_sampling_worst.to_csv(out_dir / "gaps_sampling_worst.csv", index=False)
    outputs.drift_header.to_csv(out_dir / "drift_header_vs_arrival.csv", index=False)
    outputs.drift_obs.to_csv(out_dir / "drift_obs_vs_arrival_diagnostic.csv", index=False)
    outputs.alignment_pairs.to_csv(out_dir / "alignment_pairs.csv", index=False)
    outputs.alignment_worst.to_csv(out_dir / "alignment_worst.csv", index=False)
    outputs.reconverge.to_csv(out_dir / "reconverge.csv", index=False)


def _df_to_markdown(df: pd.DataFrame, *, max_rows: int = 20) -> str:
    if df.empty:
        return "_No data_"
    subset = df.head(max_rows).copy()
    headers = [str(h) for h in subset.columns]
    rows = subset.astype(str).values.tolist()
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    if len(df) > max_rows:
        lines.append(f"... {len(df) - max_rows} more rows")
    return "\n".join(lines)


def write_report(
    out_dir: Path,
    *,
    outputs: AnalysisOutputs,
    baseline_mode: str,
    plots: Sequence[str],
    reconverge_plot: Optional[str],
) -> None:
    report_path = out_dir / "report.md"
    callouts: List[str] = []
    if not outputs.alignment_pairs.empty:
        worst_pair = outputs.alignment_pairs.sort_values("err_baseline_p99_ns", ascending=False).iloc[0]
        base_p99_ms = float(worst_pair["err_baseline_p99_ns"]) / 1e6
        mc_p99_ms = float(worst_pair["err_mc_p99_ns"]) / 1e6
        delta_ms = mc_p99_ms - base_p99_ms
        callouts.append(
            f"Worst baseline p99: {base_p99_ms:.2f} ms (pair {worst_pair['pair']}); MC p99: {mc_p99_ms:.2f} ms (delta {delta_ms:.2f} ms)."
        )
    if not outputs.alignment_worst.empty:
        worst_event = outputs.alignment_worst.iloc[0]
        callouts.append(
            "Worst matched example: "
            f"pair {worst_event['pair']} at arrival {worst_event['t_arrival_a_ns']:.0f} ns, "
            f"baseline err {worst_event['err_baseline_ns'] / 1e6:.2f} ms, "
            f"mc err {worst_event['err_mc_ns'] / 1e6:.2f} ms."
        )
    sections = [
        "# Impairment Report",
        "",
        "## Definitions",
        "- arrival: event arrival time (t_arrival_ns)",
        "- header: producer timestamp if available (t_header_ns)",
        "- obs: observed time (t_obs_ns or configured)",
        f"- baseline: t_est_baseline_ns (mode: {baseline_mode})",
        "- mc: t_est_mc_ns",
        "",
        "## Callouts",
        "\n".join(f"- {line}" for line in callouts) if callouts else "_No callouts generated_",
        "",
        "## Topic Inventory",
        _df_to_markdown(outputs.topic_inventory),
        "",
        "## Out-of-Order",
        _df_to_markdown(outputs.out_of_order),
        "",
        "## Transport Gaps",
        _df_to_markdown(outputs.gaps_transport),
        "",
        "## Sampling Gaps",
        _df_to_markdown(outputs.gaps_sampling),
        "",
        "## Drift (Header vs Arrival)",
        _df_to_markdown(outputs.drift_header),
        "",
        "## Drift (Obs vs Arrival Diagnostic)",
        _df_to_markdown(outputs.drift_obs),
        "",
        "## Alignment Pairs",
        _df_to_markdown(outputs.alignment_pairs),
        "",
        "## Reconverge",
        _df_to_markdown(outputs.reconverge),
        "",
    ]

    if plots:
        sections.append("## Alignment Tail Plots")
        for plot in plots:
            sections.append(f"- {plot}")
        sections.append("")

    if reconverge_plot:
        sections.append("## Reconverge Plot")
        sections.append(f"- {reconverge_plot}")
        sections.append("")

    report_path.write_text("\n".join(sections))


def run_determinism(
    frame: pd.DataFrame,
    *,
    out_dir: Path,
    config: AnalysisConfig,
    pair_configs: Sequence[AlignmentPairConfig],
    target_topic: str | None,
) -> Dict[str, object]:
    if config.show_progress:
        log_status("Impairment analysis: determinism (hash pass)")
    hashes_first = _hash_csvs(out_dir)

    tmp_dir = out_dir / "_determinism_tmp"
    _ensure_dir(tmp_dir)
    outputs_second = compute_metrics(
        frame,
        config=config,
        pair_configs=pair_configs,
        target_topic=target_topic,
    )
    write_outputs(tmp_dir, outputs_second)
    hashes_second = _hash_csvs(tmp_dir)

    hash_match = {
        name: hashes_first.get(name) == hashes_second.get(name)
        for name in sorted(set(hashes_first) | set(hashes_second))
    }

    if config.show_progress:
        log_status("Impairment analysis: determinism (shuffle pass)")
    shuffled = _shuffle_arrival(frame, bin_ns=config.shuffle_bin_ns)
    shuffled_outputs = compute_metrics(
        shuffled,
        config=config,
        pair_configs=pair_configs,
        target_topic=target_topic,
    )

    out_of_order_delta = float(
        (shuffled_outputs.out_of_order["lateness_p99_ns"].fillna(0) - outputs_second.out_of_order["lateness_p99_ns"].fillna(0)).abs().max()
        if not outputs_second.out_of_order.empty
        else float("nan")
    )
    sampling_gap_delta = float(
        (shuffled_outputs.gaps_sampling["gap_p95_ns"].fillna(0) - outputs_second.gaps_sampling["gap_p95_ns"].fillna(0)).abs().max()
        if not outputs_second.gaps_sampling.empty
        else float("nan")
    )
    drift_delta = float(
        (shuffled_outputs.drift_header["drift_ppm"].fillna(0) - outputs_second.drift_header["drift_ppm"].fillna(0)).abs().max()
        if not outputs_second.drift_header.empty
        else float("nan")
    )
    alignment_delta = float(
        (shuffled_outputs.alignment_pairs["err_mc_p95_ns"].fillna(0) - outputs_second.alignment_pairs["err_mc_p95_ns"].fillna(0)).abs().max()
        if not outputs_second.alignment_pairs.empty
        else float("nan")
    )

    shuffle_report = {
        "bin_ns": config.shuffle_bin_ns,
        "out_of_order_changed": out_of_order_delta > 0,
        "sampling_gaps_stable": sampling_gap_delta <= config.alignment_tolerance_ns,
        "drift_stable": drift_delta <= 1.0,
        "alignment_stable": alignment_delta <= config.alignment_tolerance_ns,
        "out_of_order_delta": out_of_order_delta,
        "sampling_gap_delta": sampling_gap_delta,
        "drift_delta": drift_delta,
        "alignment_delta": alignment_delta,
    }

    payload = {
        "hashes_first": hashes_first,
        "hashes_second": hashes_second,
        "hash_match": hash_match,
        "shuffle": shuffle_report,
    }
    (out_dir / "determinism.json").write_text(json.dumps(payload, indent=2))
    return payload


def run_analysis(
    events_path: Path,
    *,
    out_dir: Path,
    obs_time_col: str | None = None,
    pair_configs: Sequence[AlignmentPairConfig] | None = None,
    target_topic: str | None = None,
    config: AnalysisConfig | None = None,
    run_determinism_check: bool = True,
    run_report: bool = True,
    show_progress: bool = False,
) -> AnalysisOutputs:
    config = config or AnalysisConfig()
    config.show_progress = show_progress or config.show_progress
    if config.show_progress:
        log_status(f"Impairment analysis: loading {events_path}")
    df = read_parquet(events_path)
    standardized = standardize_events(df, obs_time_col=obs_time_col)
    if pair_configs is None:
        pair_configs = select_alignment_pairs(standardized.frame, window_ns=config.alignment_window_ns)
    outputs = compute_metrics(
        standardized.frame,
        config=config,
        pair_configs=pair_configs,
        target_topic=target_topic,
    )
    write_outputs(out_dir, outputs)

    plots = _plot_alignment_tails(outputs.alignment_matches, out_dir)
    reconverge_plot = _plot_reconverge(outputs.rolling_reconverge, out_dir)
    if run_report:
        write_report(
            out_dir,
            outputs=outputs,
            baseline_mode=standardized.baseline_mode,
            plots=plots,
            reconverge_plot=reconverge_plot,
        )

    if run_determinism_check:
        run_determinism(
            standardized.frame,
            out_dir=out_dir,
            config=config,
            pair_configs=pair_configs,
            target_topic=target_topic,
        )
    return outputs


def _parse_pair(value: str) -> Tuple[str | None, str, str]:
    source_id = None
    if ":" in value:
        source_id, value = value.split(":", 1)
    topic_a, topic_b = value.split(",", 1)
    return source_id, topic_a.strip(), topic_b.strip()


def _parse_window(value: str) -> Tuple[float, float]:
    start_s, end_s = value.split(",", 1)
    return float(start_s), float(end_s)


def _build_pair_configs(
    frame: pd.DataFrame,
    *,
    pairs: Sequence[str],
    window_ns: int,
) -> List[AlignmentPairConfig]:
    configs: List[AlignmentPairConfig] = []
    if not pairs:
        return configs
    for value in pairs:
        source_id, topic_a, topic_b = _parse_pair(value)
        if source_id is None:
            for candidate in frame["source_id"].unique():
                subset = frame[frame["source_id"] == candidate]
                topics = subset["topic"].astype(str).unique()
                if topic_a in topics and topic_b in topics:
                    name = f"{candidate}:{topic_a}_vs_{topic_b}"
                    configs.append(
                        AlignmentPairConfig(
                            name=name,
                            source_id=str(candidate),
                            topic_a=topic_a,
                            topic_b=topic_b,
                            window_ns=window_ns,
                        )
                    )
        else:
            name = f"{source_id}:{topic_a}_vs_{topic_b}"
            configs.append(
                AlignmentPairConfig(
                    name=name,
                    source_id=source_id,
                    topic_a=topic_a,
                    topic_b=topic_b,
                    window_ns=window_ns,
                )
            )
    return configs


def main(argv: List[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Impairment metrics analysis")
    parser.add_argument("--events", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--obs-time-col", default=None)
    parser.add_argument("--pair", action="append", default=[])
    parser.add_argument("--target-topic", default=None)
    parser.add_argument("--window-s", type=float, default=0.5)
    parser.add_argument("--reconverge-window", action="append", default=[])
    parser.add_argument("--skip-determinism", action="store_true")
    parser.add_argument("--skip-report", action="store_true")
    parser.add_argument("--progress", action="store_true", help="show tqdm progress bars")
    args = parser.parse_args(argv)

    config = AnalysisConfig(alignment_window_ns=int(args.window_s * 1e9))
    if args.reconverge_window:
        config.reconverge_windows_s = tuple(_parse_window(val) for val in args.reconverge_window)

    df = read_parquet(args.events)
    standardized = standardize_events(df, obs_time_col=args.obs_time_col)
    pair_configs = _build_pair_configs(
        standardized.frame,
        pairs=args.pair,
        window_ns=config.alignment_window_ns,
    )
    if not pair_configs:
        pair_configs = select_alignment_pairs(standardized.frame, window_ns=config.alignment_window_ns)

    run_analysis(
        args.events,
        out_dir=args.out,
        obs_time_col=args.obs_time_col,
        pair_configs=pair_configs,
        target_topic=args.target_topic,
        config=config,
        run_determinism_check=not args.skip_determinism,
        run_report=not args.skip_report,
        show_progress=args.progress,
    )


if __name__ == "__main__":
    main()
