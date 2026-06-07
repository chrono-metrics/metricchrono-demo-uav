"""Optional progress helpers backed by tqdm."""

from __future__ import annotations

import sys
from typing import Iterable, Optional, TypeVar


T = TypeVar("T")
_WARNED_MISSING_TQDM = False


def maybe_tqdm(
    iterable: Iterable[T],
    *,
    enabled: bool,
    desc: str | None = None,
    total: Optional[int] = None,
) -> Iterable[T]:
    if not enabled:
        return iterable
    try:
        from tqdm import tqdm
    except ImportError:
        global _WARNED_MISSING_TQDM
        if not _WARNED_MISSING_TQDM:
            print("tqdm not installed; progress bars disabled", file=sys.stderr)
            _WARNED_MISSING_TQDM = True
        return iterable
    return tqdm(iterable, desc=desc, total=total)


def log_status(message: str) -> None:
    print(f"[swarm_eval] {message}", file=sys.stdout, flush=True)


def log_progress(label: str, *, current: int, total: Optional[int] = None) -> None:
    if total:
        pct = (current / total) * 100.0
        log_status(f"{label}: {current}/{total} ({pct:.1f}%)")
    else:
        log_status(f"{label}: {current}")


__all__ = ["maybe_tqdm", "log_status", "log_progress"]
