"""Canonical stream key helpers shared across the evaluation pipeline."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, Tuple

import numpy as np


_UAV_RE = re.compile(r"(?:^|/)(uav\d+)(?:/|$)")
_CAMERA_RE = re.compile(r"(?:^|/)(mv_\d+)(?:/|$)")
_CAMERA_SEG_RE = re.compile(r"(?:^|/)(mv_\d+)(?:/|$)")


def find_firmware_column(columns: Iterable[str]) -> str | None:
    for name in ("firmware_version", "fw_version", "firmware"):
        if name in columns:
            return name
    return None


def find_sensor_id_column(columns: Iterable[str]) -> str | None:
    for name in ("sensor_id", "device_id", "sensor"):
        if name in columns:
            return name
    return None


def canonical_bag_id(bag: object) -> str:
    if bag is None or (isinstance(bag, float) and np.isnan(bag)):
        return "unknown"
    text = str(bag)
    if not text:
        return "unknown"
    return Path(text).stem


def canonical_uav_id(uav: object, topic: object | None = None) -> str:
    if isinstance(uav, str) and uav:
        return uav
    if topic is not None:
        topic_text = str(topic)
        if topic_text:
            match = _UAV_RE.search(topic_text)
            if match:
                return match.group(1)
            cam_match = _CAMERA_RE.search(topic_text)
            if cam_match:
                return cam_match.group(1)
    return "global"


def canonical_topic(topic: object) -> str:
    if topic is None or (isinstance(topic, float) and np.isnan(topic)):
        return "unknown"
    text = str(topic)
    return text if text else "unknown"


def extract_sensor_id(topic: object) -> str:
    if topic is None or (isinstance(topic, float) and np.isnan(topic)):
        return "unknown"
    text = str(topic)
    if not text:
        return "unknown"
    match = _CAMERA_SEG_RE.search(text)
    if match:
        return match.group(1)
    parts = text.strip("/").split("/")
    if not parts:
        return "unknown"
    if parts[0].startswith("uav") and len(parts) > 1:
        return parts[1]
    return parts[0]


def _normalize_optional(value: object | None) -> object | None:
    try:
        import pandas as pd

        if value is pd.NA:
            return None
    except Exception:
        pass
    if value is None:
        return None
    if isinstance(value, float) and np.isnan(value):
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return value


def calibration_key_parts(
    uav: object,
    topic: object,
    *,
    firmware_version: object | None = None,
    sensor_id: object | None = None,
) -> Tuple[str, str, str]:
    uav_id = canonical_uav_id(uav, topic)
    topic_id = canonical_topic(topic)
    firmware_version = _normalize_optional(firmware_version)
    sensor_id = _normalize_optional(sensor_id)
    firmware_tag = str(firmware_version) if firmware_version is not None else None
    if firmware_tag:
        return uav_id, topic_id, firmware_tag
    sensor = extract_sensor_id(topic) if sensor_id is None else str(sensor_id)
    if not sensor:
        sensor = "unknown"
    return uav_id, sensor, topic_id


def calibration_key(
    uav: object,
    topic: object,
    *,
    firmware_version: object | None = None,
    sensor_id: object | None = None,
) -> str:
    return "::".join(calibration_key_parts(
        uav,
        topic,
        firmware_version=firmware_version,
        sensor_id=sensor_id,
    ))


def stream_key_parts(uav: object, bag: object, topic: object) -> Tuple[str, str, str]:
    uav_id = canonical_uav_id(uav, topic)
    bag_id = canonical_bag_id(bag)
    topic_id = canonical_topic(topic)
    return uav_id, bag_id, topic_id


def stream_key(uav: object, bag: object, topic: object) -> str:
    uav_id, bag_id, topic_id = stream_key_parts(uav, bag, topic)
    return f"{uav_id}::{bag_id}::{topic_id}"


__all__ = [
    "canonical_bag_id",
    "canonical_topic",
    "canonical_uav_id",
    "calibration_key",
    "calibration_key_parts",
    "extract_sensor_id",
    "find_firmware_column",
    "find_sensor_id_column",
    "stream_key",
    "stream_key_parts",
]
