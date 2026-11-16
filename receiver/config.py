"""Configuration, paths, validation limits and small shared helpers."""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SOURCE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
BATCH_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
DOC_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,199}$")

# key -> (type, default, minimum, maximum)
SETTINGS: dict[str, tuple[type, Any, Any, Any]] = {
    "spool_cap_bytes": (int, 32 * 1024 * 1024, 64 * 1024, 1024 * 1024 * 1024),
    "soft_limit_pct": (float, 0.70, 0.10, 0.98),
    "max_batch_bytes": (int, 256 * 1024, 4096, 8 * 1024 * 1024),
    "max_batch_records": (int, 2000, 1, 200_000),
    "max_record_bytes": (int, 64 * 1024, 256, 1024 * 1024),
    "max_inflight_batches": (int, 512, 2, 1_000_000),
    "storage_delay_ms": (int, 0, 0, 60_000),
    "index_delay_ms": (int, 0, 0, 60_000),
    "min_free_bytes": (int, 64 * 1024 * 1024, 0, 16 * 1024 * 1024 * 1024),
    "compact_after_secs": (int, 45, 0, 86_400),
    "promotion_max_batches": (int, 16, 1, 500),
    "hot_window_batches": (int, 24, 1, 100_000),
    "cold_compact_threshold": (int, 8, 2, 1000),
    "compaction_max_segments": (int, 16, 2, 1000),
    "lease_seconds": (int, 30, 5, 600),
    "max_attempts": (int, 3, 1, 10),
    "quarantine_max_bytes": (int, 8 * 1024 * 1024, 64 * 1024, 1024 * 1024 * 1024),
    "quarantine_max_batches": (int, 128, 1, 100_000),
    "backlog_reduce_secs": (int, 8, 1, 3600),
    "sample_interval_secs": (float, 2.0, 0.5, 60.0),
    "worker_stale_secs": (int, 6, 2, 120),
    "ack_wait_secs": (float, 8.0, 0.5, 60.0),
}

DEFAULT_RESERVED_BYTES = 4 * 1024 * 1024
DEFAULT_RESERVED_BATCHES = 64
DEFAULT_SOURCE_MAX_BATCH = 256 * 1024
DEFAULT_MAX_RECORD_BYTES = 64 * 1024

SEED_SOURCES = (
    ("alpha", "Alpha local source"),
    ("beta", "Beta local source"),
)

LIVE_STATES = ("staged", "indexing")
SPOOL_STATES = ("staged", "indexing")  # quarantined payloads leave the spool
ACK_LEVELS = ("spool-durable", "indexed", "quarantined")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def epoch_now() -> float:
    return time.time()


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def records_digest(records: list[dict[str, Any]]) -> str:
    return sha256_hex(canonical_json(records))


def coerce_setting(key: str, value: Any) -> Any:
    """Validate and coerce one setting, raising ValueError with context."""
    if key not in SETTINGS:
        raise ValueError(f"unknown setting: {key}")
    typ, _default, minimum, maximum = SETTINGS[key]
    try:
        if typ is int:
            if isinstance(value, bool):
                raise ValueError("boolean is not an integer setting")
            if isinstance(value, float) and not value.is_integer():
                raise ValueError("value must be a whole number")
            coerced = int(value)
        elif typ is float:
            coerced = float(value)
        else:  # pragma: no cover - all settings above are numeric
            coerced = typ(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be {typ.__name__}") from exc
    if minimum is not None and coerced < minimum:
        raise ValueError(f"{key} must be >= {minimum}")
    if maximum is not None and coerced > maximum:
        raise ValueError(f"{key} must be <= {maximum}")
    return coerced


def default_settings() -> dict[str, Any]:
    return {key: spec[1] for key, spec in SETTINGS.items()}


def normalize_settings(raw: dict[str, Any]) -> dict[str, Any]:
    return {key: coerce_setting(key, value) for key, value in raw.items()}


@dataclass(frozen=True)
class Layout:
    data_dir: Path

    @property
    def meta_db(self) -> Path:
        return self.data_dir / "meta.db"

    @property
    def spool(self) -> Path:
        return self.data_dir / "spool"

    @property
    def hot(self) -> Path:
        return self.data_dir / "hot"

    @property
    def cold(self) -> Path:
        return self.data_dir / "cold"

    @property
    def quarantine(self) -> Path:
        return self.data_dir / "quarantine"

    @property
    def tmp(self) -> Path:
        return self.data_dir / "tmp"

    @property
    def worker_lock(self) -> Path:
        return self.data_dir / "worker.lock"

    @property
    def worker_pidfile(self) -> Path:
        return self.data_dir / "worker.pid"

    @property
    def worker_log(self) -> Path:
        return self.data_dir / "worker.log"

    @property
    def api_log(self) -> Path:
        return self.data_dir / "api.log"

    def ensure(self) -> "Layout":
        for path in (
            self.data_dir,
            self.spool,
            self.hot,
            self.cold,
            self.quarantine,
            self.tmp,
        ):
            path.mkdir(parents=True, exist_ok=True)
        return self

    def abs_path(self, relative: str | os.PathLike[str]) -> Path:
        """Resolve a stored relative path and refuse traversal."""
        if not relative:
            raise ValueError("empty path")
        base = self.data_dir.resolve()
        target = (base / Path(relative)).resolve()
        if target != base and base not in target.parents:
            raise ValueError("path escapes data directory")
        return target

    def relpath(self, path: Path) -> str:
        return str(path.resolve().relative_to(self.data_dir.resolve()))


def fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def durable_write(path: Path, data: bytes, tmp_dir: Path) -> None:
    """Write bytes, fsync, atomically rename and fsync the parent directory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp = tmp_dir / f"{uuid.uuid4().hex}.part"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            if written <= 0:  # pragma: no cover - defensive
                raise OSError("short write")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path)
    fsync_dir(path.parent)


def dir_size(path: Path) -> int:
    total = 0
    stack = [path]
    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as it:
                for entry in it:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(Path(entry.path))
                        elif entry.is_file(follow_symlinks=False):
                            total += entry.stat(follow_symlinks=False).st_size
                    except FileNotFoundError:
                        continue
        except FileNotFoundError:
            continue
    return total


def disk_usage(path: Path) -> tuple[int, int]:
    usage = os.statvfs(path)
    free = usage.f_bavail * usage.f_frsize
    total = usage.f_blocks * usage.f_frsize
    return free, total


def test_crash(point: str) -> None:
    """Fault-injection hook used by the crash-recovery tests.

    Set ``POINTWISE_CRASH_AFTER=<point>`` in the *process under test* to make
    it die abruptly at a named boundary between a file write and its metadata
    commit.  Never set in normal operation.
    """
    if os.environ.get("POINTWISE_CRASH_AFTER") == point:
        os._exit(70)


def process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True
