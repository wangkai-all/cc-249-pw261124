"""The index worker: an independent process that drains the spool.

Running this as a separate OS process is the point of the exercise:

* the API can keep accepting within credit while indexing is slowed;
* ``SIGKILL`` cannot leave a half-committed metadata state, because every
  file write is fsynced before its metadata row, and every row change is a
  single transaction;
* on restart the worker holds an exclusive lock and returns every
  ``indexing`` lease to ``staged``.  The batch is then re-indexed from the
  spool.  Hot files are deterministic per batch and replaced atomically, and
  record-index upserts are idempotent, so replaying cannot duplicate rows.

Fault switches live in the settings table (so they survive restarts and are
visible to the dashboard): ``index_delay_ms``, ``storage_delay_ms``,
``max_attempts``.  A "corrupt payload" test flips bytes in the spool file
while the worker is stopped; validation then catches it and quarantines only
that batch.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import signal
import sqlite3
import sys
import time
import traceback
import uuid
from pathlib import Path
from typing import Any

from .config import (
    Layout,
    canonical_json,
    disk_usage,
    epoch_now,
    process_alive,
    sha256_hex,
    test_crash,
    utc_now,
)
from .meta import Meta
from .storage import Storage, StorageError

BATCH_STATE_INDEXING = "indexing"


class Worker:
    def __init__(self, layout: Layout, *, owner: str | None = None):
        self.layout = layout.ensure()
        self.meta = Meta(layout)
        self.storage = Storage(layout)
        self.owner = owner or f"{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self.stop_requested = False
        self.lock_fd: int | None = None

    # -- lifecycle ----------------------------------------------------------
    def acquire_lock(self) -> bool:
        fd = os.open(self.layout.worker_lock, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            return False
        os.ftruncate(fd, 0)
        os.write(fd, str(os.getpid()).encode())
        os.fsync(fd)
        self.lock_fd = fd
        self.layout.worker_pidfile.write_text(str(os.getpid()))
        return True

    def release_lock(self) -> None:
        if self.lock_fd is not None:
            try:
                fcntl.flock(self.lock_fd, fcntl.LOCK_UN)
                os.close(self.lock_fd)
            except OSError:
                pass
            self.lock_fd = None
        try:
            self.layout.worker_pidfile.unlink()
        except FileNotFoundError:
            pass

    def request_stop(self, *_args: Any) -> None:
        self.stop_requested = True

    def log(self, level: str, kind: str, message: str, **detail: Any) -> None:
        record = {
            "at": utc_now(),
            "level": level,
            "kind": kind,
            "message": message,
            "detail": detail or None,
        }
        try:
            with open(self.layout.worker_log, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        except OSError:
            pass
        if level in ("warn", "error"):
            print(json.dumps(record, ensure_ascii=False), file=sys.stderr, flush=True)
        else:
            print(json.dumps(record, ensure_ascii=False), flush=True)

    def event(self, level: str, kind: str, message: str, **kwargs: Any) -> None:
        try:
            self.meta.add_event(level, kind, message, **kwargs)
        except sqlite3.Error as exc:  # pragma: no cover - defensive
            self.log("warn", "event_failed", str(exc))

    # -- startup recovery ---------------------------------------------------
    def recover(self) -> dict[str, int]:
        """Clean up after a previous worker that died without unlinking files.

        The API keeps admitting batches while we run, so "not referenced by a
        row I read a moment ago" is not proof of an orphan.  Only files whose
        mtime predates this scan can be orphans: anything newer may belong to
        a row committed after the snapshot, and deleting it would quarantine a
        perfectly good batch.  Recovered leases are handled separately by the
        database and are always safe to replay.
        """
        scan_started = time.time()
        # The API may have fsynced a spool file microseconds before the row
        # that names it is committed.  Only old, still-unreferenced files can
        # be treated as abandoned.
        orphan_grace = 120.0
        removed = self.storage.cleanup_tmp()
        recovered = self.meta.recover_all_leases()
        rows = self.meta.all_batches()
        referenced_spool = {
            Path(r["spool_path"]).name for r in rows if r["spool_path"]
        }
        referenced_hot = {r["hot_path"] for r in rows if r["hot_path"]}
        cold_refs = {r["cold_path"] for r in rows if r["cold_path"]} | {
            s["relpath"] for s in self.meta.list_segment_files()
        }
        orphans = 0

        def stale(path: Path) -> bool:
            try:
                return path.stat().st_mtime < scan_started - orphan_grace
            except FileNotFoundError:
                return False

        for name in self.storage.spool_files():
            if name in referenced_spool:
                continue
            path = self.layout.spool / name
            if stale(path):
                try:
                    path.unlink()
                    orphans += 1
                except FileNotFoundError:
                    pass
        for rel in self.storage.hot_files():
            if rel in referenced_hot:
                continue
            path = self.layout.abs_path(rel)
            if stale(path):
                try:
                    path.unlink()
                    orphans += 1
                except FileNotFoundError:
                    pass
        for rel in self.storage.cold_files():
            if rel in cold_refs:
                continue
            path = self.layout.abs_path(rel)
            if stale(path):
                try:
                    path.unlink()
                    orphans += 1
                except FileNotFoundError:
                    pass
        summary = {
            "tmp_removed": removed,
            "leases_recovered": recovered,
            "orphans_removed": orphans,
        }
        if any(summary.values()):
            self.log("info", "recovery", "startup recovery complete", **summary)
            self.event("info", "recovery", "worker recovered interrupted work", detail=summary)
        return summary

    # -- one indexing step --------------------------------------------------
    def settings(self) -> dict[str, Any]:
        return self.meta.get_settings()

    def index_one(self, *, skip_seq: int | None = None) -> str | None:
        """Lease and index at most one batch.

        Returns ``None`` when idle, otherwise ``indexed`` or ``quarantine``.
        """
        settings = self.settings()
        if settings.get("index_delay_ms"):
            time.sleep(float(settings["index_delay_ms"]) / 1000.0)
        batch = self.meta.lease_next_batch(
            self.owner, int(settings["lease_seconds"])
        )
        if batch is None:
            return None
        if skip_seq is not None and batch["seq"] == skip_seq:
            self.meta.release_lease(batch["seq"], self.owner)
            return None
        try:
            result = self._process(batch, settings)
        except StorageError as exc:
            result = self._failed(batch, f"{exc}", exc.kind, settings)
        except (sqlite3.Error, OSError) as exc:
            self.log("error", "index_error", f"batch {batch['seq']} failed: {exc}")
            result = self._failed(batch, f"worker error: {exc}", "transient", settings)
        except Exception as exc:
            # A programming error must not strand the batch behind a lease
            # for the full timeout.  Release it and keep the worker alive.
            self.log(
                "error",
                "unhandled",
                f"batch {batch['seq']}: {exc!r}",
                traceback=traceback.format_exc(limit=6),
            )
            try:
                self.meta.release_lease(int(batch["seq"]), self.owner)
            except sqlite3.Error:
                pass
            result = "error"
        return result

    def _process(self, batch: dict[str, Any], settings: dict[str, Any]) -> str:
        seq = int(batch["seq"])
        storage_delay = float(settings.get("storage_delay_ms") or 0) / 1000.0
        if storage_delay:
            time.sleep(storage_delay)
        try:
            raw = self.storage.spool_read(batch["spool_path"])
        except (FileNotFoundError, ValueError) as exc:
            return self._failed(
                batch,
                f"spool payload missing: {exc}",
                "spool_missing",
                settings,
                keep_spool=False,
            )
        # The spool copy is authoritative for what was acknowledged.  Verify
        # its hash before trusting it: this is also what catches a bad batch
        # that was written by a faulty producer or damaged on disk.
        actual = hashlib.sha256(raw).hexdigest()
        if actual != batch["content_sha256"]:
            return self._quarantine(
                batch,
                f"spool checksum mismatch: expected {batch['content_sha256']}, got {actual}",
                "checksum_mismatch",
                settings,
            )
        try:
            doc = json.loads(raw.decode("utf-8"))
            records = doc["records"]
            assert isinstance(records, list)
        except (UnicodeDecodeError, json.JSONDecodeError, KeyError, AssertionError) as exc:
            return self._quarantine(
                batch, f"spool payload is not indexable JSON: {exc}", "corrupt_payload", settings
            )
        # Re-validate at index time.  Ingest already quarantines bad records,
        # but the stored copy is what this worker actually consumes, and a
        # requeued or replayed batch must not skip validation.
        from .validate import _validate_record

        bad = [
            {"index": i, "problem": problem}
            for i, record in enumerate(records)
            if (problem := _validate_record(record, int(settings["max_record_bytes"])))
        ]
        if bad:
            return self._quarantine(
                batch,
                f"{len(bad)} invalid record(s); first: {bad[0]['problem']}",
                "invalid_record",
                settings,
                detail={"invalid": bad[:10]},
            )
        records_digest = sha256_hex(canonical_json(records))
        if records_digest != batch["records_sha256"]:
            return self._quarantine(
                batch,
                "record content changed since admission",
                "content_changed",
                settings,
            )
        if not self.meta.owns_lease(seq, self.owner):
            return "lost_lease"
        hot_rel, _digest = self.storage.write_hot(
            batch["source_id"],
            batch["batch_id"],
            seq=seq,
            digest=batch["content_sha256"],
            records=records,
        )
        test_crash("after-hot-write")
        if storage_delay:
            time.sleep(storage_delay)
        self.meta.finish_indexed(seq, hot_path=hot_rel, records=records)
        self.storage.spool_unlink(batch["spool_path"])
        self.log(
            "info",
            "indexed",
            f"batch {batch['source_id']}/{batch['batch_id']} indexed",
            seq=seq,
            records=len(records),
        )
        return "indexed"

    def _failed(
        self,
        batch: dict[str, Any],
        error: str,
        kind: str,
        settings: dict[str, Any],
        *,
        keep_spool: bool = True,
    ) -> str:
        max_attempts = int(settings["max_attempts"])
        state, attempts = self.meta.fail_attempt(
            int(batch["seq"]), error=error, error_kind=kind, max_attempts=max_attempts
        )
        if state == "quarantine":
            self.storage.spool_unlink(batch["spool_path"])
            self.event(
                "error",
                "batch_quarantined",
                f"batch {batch['source_id']}/{batch['batch_id']} quarantined after {attempts} attempts",
                source_id=batch["source_id"],
                batch_id=batch["batch_id"],
                detail={"error": error, "kind": kind},
            )
        else:
            self.log(
                "warn",
                "retry",
                f"batch {batch['seq']} attempt {attempts} failed: {error}",
                kind=kind,
            )
        return "quarantine" if state == "quarantine" else "retry"

    def _quarantine(
        self,
        batch: dict[str, Any],
        error: str,
        kind: str,
        settings: dict[str, Any],
        *,
        detail: Any = None,
    ) -> str:
        self.meta.transition_to_quarantine(
            int(batch["seq"]), error=error, error_kind=kind
        )
        self.storage.spool_unlink(batch["spool_path"])
        self.event(
            "error",
            "batch_quarantined",
            f"batch {batch['source_id']}/{batch['batch_id']} quarantined: {error}",
            source_id=batch["source_id"],
            batch_id=batch["batch_id"],
            detail={"kind": kind, **(detail or {})},
        )
        self.log(
            "warn",
            "quarantine",
            f"batch {batch['seq']} quarantined ({kind}): {error}",
        )
        return "quarantine"

    # -- maintenance --------------------------------------------------------
    def promote(self) -> int:
        """Copy the oldest hot batches into a shared cold segment."""
        settings = self.settings()
        window = int(settings["hot_window_batches"])
        hot_total = self.meta.hot_batch_count()
        excess = hot_total - window
        if excess <= 0:
            return 0
        limit = min(int(settings["promotion_max_batches"]), excess)
        batches = self.meta.hot_batches_for_promotion(limit)
        if not batches:
            return 0
        try:
            segment = self.storage.write_cold_segment(batches)
        except StorageError as exc:
            self.log("error", "promotion_failed", str(exc))
            return 0
        test_crash("after-cold-write")
        self.meta.record_cold_segment(
            relpath=segment["relpath"],
            sha256=segment["sha256"],
            records=segment["records"],
            byte_count=segment["bytes"],
            source_count=segment["source_count"],
            batch_seqs=segment["batch_seqs"],
        )
        # Only now is it safe to unlink the hot copies: the metadata commit
        # above is the linearisation point for "cold is authoritative".
        for batch in batches:
            self.storage.remove_hot(batch["hot_path"])
        self.event(
            "info",
            "promoted",
            f"promoted {len(batches)} batch(es) to cold",
            detail={
                "segment": segment["relpath"],
                "batches": len(batches),
                "bytes": segment["bytes"],
            },
        )
        self.log(
            "info",
            "promoted",
            f"{len(batches)} batch(es) -> {segment['relpath']}",
            bytes=segment["bytes"],
        )
        return len(batches)

    def compact(self) -> bool:
        """Merge cold segments to bound file count."""
        settings = self.settings()
        threshold = int(settings["cold_compact_threshold"])
        maximum = int(settings["compaction_max_segments"])
        files = self.meta.cold_segment_files()
        if len(files) < threshold:
            return False
        candidates = files[:maximum]
        docs: list[dict[str, Any]] = []
        consumed: list[int] = []
        seen: set[tuple[str, str]] = set()
        for segment in candidates:
            try:
                batches = self.storage.read_cold_batches(segment["relpath"])
            except StorageError as exc:
                self.log("error", "compaction_skipped", str(exc))
                return False
            for doc in batches:
                key = (doc["source_id"], doc["batch_id"])
                if key in seen:
                    continue
                seen.add(key)
                docs.append(doc)
            consumed.append(segment["id"])
        if len(consumed) < 2:
            return False
        try:
            merged = self.storage.write_merged_cold(docs)
        except StorageError as exc:
            self.log("error", "compaction_failed", str(exc))
            return False
        self.meta.replace_cold_segments(
            consumed_ids=consumed,
            relpath=merged["relpath"],
            sha256=merged["sha256"],
            records=merged["records"],
            byte_count=merged["bytes"],
            source_count=merged["source_count"],
            batch_seqs=merged["batch_seqs"],
        )
        for segment in candidates:
            self.storage.remove_cold(segment["relpath"])
        self.event(
            "info",
            "compacted",
            f"merged {len(consumed)} cold segments into one",
            detail={"files": len(consumed), "bytes": merged["bytes"]},
        )
        self.log(
            "info",
            "compacted",
            f"{len(consumed)} cold segments -> {merged['relpath']}",
        )
        return True

    def maybe_compact(self) -> None:
        try:
            self.compact()
        except (sqlite3.Error, OSError) as exc:  # pragma: no cover - defensive
            self.log("error", "compaction_error", str(exc))

    def sample_point(self) -> None:
        settings = self.settings()
        live = self.meta.live_spool()
        disk = self.storage.disk()
        db = self.meta.db_stats()
        totals = self.meta.state_totals()
        records = self.meta.record_index_count()
        storage = self.meta.storage_totals()
        oldest = live["oldest_live_epoch"]
        lag = max(0.0, time.time() - oldest) if oldest else 0.0
        counts = {
            kind: n
            for kind, n in self.meta.event_counts(time.time() - 60).items()
        }
        payload = {
            "live_batches": live["live_batches"],
            "live_bytes": live["live_bytes"],
            "quarantine_batches": live["quarantine_batches"],
            "quarantine_bytes": live["quarantine_bytes"],
            "disk_free": disk["free_bytes"],
            "disk_total": disk["total_bytes"],
            "db_bytes": db["db_bytes"],
            "wal_bytes": db["wal_bytes"],
            "indexed_batches": totals.get("indexed", {}).get("count", 0),
            "indexed_records": records,
            "hot_batches": storage["hot_batches"],
            "cold_batches": storage["cold_batches"],
            "lag_secs": round(lag, 3),
            "events_60s": counts,
        }
        self.meta.add_sample(payload)
        cutoff = time.time() - 3600
        self.meta.prune_samples(cutoff)

    # -- main loop ----------------------------------------------------------
    def run(self, *, poll: float = 0.2, tenantless: bool = False) -> int:
        if not self.acquire_lock():
            self.log("error", "lock_busy", "another worker holds the lock")
            print("worker already running", file=sys.stderr)
            return 3
        signal.signal(signal.SIGTERM, self.request_stop)
        signal.signal(signal.SIGINT, self.request_stop)
        self.log("info", "started", "worker started", pid=os.getpid(), owner=self.owner)
        self.event("info", "worker", "worker started", detail={"owner": self.owner})
        self.recover()
        # Promote once at startup so a crash during cold writing is reachable
        # deterministically in tests and a restarted worker resumes compaction.
        try:
            self.promote()
            self.maybe_compact()
        except Exception as exc:  # maintenance must not kill the worker
            self.log("error", "startup_maintenance", repr(exc))
        last_promote = 0.0
        last_compact = 0.0
        last_sample = 0.0
        idle = 0
        try:
            while not self.stop_requested:
                settings = self.settings()
                now = time.monotonic()
                try:
                    result = self.index_one()
                except Exception as exc:  # pragma: no cover - keep the worker alive
                    self.log(
                        "error",
                        "unhandled",
                        str(exc),
                        traceback=traceback.format_exc(limit=4),
                    )
                    result = None
                if result is None or result == "retry":
                    idle += 1
                else:
                    idle = 0
                if now - last_promote >= float(settings["sample_interval_secs"]):
                    try:
                        self.promote()
                    except Exception as exc:  # maintenance must not kill the worker
                        self.log("error", "promotion_error", repr(exc))
                    last_promote = now
                if now - last_compact >= float(settings["compact_after_secs"]):
                    self.maybe_compact()
                    last_compact = now
                if now - last_sample >= float(settings["sample_interval_secs"]):
                    try:
                        self.sample_point()
                    except Exception as exc:  # maintenance must not kill the worker
                        self.log("warn", "sample_error", repr(exc))
                    last_sample = now
                if result is None:
                    time.sleep(min(0.5, poll * (1 + idle // 5)))
                else:
                    time.sleep(poll)
        finally:
            self.log("info", "stopped", "worker stopped", pid=os.getpid())
            self.event("info", "worker", "worker stopped", detail={"owner": self.owner})
            self.release_lock()
        return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="pointwise receiver index worker")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--poll", type=float, default=0.2)
    args = parser.parse_args(argv)
    layout = Layout(Path(args.data_dir)).ensure()
    meta = Meta(layout)
    meta.initialize()
    return Worker(layout).run(poll=args.poll)


if __name__ == "__main__":
    raise SystemExit(main())
