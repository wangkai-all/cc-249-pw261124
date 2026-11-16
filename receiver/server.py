"""HTTP API and operations dashboard for the receiver.

The API process owns ingestion and control.  It never performs indexing
itself: spooling, credit checks and acknowledgements happen here, while the
separate worker process drains the queue.  ``/ingest`` acknowledges
``spool-durable`` once bytes and a metadata row are on disk; callers that
need stronger confirmation can ask it to wait for ``indexed``.
"""

from __future__ import annotations

import fcntl
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .config import (
    BATCH_ID_RE,
    SOURCE_ID_RE,
    Layout,
    coerce_setting,
    disk_usage,
    epoch_now,
    process_alive,
    records_digest,
    test_crash,
    sha256_hex,
)
from .credit import compute_credit, refusal_retry_after
from .meta import Meta
from .storage import Storage, StorageError
from .validate import Reject, parse_batch

ROOT = Path(__file__).resolve().parent.parent
MAX_CONTROL_BODY = 256 * 1024


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


class ReceiverState:
    """Process-wide state for the API: worker handle and integrity report."""

    def __init__(self, layout: Layout):
        self.layout = layout
        self.meta = Meta(layout)
        self.storage = Storage(layout)
        self.worker_proc: subprocess.Popen[bytes] | None = None
        self.worker_started_epoch: float | None = None
        self.integrity_lock = threading.Lock()
        self.integrity_report: dict[str, Any] | None = None
        self.integrity_running = False
        self.started_epoch = time.time()
        self.ingest_lock = threading.Lock()
        self.throttle_log_at: dict[str, float] = {}
        self.counters = {
            "accepted": 0,
            "duplicate": 0,
            "conflict": 0,
            "throttled": 0,
            "rejected": 0,
            "quarantined_ingest": 0,
        }

    # -- worker process management -----------------------------------------
    def worker_running(self) -> bool:
        if self.worker_proc is not None and self.worker_proc.poll() is None:
            return True
        return self._lock_held()

    def _lock_held(self) -> bool:
        path = self.layout.worker_lock
        try:
            fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        except OSError:
            return False
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                pass
            os.close(fd)
        return False

    def worker_pid(self) -> int | None:
        if self.worker_proc is not None and self.worker_proc.poll() is None:
            return self.worker_proc.pid
        try:
            pid = int(self.layout.worker_pidfile.read_text().strip())
        except (FileNotFoundError, ValueError):
            return None
        return pid if process_alive(pid) else None

    def start_worker(self, *, extra_env: dict[str, str] | None = None) -> dict[str, Any]:
        if self.worker_running():
            return {"ok": True, "already": True, "pid": self.worker_pid()}
        env = os.environ.copy()
        env.setdefault("PYTHONUNBUFFERED", "1")
        env["PYTHONPATH"] = str(ROOT) + (
            os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""
        )
        if extra_env:
            env.update(extra_env)
        log = open(self.layout.worker_log, "ab", buffering=0)
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "receiver.worker",
                "--data-dir",
                str(self.layout.data_dir),
            ],
            cwd=str(ROOT),
            env=env,
            stdout=log,
            stderr=log,
            start_new_session=True,
            close_fds=True,
        )
        log.close()
        self.worker_proc = proc
        self.worker_started_epoch = time.time()
        self.meta.add_event(
            "info", "worker_start", "worker process started", detail={"pid": proc.pid}
        )
        return {"ok": True, "pid": proc.pid, "already": False}

    def stop_worker(self, *, hard: bool = True) -> dict[str, Any]:
        pid = self.worker_pid()
        if pid is None and not self.worker_running():
            return {"ok": True, "already": True}
        if pid is None:
            return {"ok": False, "error": "worker pid unknown"}
        sig = signal.SIGKILL if hard else signal.SIGTERM
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            return {"ok": True, "already": True}
        deadline = time.time() + 5
        while time.time() < deadline and process_alive(pid):
            time.sleep(0.02)
        if self.worker_proc is not None:
            self.worker_proc.poll()
        self.meta.add_event(
            "warn",
            "worker_stop",
            f"worker process {'killed' if hard else 'stopped'}",
            detail={"pid": pid, "signal": sig},
        )
        return {"ok": True, "pid": pid, "hard": hard}

    def worker_info(self) -> dict[str, Any]:
        running = self.worker_running()
        pid = self.worker_pid() if running else None
        log_lines: list[str] = []
        try:
            with open(self.layout.worker_log, "r", encoding="utf-8", errors="replace") as fh:
                tail = fh.readlines()[-8:]
            log_lines = [line.strip() for line in tail if line.strip()]
        except FileNotFoundError:
            pass
        return {
            "running": running,
            "pid": pid,
            "started_epoch": self.worker_started_epoch,
            "recent_log": log_lines,
        }

    # -- integrity ----------------------------------------------------------
    def run_integrity(self) -> dict[str, Any]:
        if not self.integrity_lock.acquire(blocking=False):
            return {"ok": False, "error": "integrity check already running"}
        self.integrity_running = True
        started = time.time()
        try:
            report = self._integrity_check()
            report["started_epoch"] = started
            report["finished_epoch"] = time.time()
            self.integrity_report = report
            level = "info" if report["ok"] else "error"
            self.meta.add_event(
                level,
                "integrity",
                "integrity check "
                + ("passed" if report["ok"] else f"found {len(report['issues'])} issue(s)"),
                detail={
                    "checked_batches": report["checked_batches"],
                    "issues": len(report["issues"]),
                },
            )
            return {"ok": True, "report": report}
        finally:
            self.integrity_running = False
            self.integrity_lock.release()

    def _integrity_check(self) -> dict[str, Any]:
        issues: list[dict[str, Any]] = []
        rows = self.meta.all_batches()
        indexed = [r for r in rows if r["state"] == "indexed"]
        checked = 0
        records_expected = 0
        for row in indexed:
            checked += 1
            records_expected += int(row["record_count"])
            path = row["hot_path"] or row["cold_path"]
            if not path:
                issues.append({"kind": "missing_path", "seq": row["seq"]})
                continue
            try:
                payload = self.layout.abs_path(path).read_bytes()
            except (FileNotFoundError, ValueError):
                issues.append({"kind": "missing_file", "seq": row["seq"], "path": path})
                continue
            try:
                if path.endswith(".gz"):
                    import gzip

                    doc = json.loads(gzip.decompress(payload).decode("utf-8"))
                    batch_docs = doc.get("batches", [])
                    match = next(
                        (b for b in batch_docs if b.get("batch_seq") == row["seq"]), None
                    )
                    if match is None:
                        issues.append(
                            {"kind": "missing_from_segment", "seq": row["seq"], "path": path}
                        )
                        continue
                    records = match["records"]
                else:
                    doc = json.loads(payload.decode("utf-8"))
                    records = doc["records"]
            except (
                OSError,
                UnicodeDecodeError,
                json.JSONDecodeError,
                KeyError,
                TypeError,
                EOFError,
            ):
                issues.append({"kind": "unreadable_file", "seq": row["seq"], "path": path})
                continue
            actual_digest = sha256_hex(
                json.dumps(records, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
            )
            if actual_digest != row["records_sha256"]:
                issues.append(
                    {"kind": "records_digest_mismatch", "seq": row["seq"], "path": path}
                )
            if len(records) != row["record_count"]:
                issues.append(
                    {
                        "kind": "record_count_mismatch",
                        "seq": row["seq"],
                        "expected": row["record_count"],
                        "actual": len(records),
                    }
                )
        index_count = self.meta.record_index_count()
        if index_count != records_expected:
            issues.append(
                {
                    "kind": "index_count_mismatch",
                    "expected": records_expected,
                    "actual": index_count,
                }
            )
        for segment in self.meta.list_segment_files():
            try:
                path = self.layout.abs_path(segment["relpath"])
                digest = sha256_hex(path.read_bytes())
            except (FileNotFoundError, ValueError, OSError):
                issues.append(
                    {"kind": "missing_segment", "path": segment["relpath"]}
                )
                continue
            if digest != segment["sha256"]:
                issues.append(
                    {"kind": "segment_digest_mismatch", "path": segment["relpath"]}
                )
        totals = self.meta.state_totals()
        return {
            "ok": not issues,
            "checked_batches": checked,
            "indexed_records": records_expected,
            "record_index_count": index_count,
            "segment_files": len(self.meta.list_segment_files()),
            "states": totals,
            "issues": issues[:100],
        }


class Handler(BaseHTTPRequestHandler):
    server_version = "pointwise-receiver/1.0"
    protocol_version = "HTTP/1.1"
    timeout = 30

    state: ReceiverState  # set by make_server

    # -- low-level ----------------------------------------------------------
    def log_message(self, fmt: str, *args: Any) -> None:
        # Keep stdout clean for the operator; structured events live in SQLite.
        return

    def _send(
        self,
        status: int,
        body: Any,
        *,
        content_type: str = "application/json; charset=utf-8",
        headers: dict[str, str] | None = None,
    ) -> None:
        payload = body if isinstance(body, bytes) else _json_bytes(body)
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _log_exception(self) -> None:
        """Persist an unexpected exception next to the data, for diagnosis.

        The API log may be a closed pipe or rotated; the data directory is
        where an operator will actually look after a 500.
        """
        detail = traceback.format_exc()
        sys.stderr.write(detail)
        try:
            path = self.state.layout.data_dir / "api-errors.log"
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps({"at": epoch_now(), "traceback": detail}) + "\n")
        except Exception:
            pass

    def _error(self, status: int, code: str, message: str, **extra: Any) -> None:
        self._send(status, {"ok": False, "error": {"code": code, "message": message, **extra}})

    def _read_body(self, limit: int) -> bytes:
        length_header = self.headers.get("Content-Length")
        if length_header is None:
            raise Reject(411, "length_required", "Content-Length is required")
        try:
            length = int(length_header)
        except ValueError as exc:
            raise Reject(400, "bad_length", "Content-Length is not an integer") from exc
        if length < 0:
            raise Reject(400, "bad_length", "Content-Length is negative")
        if length > limit:
            raise Reject(
                413, "body_too_large", f"body exceeds {limit} bytes", limit=limit
            )
        remaining = length
        chunks: list[bytes] = []
        while remaining:
            chunk = self.rfile.read(min(remaining, 64 * 1024))
            if not chunk:
                raise Reject(400, "truncated_body", "connection closed mid-body")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _json_body(self) -> dict[str, Any]:
        raw = self._read_body(MAX_CONTROL_BODY)
        if not raw:
            return {}
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise Reject(400, "invalid_json", f"invalid JSON: {exc}") from exc
        if not isinstance(value, dict):
            raise Reject(400, "invalid_json", "expected a JSON object")
        return value

    # -- routing ------------------------------------------------------------
    def do_GET(self) -> None:  # noqa: N802
        try:
            self._route_get()
        except Reject as exc:
            self._send(exc.status, exc.body())
        except Exception:  # pragma: no cover - surfaced to operator
            self._log_exception()
            self._error(500, "internal", "unexpected server error")

    def do_POST(self) -> None:  # noqa: N802
        try:
            self._route_post()
        except Reject as exc:
            self._send(exc.status, exc.body())
        except StorageError as exc:
            self._error(507, "storage_error", str(exc))
        except sqlite3.Error as exc:
            self._error(503, "metadata_busy", f"metadata database error: {exc}")
        except Exception:  # pragma: no cover
            self._log_exception()
            self._error(500, "internal", "unexpected server error")

    def do_HEAD(self) -> None:  # noqa: N802
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _route_get(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        if path in ("/", "/index.html"):
            return self._serve_static("index.html")
        if path == "/static/app.js":
            return self._serve_static("app.js")
        if path == "/static/style.css":
            return self._serve_static("style.css")
        if path == "/healthz":
            return self._send(200, {"ok": True, "at": epoch_now()})
        if path == "/api/status":
            return self._send(200, self._status())
        if path == "/api/control/worker":
            return self._send(200, {"ok": True, **self.state.worker_info()})
        if path == "/api/events":
            limit = int(query.get("limit", ["80"])[0])
            return self._send(200, {"events": self.state.meta.list_events(min(limit, 500))})
        if path == "/api/batches":
            return self._send(
                200,
                {
                    "pending": self.state.meta.pending_batches(100),
                    "recent": self.state.meta.recent_batches(30),
                },
            )
        if path == "/api/records":
            source = query.get("source", [None])[0]
            limit = min(int(query.get("limit", ["50"])[0]), 500)
            offset = max(0, int(query.get("offset", ["0"])[0]))
            return self._send(
                200,
                {
                    "records": self.state.meta.query_records(source, limit, offset),
                    "total_indexed": self.state.meta.record_index_count(),
                },
            )
        if path == "/api/integrity":
            return self._send(
                200,
                {
                    "report": self.state.integrity_report,
                    "running": self.state.integrity_running,
                },
            )
        if path.startswith("/api/credit/"):
            source_id = path.rsplit("/", 1)[-1]
            source = self.state.meta.get_source(source_id)
            if source is None:
                return self._error(404, "unknown_source", f"unknown source {source_id}")
            credit = self._credit_for(source)
            return self._send(200, credit.to_wire())
        if path == "/api/logs":
            name = query.get("name", ["worker"])[0]
            target = (
                self.state.layout.worker_log
                if name == "worker"
                else self.state.layout.api_log
            )
            try:
                lines = target.read_text(errors="replace").splitlines()[-120:]
            except FileNotFoundError:
                lines = []
            return self._send(200, {"name": name, "lines": lines})
        return self._error(404, "not_found", f"no route for {path}")

    def _route_post(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        if path.startswith("/ingest"):
            return self._ingest(path)
        if path == "/api/settings":
            body = self._json_body()
            try:
                updated = self.state.meta.set_settings(body)
            except ValueError as exc:
                return self._error(422, "invalid_setting", str(exc))
            self.state.meta.add_event(
                "info", "settings", "settings updated", detail=updated
            )
            return self._send(200, {"ok": True, "settings": self.state.meta.get_settings()})
        if path.startswith("/api/sources/"):
            parts = path.split("/")
            if len(parts) < 4:
                return self._error(404, "not_found", "source id missing")
            source_id = parts[3]
            body = self._json_body()
            fields: dict[str, Any] = {}
            try:
                if "active" in body:
                    fields["active"] = 1 if body["active"] else 0
                if "label" in body:
                    label = body["label"]
                    if not isinstance(label, str) or not label.strip():
                        raise ValueError("label must be a non-empty string")
                    fields["label"] = label.strip()[:120]
                for key, maximum in (
                    ("reserved_bytes", 1024 * 1024 * 1024),
                    ("reserved_batches", 1_000_000),
                    ("max_batch_bytes", 8 * 1024 * 1024),
                    ("max_record_bytes", 1024 * 1024),
                ):
                    if key in body:
                        value = int(body[key])
                        if not (1 <= value <= maximum):
                            raise ValueError(f"{key} must be between 1 and {maximum}")
                        fields[key] = value
                self.state.meta.update_source(source_id, **fields)
            except (ValueError, TypeError) as exc:
                return self._error(422, "invalid_source", str(exc))
            except KeyError:
                return self._error(404, "unknown_source", f"unknown source {source_id}")
            self.state.meta.add_event(
                "info",
                "source_update",
                f"source {source_id} updated",
                source_id=source_id,
                detail=body,
            )
            source = self.state.meta.get_source(source_id)
            return self._send(
                200, {"ok": True, "source": source, "credit": self._credit_for(source).to_wire()}
            )
        if path.startswith("/api/batches/") and path.endswith("/requeue"):
            seq = self._seq_from_path(path)
            if seq is None:
                return self._error(400, "bad_seq", "batch seq must be an integer")
            changed = self.state.meta.requeue_batch(seq)
            if changed:
                self.state.meta.add_event(
                    "info", "requeued", f"batch {seq} requeued", detail={"seq": seq}
                )
            return self._send(200, {"ok": changed, "requeued": changed})
        if path.startswith("/api/batches/") and path.endswith("/discard"):
            seq = self._seq_from_path(path)
            if seq is None:
                return self._error(400, "bad_seq", "batch seq must be an integer")
            row = self.state.meta.get_batch_by_seq(seq)
            if row is None or row["state"] != "quarantine":
                return self._error(409, "not_quarantined", "only quarantined batches can be discarded")
            self.state.storage.spool_unlink(row["spool_path"])
            with self.state.meta.tx() as conn:
                conn.execute("DELETE FROM batches WHERE seq=? AND state='quarantine'", (seq,))
            self.state.meta.add_event(
                "warn", "discarded", f"quarantined batch {seq} discarded", detail={"seq": seq}
            )
            return self._send(200, {"ok": True})
        if path == "/api/control/worker":
            body = self._json_body()
            action = body.get("action", "status")
            if action == "start":
                result = self.state.start_worker()
            elif action in ("stop", "kill"):
                result = self.state.stop_worker(hard=action == "kill")
                if action == "kill":
                    self.state.worker_proc = None
            elif action == "status":
                result = {"ok": True, **self.state.worker_info()}
            else:
                return self._error(422, "bad_action", f"unknown action {action!r}")
            return self._send(200, result)
        if path == "/api/control/integrity":
            result = self.state.run_integrity()
            return self._send(200 if result.get("ok") else 409, result)
        if path == "/api/control/inject-bad":
            body = self._json_body()
            return self._inject_bad(body)
        return self._error(404, "not_found", f"no route for {path}")

    def _seq_from_path(self, path: str) -> int | None:
        """Extract the seq from /api/batches/<seq>/<action>."""
        parts = [p for p in path.split("/") if p]
        try:
            return int(parts[2])
        except (IndexError, ValueError):
            return None

    # -- static -------------------------------------------------------------
    def _serve_static(self, name: str) -> None:
        file = Path(__file__).parent / "static" / name
        try:
            payload = file.read_bytes()
        except FileNotFoundError:
            return self._error(404, "not_found", "static asset missing")
        types = {
            ".html": "text/html; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
            ".css": "text/css; charset=utf-8",
        }
        return self._send(200, payload, content_type=types.get(file.suffix, "text/plain"))

    # -- credit / status ----------------------------------------------------
    def _reservations(self) -> tuple[int, int]:
        totals = self.state.meta.reservation_totals()
        return totals["bytes"], totals["batches"]

    def _credit_for(self, source: dict[str, Any]):
        settings = self.state.meta.get_settings()
        live = self.state.meta.live_spool()
        usage = self.state.meta.source_live_usage(source["source_id"])
        reservations_bytes, reservations_batches = self._reservations()
        free, _total = disk_usage(self.state.layout.data_dir)
        oldest = live["oldest_live_epoch"]
        return compute_credit(
            source=source,
            live_spool_bytes=live["live_bytes"],
            live_spool_batches=live["live_batches"],
            reservations_bytes=reservations_bytes,
            reservations_batches=reservations_batches,
            source_bytes=usage["bytes"],
            source_batches=usage["batches"],
            settings=settings,
            disk_free=free,
            oldest_live_age=(time.time() - oldest) if oldest else None,
        )

    def _status(self) -> dict[str, Any]:
        meta = self.state.meta
        settings = meta.get_settings()
        sources = meta.list_sources()
        live = meta.live_spool()
        totals = meta.state_totals()
        storage = meta.storage_totals()
        disk = self.state.storage.disk()
        db = meta.db_stats()
        samples = meta.list_samples(120)
        credits = [self._credit_for(s).to_wire() for s in sources]
        spool_used = live["live_bytes"]
        cap = int(settings["spool_cap_bytes"])
        free_floor = int(settings["min_free_bytes"])
        disk_pressure = disk["free_bytes"] <= free_floor
        oldest = live["oldest_live_epoch"]
        lag_secs = (time.time() - oldest) if oldest else 0.0
        lag_reduced = lag_secs >= float(settings["backlog_reduce_secs"])
        percent = 0.0 if cap <= 0 else min(100.0, 100.0 * spool_used / cap)
        return {
            "ok": True,
            "now": epoch_now(),
            "uptime_secs": time.time() - self.state.started_epoch,
            "worker": self.state.worker_info(),
            "settings": settings,
            "sources": [
                {
                    "source_id": s["source_id"],
                    "label": s["label"],
                    "active": s["active"],
                    "reserved_bytes": s["reserved_bytes"],
                    "reserved_batches": s["reserved_batches"],
                    "max_batch_bytes": s["max_batch_bytes"],
                    "states": s["states"],
                    "indexed_hot_batches": s["indexed_hot_batches"],
                    "indexed_cold_batches": s["indexed_cold_batches"],
                    "credit": c,
                }
                for s, c in zip(sources, credits)
            ],
            "spool": {
                "cap_bytes": cap,
                "soft_limit_bytes": int(cap * float(settings["soft_limit_pct"])),
                "used_bytes": spool_used,
                "percent": round(percent, 2),
                "live_batches": live["live_batches"],
                "quarantine_batches": live["quarantine_batches"],
                "quarantine_bytes": live["quarantine_bytes"],
                "max_inflight_batches": settings["max_inflight_batches"],
                "pressure": (
                    "disk_free_floor"
                    if disk_pressure
                    else "hard_cap"
                    if spool_used >= cap
                    else "soft_limit"
                    if spool_used >= cap * float(settings["soft_limit_pct"])
                    or lag_reduced
                    else "normal"
                ),
                "lag_reduced": lag_reduced,
            },
            "totals": {
                "states": totals,
                "indexed_batches": totals.get("indexed", {}).get("count", 0),
                "indexed_records": meta.record_index_count(),
                "storage": storage,
            },
            "disk": {**disk, "min_free_bytes": free_floor},
            "db": db,
            "backlog": {
                "pending": meta.pending_batches(30),
                "recent": meta.recent_batches(20),
                "oldest_live_epoch": live["oldest_live_epoch"],
            },
            "events": meta.list_events(40),
            "samples": samples,
            "latest_sample": samples[-1] if samples else None,
            "integrity": {
                "report": self.state.integrity_report,
                "running": self.state.integrity_running,
            },
            "counters": dict(self.state.counters),
        }

    # -- ingest -------------------------------------------------------------
    def _ingest(self, path: str) -> None:
        parts = [p for p in path.split("/") if p]
        path_source = parts[1] if len(parts) >= 2 else None
        if path_source is not None and not SOURCE_ID_RE.match(path_source):
            return self._error(422, "invalid_source_id", "malformed source in path")
        meta = self.state.meta
        settings = meta.get_settings()
        query = parse_qs(urlparse(self.path).query)
        body = self._read_body(int(settings["max_batch_bytes"]))
        header_source = self.headers.get("X-Source-Id")
        header_batch = self.headers.get("X-Batch-Id") or self.headers.get(
            "Idempotency-Key"
        )
        request_id = self.headers.get("X-Request-Id") or f"req-{uuid.uuid4().hex}"
        source_id_hint = header_source or path_source
        if source_id_hint is None:
            return self._error(422, "missing_source", "source_id is required")
        source = meta.get_source(source_id_hint)
        if source is None:
            return self._error(404, "unknown_source", f"unknown source {source_id_hint}")
        if not source["active"]:
            return self._throttle(
                source,
                "source_paused",
                "this source is paused by the operator",
                retry_after=5,
                status=423,
            )
        try:
            batch = parse_batch(
                body,
                header_source=header_source,
                path_source=path_source,
                header_batch=header_batch,
                path_batch=None,
                max_batch_bytes=int(min(settings["max_batch_bytes"], source["max_batch_bytes"])),
                max_batch_records=int(settings["max_batch_records"]),
                max_record_bytes=int(min(settings["max_record_bytes"], source["max_record_bytes"])),
                allowed_source=path_source if header_source is None else path_source,
            )
        except Reject as exc:
            self.state.counters["rejected"] += 1
            return self._send(exc.status, exc.body())
        if path_source is not None and batch.source_id != path_source:
            return self._error(
                409, "source_mismatch", "body source_id does not match the path"
            )
        digest = sha256_hex(body)
        existing = meta.get_batch(batch.source_id, batch.batch_id)
        if existing is not None:
            return self._duplicate_reply(existing, digest, request_id)
        if batch.invalid:
            if not source["active"]:
                return self._throttle(
                    source,
                    "source_paused",
                    "this source is paused by the operator",
                    retry_after=5,
                    status=423,
                )
            return self._quarantine_ingest(batch, settings, request_id)

        credit = self._credit_for(source)
        if credit.granted_batches < 1 or credit.granted_bytes < len(body):
            self.state.counters["throttled"] += 1
            meta.add_event(
                "warn",
                "throttled",
                f"source {batch.source_id} throttled: {credit.reason}",
                source_id=batch.source_id,
                batch_id=batch.batch_id,
                detail={
                    "credit_bytes": credit.granted_bytes,
                    "credit_batches": credit.granted_batches,
                    "batch_bytes": len(body),
                    "reason": credit.reason,
                },
            )
            return self._throttle(
                source,
                credit.reason,
                "receiver will not accept new work right now",
                retry_after=refusal_retry_after(credit),
                credit=credit.to_wire(),
            )

        spool_rel = self.state.storage.spool_batch(batch.source_id, batch.batch_id, body)
        test_crash("after-spool-write")
        try:
            row, outcome = meta.admit_live_batch(
                source_id=batch.source_id,
                batch_id=batch.batch_id,
                digest=digest,
                records_digest=records_digest(batch.records),
                record_count=batch.record_count,
                payload_bytes=len(body),
                spool_path=spool_rel,
                max_inflight=int(settings["max_inflight_batches"]),
                spool_cap_bytes=int(settings["spool_cap_bytes"]),
                reserved_batches=int(source["reserved_batches"]),
                reserved_bytes=int(source["reserved_bytes"]),
            )
        except BaseException:
            self.state.storage.spool_unlink(spool_rel)
            raise
        if outcome == "created":
            self.state.counters["accepted"] += 1
            reply = self._accepted_body(row, source, request_id)
            wait = self._wants_indexed(body, query)
            if wait:
                row = self._wait_for_terminal(row["seq"], float(settings["ack_wait_secs"]))
                reply = self._accepted_body(row, source, request_id)
                reply["waited"] = True
                return self._send(
                    HTTPStatus.OK,
                    reply,
                    headers=self._ack_headers(row),
                )
            return self._send(HTTPStatus.ACCEPTED, reply, headers=self._ack_headers(row))
        self.state.storage.spool_unlink(spool_rel)
        if outcome in ("duplicate", "conflict") and row is not None:
            return self._duplicate_reply(row, digest, request_id)
        if outcome in ("limit_global", "limit_source"):
            self.state.counters["throttled"] += 1
            reason = (
                "spool_hard_cap" if outcome == "limit_global" else "source_reservation_full"
            )
            return self._throttle(
                source,
                reason,
                "reservation was consumed concurrently; retry with the batch id",
                retry_after=2,
            )
        return self._error(503, "admission_failed", "batch could not be admitted")

    def _wants_indexed(self, body: bytes, query: dict[str, list[str]]) -> bool:
        if query.get("wait", [""])[0] in ("indexed", "1", "true"):
            return True
        header = self.headers.get("X-Wait-Indexed", "")
        return header.lower() in ("1", "true", "yes", "indexed")

    def _wait_for_terminal(self, seq: int, timeout: float) -> dict[str, Any]:
        deadline = time.time() + max(0.5, timeout)
        row = self.state.meta.get_batch_by_seq(seq) or {}
        while time.time() < deadline:
            row = self.state.meta.get_batch_by_seq(seq) or row
            if row.get("state") in ("indexed", "quarantine"):
                return row
            time.sleep(0.05)
        return row

    def _ack_headers(self, row: dict[str, Any]) -> dict[str, str]:
        headers = {
            "X-Ack-Level": str(row.get("ack_level") or "spool-durable"),
            "X-Batch-State": str(row.get("state")),
        }
        if row.get("state") == "staged":
            headers["Location"] = f"/api/batches?source={row.get('source_id')}"
        return headers

    def _accepted_body(
        self, row: dict[str, Any], source: dict[str, Any], request_id: str
    ) -> dict[str, Any]:
        state = row.get("state")
        level = {
            "indexed": "indexed",
            "quarantine": "quarantined",
        }.get(state, row.get("ack_level") or "spool-durable")
        body = {
            "ok": True,
            "request_id": request_id,
            "duplicate": False,
            "source_id": row["source_id"],
            "batch_id": row["batch_id"],
            "batch_seq": row["seq"],
            "state": state,
            "ack": {
                "level": level,
                "durable": level in ("spool-durable", "indexed"),
                "meaning": {
                    "spool-durable": "raw bytes fsynced to the spool and a metadata row committed; indexing is pending",
                    "indexed": "hot segment fsynced and record-index rows committed atomically; queryable",
                    "quarantined": "bytes stored in quarantine and the outcome recorded; this batch will not be indexed",
                }[level],
            },
            "credit": self._credit_for(source).to_wire(),
            "received_at": row.get("received_at"),
        }
        if state == "quarantine":
            body["ok"] = False
            body["error"] = {
                "code": row.get("error_kind") or "quarantined",
                "message": row.get("error") or "batch quarantined",
            }
        return body

    def _duplicate_reply(
        self, row: dict[str, Any], digest: str, request_id: str
    ) -> None:
        if row["content_sha256"] != digest:
            self.state.counters["conflict"] += 1
            return self._error(
                409,
                "batch_conflict",
                "batch_id was already used with different content; use a new batch_id",
                batch_seq=row["seq"],
            )
        self.state.counters["duplicate"] += 1
        self.state.meta.add_event(
            "info",
            "duplicate",
            f"duplicate batch {row['source_id']}/{row['batch_id']} reused",
            source_id=row["source_id"],
            batch_id=row["batch_id"],
            detail={"seq": row["seq"], "state": row["state"]},
        )
        source = self.state.meta.get_source(row["source_id"]) or {
            "source_id": row["source_id"],
            "active": 1,
            "max_batch_bytes": 4096,
            "reserved_bytes": 4096,
            "reserved_batches": 1,
            "max_record_bytes": 4096,
            "states": {},
        }
        body = self._accepted_body(row, source, request_id)
        body["duplicate"] = True
        body["ack"]["meaning"] = (
            "this batch_id already had this exact content; the existing result is reused"
        )
        status = (
            HTTPStatus.CONFLICT
            if body["state"] == "quarantine"
            else HTTPStatus.OK
        )
        return self._send(status, body, headers=self._ack_headers(row))

    def _quarantine_ingest(
        self, batch: Any, settings: dict[str, Any], request_id: str
    ) -> None:
        first = batch.invalid[0]["problem"]
        error = f"{len(batch.invalid)} invalid record(s); first: {first}"
        spool_rel = self.state.storage.spool_batch(
            batch.source_id, batch.batch_id, batch.content
        )
        try:
            row, outcome = self.state.meta.admit_quarantine_batch(
                source_id=batch.source_id,
                batch_id=batch.batch_id,
                digest=batch.content_sha256,
                records_digest=records_digest(batch.records),
                record_count=batch.record_count,
                payload_bytes=len(batch.content),
                spool_path=spool_rel,
                error=error,
                error_kind="invalid_record",
                max_batches=int(settings["quarantine_max_batches"]),
                max_bytes=int(settings["quarantine_max_bytes"]),
            )
        except BaseException:
            self.state.storage.spool_unlink(spool_rel)
            raise
        if outcome != "created" and row is not None:
            self.state.storage.spool_unlink(spool_rel)
            return self._duplicate_reply(row, batch.content_sha256, request_id)
        if outcome == "limit_quarantine":
            self.state.storage.spool_unlink(spool_rel)
            return self._error(
                429,
                "quarantine_full",
                "quarantine budget is exhausted; an operator must requeue or discard held batches",
                Retry_After="10",
            )
        self.state.counters["quarantined_ingest"] += 1
        self.state.meta.add_event(
            "error",
            "batch_quarantined",
            f"batch {batch.source_id}/{batch.batch_id} quarantined at ingest: {error}",
            source_id=batch.source_id,
            batch_id=batch.batch_id,
            detail={"invalid": batch.invalid[:10]},
        )
        source = self.state.meta.get_source(batch.source_id)
        body = self._accepted_body(row, source, request_id)
        body["invalid_records"] = batch.invalid[:10]
        return self._send(422, body, headers=self._ack_headers(row))

    def _throttle(
        self,
        source: dict[str, Any],
        reason: str,
        message: str,
        *,
        retry_after: int,
        credit: dict[str, Any] | None = None,
        status: int = 429,
    ) -> None:
        body = {
            "ok": False,
            "error": {
                "code": reason,
                "message": message,
                "source_id": source["source_id"],
                "retry_after": retry_after,
            },
            "credit": credit or self._credit_for(source).to_wire(),
        }
        self._send(status, body, headers={"Retry-After": str(retry_after)})

    def _inject_bad(self, body: dict[str, Any]) -> None:
        """Operator action: send a deliberately bad batch through /ingest.

        This reuses the real HTTP path in-process so the dashboard cannot
        pretend a bad batch was handled when it was not.
        """
        source_id = str(body.get("source_id") or "alpha")
        batch_id = str(body.get("batch_id") or f"bad-{uuid.uuid4().hex[:8]}")
        if not SOURCE_ID_RE.match(source_id):
            return self._error(422, "invalid_source_id", "bad source id")
        if not BATCH_ID_RE.match(batch_id):
            return self._error(422, "invalid_batch_id", "bad batch id")
        mode = str(body.get("mode") or "invalid-record")
        records: list[Any]
        if mode == "bad-json":
            raw = b'{"format":"pointwise-receiver/v1","records":['
            return self._ingest_payload(source_id, batch_id, raw, expect_status=422)
        if mode == "too-many":
            records = [{"id": f"d-{i}", "value": i} for i in range(100_000)]
        elif mode == "huge-value":
            records = [{"id": "huge", "value": "x" * 200_000}]
        elif mode == "duplicate-ids":
            records = [{"id": "same", "value": 1}, {"id": "same", "value": 2}]
        else:
            records = [
                {"id": "good-1", "value": "ok"},
                {"id": "broken", "value": {"nested": True}},
                {"value": "missing id"},
            ]
        payload = json.dumps(
            {
                "format": "pointwise-receiver/v1",
                "source_id": source_id,
                "batch_id": batch_id,
                "records": records,
            }
        ).encode()
        return self._ingest_payload(source_id, batch_id, payload)

    def _ingest_payload(
        self, source_id: str, batch_id: str, payload: bytes, *, expect_status: int = 422
    ) -> None:
        """Re-enter the real validation/admission path in-process.

        Injection exists so the dashboard cannot fabricate a success: whatever
        this returns is what a real sender with the same bytes would get.
        """
        settings = self.state.meta.get_settings()
        try:
            batch = parse_batch(
                payload,
                header_source=source_id,
                path_source=source_id,
                header_batch=batch_id,
                path_batch=None,
                max_batch_bytes=int(settings["max_batch_bytes"]),
                max_batch_records=int(settings["max_batch_records"]),
                max_record_bytes=int(settings["max_record_bytes"]),
                allowed_source=source_id,
            )
        except Reject as exc:
            self.state.counters["rejected"] += 1
            self.state.meta.add_event(
                "warn",
                "injected_bad",
                f"operator injected batch rejected at framing: {batch_id}",
                source_id=source_id,
                batch_id=batch_id,
                detail=exc.body(),
            )
            return self._send(
                exc.status,
                {"ok": False, "injected": True, "error": exc.body()["error"]},
            )
        request_id = f"inject-{uuid.uuid4().hex}"
        if batch.invalid:
            return self._quarantine_ingest(batch, settings, request_id)
        source = self.state.meta.get_source(source_id)
        spool_rel = self.state.storage.spool_batch(source_id, batch_id, payload)
        row, outcome = self.state.meta.admit_live_batch(
            source_id=source_id,
            batch_id=batch_id,
            digest=batch.content_sha256,
            records_digest=records_digest(batch.records),
            record_count=batch.record_count,
            payload_bytes=len(payload),
            spool_path=spool_rel,
            max_inflight=int(settings["max_inflight_batches"]),
            spool_cap_bytes=int(settings["spool_cap_bytes"]),
            reserved_batches=int(source["reserved_batches"]),
            reserved_bytes=int(source["reserved_bytes"]),
        )
        if outcome != "created":
            self.state.storage.spool_unlink(spool_rel)
        body = {
            "ok": outcome == "created",
            "injected": True,
            "outcome": outcome,
            "batch_seq": row["seq"] if row else None,
            "state": row["state"] if row else None,
        }
        if outcome == "conflict":
            return self._send(409, {**body, "error": {"code": "batch_conflict", "message": "batch id reused with different content"}})
        return self._send(202 if outcome == "created" else 409, body)


def make_server(
    data_dir: Path, host: str = "127.0.0.1", port: int = 8787
) -> ThreadingHTTPServer:
    layout = Layout(data_dir).ensure()
    meta = Meta(layout)
    meta.initialize()
    state = ReceiverState(layout)
    handler = type("BoundHandler", (Handler,), {"state": state})
    httpd = ThreadingHTTPServer((host, port), handler)
    httpd.daemon_threads = True
    httpd.allow_reuse_address = True
    return httpd


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="pointwise receiver API")
    parser.add_argument("--data-dir", default=str(ROOT / "data"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument(
        "--with-worker", action="store_true", help="also supervise an index worker"
    )
    args = parser.parse_args(argv)
    httpd = make_server(Path(args.data_dir), args.host, args.port)
    state: ReceiverState = httpd.RequestHandlerClass.state  # type: ignore[attr-defined]
    if args.with_worker:
        state.start_worker()
    print(
        f"receiver listening on http://{args.host}:{args.port} "
        f"(data {args.data_dir})",
        flush=True,
    )
    try:
        httpd.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        if args.with_worker:
            state.stop_worker(hard=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
