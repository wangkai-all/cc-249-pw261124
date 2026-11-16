"""Shared harness: real API subprocess, real worker subprocess, real HTTP.

Tests deliberately avoid calling internal functions where process boundaries
matter.  A crash test that imports the worker and raises an exception proves
nothing about fsync ordering; these tests send HTTP, SIGKILL the worker, and
restart it.
"""

from __future__ import annotations

import http.client
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class ReceiverHarness:
    def __init__(self, data_dir: Path | None = None):
        self._tmp = None
        if data_dir is None:
            self._tmp = tempfile.TemporaryDirectory(prefix="pointwise-test-")
            data_dir = Path(self._tmp.name) / "data"
        self.data_dir = Path(data_dir)
        self.port = free_port()
        self.base = f"http://127.0.0.1:{self.port}"
        self.api: subprocess.Popen[bytes] | None = None
        self.worker: subprocess.Popen[bytes] | None = None
        import uuid

        log_dir = Path(tempfile.gettempdir()) / "pointwise-logs"
        log_dir.mkdir(exist_ok=True)
        tag = uuid.uuid4().hex[:8]
        self.api_log = log_dir / f"api-{tag}.log"
        self.worker_log = log_dir / f"worker-{tag}.log"

    # -- lifecycle ----------------------------------------------------------
    def start_api(self, *, crash_after: str | None = None) -> None:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(ROOT)
        if crash_after:
            env["POINTWISE_CRASH_AFTER"] = crash_after
        else:
            env.pop("POINTWISE_CRASH_AFTER", None)
        self.api_log.parent.mkdir(parents=True, exist_ok=True)
        log = open(self.api_log, "ab")
        self.api = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "receiver.server",
                "--data-dir",
                str(self.data_dir),
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
            ],
            cwd=str(ROOT),
            env=env,
            stdout=log,
            stderr=log,
        )
        log.close()
        deadline = time.time() + 15
        while time.time() < deadline:
            try:
                if self.get("/healthz", timeout=0.5)[0] == 200:
                    return
            except (ConnectionError, urllib.error.URLError, OSError):
                time.sleep(0.05)
        raise RuntimeError(f"API did not start:\n{_tail(self.api_log)}")

    def start_worker(self, *, crash_after: str | None = None) -> None:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(ROOT)
        if crash_after:
            env["POINTWISE_CRASH_AFTER"] = crash_after
        else:
            env.pop("POINTWISE_CRASH_AFTER", None)
        log = open(self.worker_log, "ab")
        self.worker = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "receiver.worker",
                "--data-dir",
                str(self.data_dir),
                "--poll",
                "0.05",
            ],
            cwd=str(ROOT),
            env=env,
            stdout=log,
            stderr=log,
        )
        log.close()
        deadline = time.time() + 15
        while time.time() < deadline:
            _, body = self.get("/api/control/worker")
            if body.get("running") or (self.worker is not None and self.worker.poll() is not None):
                if body.get("running"):
                    return
                raise RuntimeError(f"worker exited:\n{_tail(self.worker_log)}")
            time.sleep(0.05)
        raise RuntimeError(f"worker did not start:\n{_tail(self.worker_log)}")

    def kill_worker(self, sig: int = signal.SIGKILL) -> int:
        status, body = self.get("/api/control/worker")
        pid = body.get("pid")
        if not pid:
            raise RuntimeError("worker not running")
        os.kill(pid, sig)
        if self.worker is not None:
            try:
                self.worker.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.worker.kill()
        return pid

    def kill_api(self) -> None:
        if self.api is not None and self.api.poll() is None:
            self.api.kill()
            self.api.wait(timeout=10)

    def stop(self) -> None:
        if self.api is not None and self.api.poll() is None:
            try:
                status, body = self.get("/api/control/worker", timeout=1)
                if body.get("pid"):
                    os.kill(body["pid"], signal.SIGKILL)
            except Exception:
                pass
            self.api.terminate()
            try:
                self.api.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.api.kill()
        if self.worker is not None and self.worker.poll() is None:
            self.worker.kill()
            self.worker.wait(timeout=10)

    def close(self) -> None:
        self.stop()
        if self._tmp is not None:
            self._tmp.cleanup()

    # -- HTTP ---------------------------------------------------------------
    def get(self, path: str, *, timeout: float = 10.0) -> tuple[int, dict]:
        status, body, _headers = self._request("GET", path, None, timeout)
        return status, body

    def get_with_headers(self, path: str, *, timeout: float = 10.0) -> tuple[int, dict, dict]:
        return self._request("GET", path, None, timeout)

    def post(
        self, path: str, body: dict | None = None, *, timeout: float = 20.0
    ) -> tuple[int, dict, dict]:
        return self._request("POST", path, json.dumps(body or {}).encode(), timeout)

    def ingest(
        self,
        source: str,
        batch: dict,
        *,
        headers: dict[str, str] | None = None,
        timeout: float = 20.0,
    ) -> tuple[int, dict, dict]:
        payload = json.dumps(batch).encode()
        return self._request("POST", f"/ingest/{source}", payload, timeout, headers)

    def _request(
        self,
        method: str,
        path: str,
        body: bytes | None,
        timeout: float,
        headers: dict[str, str] | None = None,
    ) -> tuple:
        request = urllib.request.Request(
            self.base + path, data=body, method=method, headers=headers or {}
        )
        if body is not None:
            request.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                status = response.status
                raw = response.read()
                result = json.loads(raw) if raw else {}
                return status, result, dict(response.headers)
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                result = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                result = {"raw": raw.decode("utf-8", "replace")}
            return exc.code, result, dict(exc.headers)
        except (ConnectionError, http.client.HTTPException, OSError):
            raise

    # -- convenience --------------------------------------------------------
    def batch(
        self, source: str, batch_id: str, records: int = 5, value_bytes: int = 16
    ) -> dict:
        return {
            "format": "pointwise-receiver/v1",
            "source_id": source,
            "batch_id": batch_id,
            "records": [
                {
                    "id": f"{batch_id}-{n}",
                    "value": "x" * value_bytes + f":{n}",
                }
                for n in range(records)
            ],
        }

    def status(self) -> dict:
        return self.get("/api/status")[1]

    def wait_for(self, predicate, *, timeout: float = 20.0, interval: float = 0.05):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            last = predicate()
            if last:
                return last
            time.sleep(interval)
        raise AssertionError(f"condition not met within {timeout}s; last={last!r}")

    def wait_state(self, batch_id: str, state: str, *, source: str | None = None, timeout: float = 25.0) -> dict:
        def check():
            status, batches = self.get("/api/batches")
            for row in batches["pending"] + batches["recent"]:
                if row["batch_id"] == batch_id and row["state"] == state:
                    if source is None or row["source_id"] == source:
                        return row
            return None

        return self.wait_for(check, timeout=timeout)

    def set_settings(self, **values) -> None:
        status, body, _ = self.post("/api/settings", values)
        if status != 200:
            raise AssertionError(f"settings failed: {status} {body}")


def _tail(path: Path, lines: int = 25) -> str:
    try:
        return "\n".join(path.read_text(errors="replace").splitlines()[-lines:])
    except FileNotFoundError:
        return "<no log>"


class ReceiverTestCase(unittest.TestCase):
    """Base class: fresh data dir + API per test, opt-in worker."""

    with_worker = False

    def setUp(self) -> None:
        self.h = ReceiverHarness()
        self.addCleanup(self.h.close)
        self.h.start_api()
        self.h.set_settings(sample_interval_secs=0.5, lease_seconds=5)
        if self.with_worker:
            self.h.start_worker()

    def assertIngest(self, status: int, body: dict, expected: int) -> None:
        self.assertEqual(status, expected, msg=json.dumps(body, indent=2)[:2000])
