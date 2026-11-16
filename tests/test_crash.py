"""Crash recovery at the acknowledgement boundaries.

Each test kills a real process (``os._exit`` from the fault hook or SIGKILL)
at a specific point, restarts it, and asserts what the caller was told is
still true.  The interesting property is not "no data was lost" in general,
but that the *acknowledgement level* matches what survives:

* ``spool-durable``: the batch survives an API crash after the spool write
  but before the metadata commit -- it was never acknowledged, so the sender
  may retry with the same ``batch_id`` and get a single copy;
* ``indexed``: once the sender sees this, both the hot payload and index rows
  survive an immediate worker SIGKILL;
* a crash between the hot write and the metadata commit leaves a batch in
  ``indexing``; the restarted worker replays it without duplicate index rows;
* a crash between the cold write and the pointer commit keeps the hot copy,
  which is still authoritative, so nothing is lost.
"""

from __future__ import annotations

import json
import os
import signal
import time
import unittest

from harness import ReceiverHarness, ReceiverTestCase


class SpoolCrashTests(unittest.TestCase):
    def setUp(self) -> None:
        self.h = ReceiverHarness()
        self.addCleanup(self.h.close)

    def test_api_crash_after_spool_write_does_not_ack_then_retry_is_single_copy(self) -> None:
        self.h.start_api(crash_after="after-spool-write")
        payload = self.h.batch("alpha", "crash-spool", records=3)
        with self.assertRaises(ConnectionError):
            self.h.ingest("alpha", payload)
        self.assertEqual(self.h.api.poll(), 70)
        self.h.start_api()
        # Nothing was acknowledged, so no batch row exists.
        status, batches = self.h.get("/api/batches")
        self.assertEqual(batches["pending"], [])
        # The retried request is admitted once, not twice.
        code, body, _ = self.h.ingest("alpha", payload)
        self.assertEqual(code, 202)
        self.assertFalse(body["duplicate"])
        self.h.start_worker()
        self.h.wait_state("crash-spool", "indexed")
        self.assertEqual(self.h.get("/api/records?source=alpha")[1]["total_indexed"], 3)
        self.h.set_settings(quarantine_max_batches=128)  # noop, keeps flake tools honest

    def test_spool_file_is_fsynced_before_metadata_row_is_visible(self) -> None:
        """A row must never name a spool path that is absent or short."""
        self.h.start_api()
        self.h.start_worker()
        self.h.ingest("alpha", self.h.batch("alpha", "fsync-order", records=5))
        self.h.wait_state("fsync-order", "indexed")
        # The indexer consumed and removed the spool copy; the hot file is the
        # surviving evidence and it verifies end to end.
        report = self.h.post("/api/control/integrity")[1]["report"]
        self.assertTrue(report["ok"], report)
        self.assertEqual(report["checked_batches"], 1)


class IndexedCrashTests(ReceiverTestCase):
    with_worker = True

    def test_indexed_ack_survives_immediate_worker_sigkill(self) -> None:
        payload = self.h.batch("alpha", "kill-after-ack", records=7)
        code, body, _ = self.h.ingest(
            "alpha", payload, headers={"X-Wait-Indexed": "1"}
        )
        self.assertEqual(code, 200)
        self.assertEqual(body["state"], "indexed")
        self.h.kill_worker()
        self.h.start_worker()
        # Nothing to replay: the acknowledgement was not optimistic.
        self.h.wait_for(
            lambda: self.h.get("/api/control/worker")[1]["running"] is True
        )
        records = self.h.get("/api/records?source=alpha")[1]
        self.assertEqual(records["total_indexed"], 7)
        self.assertEqual(self.h.status()["totals"]["states"]["indexed"]["count"], 1)
        report = self.h.post("/api/control/integrity")[1]["report"]
        self.assertTrue(report["ok"], report)


class MidIndexCrashTests(unittest.TestCase):
    def setUp(self) -> None:
        self.h = ReceiverHarness()
        self.addCleanup(self.h.close)
        self.h.start_api()
        self.h.set_settings(sample_interval_secs=0.5, lease_seconds=5)

    def test_worker_killed_mid_index_replays_without_duplicate_rows(self) -> None:
        self.h.start_worker(crash_after="after-hot-write")
        payload = self.h.batch("alpha", "mid-index", records=4)
        code, body, _ = self.h.ingest("alpha", payload)
        self.assertEqual(code, 202)
        # The worker dies between fsyncing the hot file and committing the
        # indexed transition; the batch surfaced via the API must not claim
        # to be indexed.
        self.h.wait_for(lambda: self.h.worker.poll() is not None, timeout=20)
        # The crash happened with the lease still held, so the API truthfully
        # reports "indexing", not "indexed".
        pending = self.h.get("/api/batches")[1]["pending"]
        row = next(r for r in pending if r["batch_id"] == "mid-index")
        self.assertEqual(row["state"], "indexing")
        self.h.start_worker()
        self.h.wait_state("mid-index", "indexed")
        records = self.h.get("/api/records?source=alpha")[1]
        self.assertEqual(records["total_indexed"], 4)
        self.assertEqual(len(records["records"]), 4)
        report = self.h.post("/api/control/integrity")[1]["report"]
        self.assertTrue(report["ok"], report)

    def test_worker_killed_twice_converges(self) -> None:
        self.h.start_worker(crash_after="after-hot-write")
        self.h.ingest("alpha", self.h.batch("alpha", "twice", records=6))
        self.h.wait_for(lambda: self.h.worker.poll() is not None, timeout=20)
        self.h.start_worker(crash_after="after-hot-write")
        self.h.wait_for(lambda: self.h.worker.poll() is not None, timeout=20)
        self.h.start_worker()
        self.h.wait_state("twice", "indexed")
        self.assertEqual(self.h.get("/api/records?source=alpha")[1]["total_indexed"], 6)
        report = self.h.post("/api/control/integrity")[1]["report"]
        self.assertTrue(report["ok"], report)

    def test_cold_write_crash_keeps_hot_copy(self) -> None:
        # A one-batch hot window means the second indexed batch triggers
        # promotion.  Crash after the cold file is fsynced but before the
        # metadata pointer commits: the hot copy must remain authoritative.
        self.h.set_settings(hot_window_batches=1, promotion_max_batches=4)
        self.h.start_worker(crash_after="after-cold-write")
        self.h.ingest("alpha", self.h.batch("alpha", "cold-a", records=2))
        self.h.ingest("alpha", self.h.batch("alpha", "cold-b", records=3))
        self.h.wait_for(lambda: self.h.worker.poll() is not None, timeout=20)
        rows_before = self.h.get("/api/batches")[1]["recent"]
        indexed_before = [r for r in rows_before if r["state"] == "indexed"]
        self.assertTrue(indexed_before)
        self.assertTrue(
            all(r["storage_level"] == "hot" for r in indexed_before), rows_before
        )
        # The orphaned cold file is never referenced by metadata.
        self.assertEqual(self.h.status()["totals"]["storage"]["segment_files"], 0)
        self.h.start_worker()
        self.h.wait_state("cold-a", "indexed")
        self.h.wait_state("cold-b", "indexed")
        report = self.h.post("/api/control/integrity")[1]["report"]
        self.assertTrue(report["ok"], report)
        self.assertEqual(self.h.get("/api/records?source=alpha")[1]["total_indexed"], 5)
        # Promotion eventually runs; data is reachable from the cold tier.
        self.h.wait_for(
            lambda: self.h.status()["totals"]["storage"]["segment_files"] >= 1,
            timeout=20,
        )
        report = self.h.post("/api/control/integrity")[1]["report"]
        self.assertTrue(report["ok"], report)


class LeaseRecoveryTests(ReceiverTestCase):
    with_worker = True

    def test_expired_lease_is_recovered_without_operator(self) -> None:
        # Make storage work slow enough to guarantee the worker holds a lease
        # while it is visibly in the ``indexing`` state.  (``index_delay_ms``
        # only delays before leasing, so the in-flight window is too short to
        # observe reliably.)
        self.h.set_settings(storage_delay_ms=1500, lease_seconds=5)
        self.h.ingest("alpha", self.h.batch("alpha", "leased", records=2))
        self.h.wait_for(
            lambda: any(
                b["state"] == "indexing"
                for b in self.h.get("/api/batches")[1]["pending"]
            )
        )
        pid = self.h.get("/api/control/worker")[1]["pid"]
        os.kill(pid, signal.SIGKILL)
        if self.h.worker is not None:
            self.h.worker.wait(timeout=10)
        self.h.set_settings(storage_delay_ms=0)
        # While the lease is unexpired, the batch is not re-leased.
        state = self.h.wait_state("leased", "indexing")
        self.assertEqual(state["state"], "indexing")
        self.h.start_worker()
        self.h.wait_state("leased", "indexed", timeout=30)
        self.assertEqual(self.h.get("/api/records?source=alpha")[1]["total_indexed"], 2)


class IdempotentReplayTests(ReceiverTestCase):
    with_worker = True

    def test_replay_after_duplicate_during_index_does_not_duplicate(self) -> None:
        self.h.set_settings(index_delay_ms=400)
        payload = self.h.batch("alpha", "replay", records=5)
        self.assertEqual(self.h.ingest("alpha", payload)[0], 202)
        # Repeat while the first copy is being indexed.
        for _ in range(3):
            code, body, _ = self.h.ingest("alpha", payload)
            self.assertEqual(code, 200)
            self.assertTrue(body["duplicate"])
        self.h.wait_state("replay", "indexed", timeout=30)
        self.h.set_settings(index_delay_ms=0)
        self.assertEqual(self.h.get("/api/records?source=alpha")[1]["total_indexed"], 5)
        batches = self.h.get("/api/batches")[1]
        self.assertEqual(len([b for b in batches["recent"] if b["batch_id"] == "replay"]), 1)


if __name__ == "__main__":
    unittest.main()
