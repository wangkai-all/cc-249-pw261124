"""Backpressure: slow indexer, bounded spool, no cross-source starvation.

The scenario is the one the design is for: two local producers push at the
receiver while the index worker is slowed or stopped.  Assertions are about
what a sender observes (429/Retry-After, reduced credit), what the disk does
(bounded spool, no runaway), and fairness between sources.
"""

from __future__ import annotations

import json
import threading
import time
import unittest

from harness import ReceiverTestCase


class SlowIndexerTests(ReceiverTestCase):
    with_worker = True

    def test_slow_index_shrinks_credit_and_then_recovers(self) -> None:
        self.h.post("/api/sources/alpha", {"reserved_batches": 2})
        self.h.post("/api/sources/beta", {"reserved_batches": 2})
        self.h.set_settings(
            spool_cap_bytes=262144,
            max_inflight_batches=4,
            index_delay_ms=1500,
            sample_interval_secs=0.5,
        )
        codes = []
        for n in range(6):
            code, body, _ = self.h.ingest("alpha", self.h.batch("alpha", f"slow-{n}", 1))
            codes.append(code)
        self.assertIn(429, codes)
        status = self.h.status()
        self.assertGreater(status["spool"]["used_bytes"], 0)
        self.assertLessEqual(status["spool"]["used_bytes"], status["spool"]["cap_bytes"])
        credit = self.h.get("/api/credit/alpha")[1]
        self.assertLessEqual(credit["credit_batches"], 2)
        # After the indexer catches up, credit returns without an operator.
        self.h.set_settings(index_delay_ms=0)
        self.h.wait_for(
            lambda: self.h.get("/api/credit/alpha")[1]["credit_batches"] > 0,
            timeout=40,
        )


class BoundedSpoolTests(ReceiverTestCase):
    with_worker = False

    def test_stopped_worker_never_lets_spool_exceed_cap(self) -> None:
        self.h.set_settings(spool_cap_bytes=65536, max_inflight_batches=64)
        self.h.post("/api/sources/alpha", {"reserved_batches": 100, "reserved_bytes": 1_000_000})
        self.h.post("/api/sources/beta", {"reserved_batches": 100, "reserved_bytes": 1_000_000})
        refused = 0
        for n in range(25):
            code, _, headers = self.h.ingest("alpha", self.h.batch("alpha", f"cap-{n}", 3, 1000))
            if code == 429:
                refused += 1
                self.assertIn("Retry-After", headers)
        status = self.h.status()
        self.assertGreater(refused, 0)
        self.assertLessEqual(status["spool"]["used_bytes"], status["spool"]["cap_bytes"])
        self.assertLessEqual(status["spool"]["live_batches"], status["spool"]["max_inflight_batches"])
        # Refusals write no spool files.
        spool_bytes = sum(p.stat().st_size for p in (self.h.data_dir / "spool").glob("*"))
        self.assertLessEqual(spool_bytes, 65536)

    def test_duplicate_is_answered_even_when_spool_is_full(self) -> None:
        """A retry of an accepted batch must not be punished by backpressure.

        Senders retry on ambiguous timeouts.  If the receiver refused the
        retry because the spool filled up, the sender could never learn that
        its original write succeeded -- so duplicate lookup happens before
        the credit gate and returns the existing result.
        """
        self.h.set_settings(spool_cap_bytes=65536, max_inflight_batches=2)
        payload = self.h.batch("alpha", "already-in", records=5, value_bytes=8000)
        self.assertEqual(self.h.ingest("alpha", payload)[0], 202)
        code, body, _ = self.h.ingest("alpha", payload)
        self.assertEqual(code, 200)
        self.assertTrue(body["duplicate"])
        # A *new* batch id is refused while the spool is full.
        other = self.h.batch("alpha", "brand-new", records=5, value_bytes=8000)
        code, body, _ = self.h.ingest("alpha", other)
        self.assertEqual(code, 429)
        self.h.start_worker()
        # "Some credit" is not enough: both the batch and byte allowances must
        # cover this payload before a new batch can be admitted.
        need_bytes = len(json.dumps(other).encode()) + 1024
        self.h.wait_for(
            lambda: (lambda c: c["credit_batches"] >= 1 and c["credit_bytes"] >= need_bytes)(
                self.h.get("/api/credit/alpha")[1]
            ),
            timeout=20,
        )
        code, body, _ = self.h.ingest("alpha", other)
        self.assertIn(code, (200, 202))
        self.h.wait_state("brand-new", "indexed")


class FairnessTests(ReceiverTestCase):
    with_worker = False

    def test_one_source_backlog_cannot_starve_another(self) -> None:
        self.h.post("/api/sources/alpha", {"reserved_batches": 3, "reserved_bytes": 65536})
        self.h.post("/api/sources/beta", {"reserved_batches": 3, "reserved_bytes": 65536})
        self.h.set_settings(spool_cap_bytes=262144, max_inflight_batches=8)
        alpha_codes = [
            self.h.ingest("alpha", self.h.batch("alpha", f"flood-{n}", 1))[0]
            for n in range(8)
        ]
        self.assertEqual(alpha_codes[:3], [202, 202, 202])
        self.assertIn(429, alpha_codes)
        # beta is unaffected by alpha's exhausted reservation.
        beta_codes = [
            self.h.ingest("beta", self.h.batch("beta", f"b-{n}", 1))[0] for n in range(4)
        ]
        self.assertEqual(beta_codes[:3], [202, 202, 202])
        self.assertEqual(beta_codes[3], 429)
        # Neither source's files exceed its reservation's batch count.
        spool = self.h.data_dir / "spool"
        alpha_files = list(spool.glob("alpha__*"))
        beta_files = list(spool.glob("beta__*"))
        self.assertEqual(len(alpha_files), 3)
        self.assertEqual(len(beta_files), 3)

    def test_concurrent_sources_share_slack_and_stay_within_cap(self) -> None:
        self.h.set_settings(spool_cap_bytes=131072, max_inflight_batches=10)
        self.h.post("/api/sources/alpha", {"reserved_batches": 10, "reserved_bytes": 200_000})
        self.h.post("/api/sources/beta", {"reserved_batches": 10, "reserved_bytes": 200_000})
        results: dict[str, list[int]] = {"alpha": [], "beta": []}
        lock = threading.Lock()

        def flood(source: str) -> None:
            for n in range(30):
                code, _, _ = self.h.ingest(
                    source, self.h.batch(source, f"{source}-{n}", 2, 300)
                )
                with lock:
                    results[source].append(code)
                if code == 429:
                    time.sleep(0.01)

        threads = [threading.Thread(target=flood, args=(s,)) for s in ("alpha", "beta")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)
        status = self.h.status()
        self.assertLessEqual(status["spool"]["used_bytes"], 131072)
        self.assertLessEqual(status["spool"]["live_batches"], 10)
        self.assertGreater(len(results["alpha"]), 0)
        self.assertGreater(len(results["beta"]), 0)
        # Both sources got at least some work in: no total starvation.
        self.assertIn(202, results["alpha"])
        self.assertIn(202, results["beta"])

    def test_worker_never_relies_on_refusing_its_own_queue(self) -> None:
        # A full spool must not deadlock the indexer: indexing is what drains
        # the spool, so the worker must keep taking staged batches.
        self.h.set_settings(spool_cap_bytes=65536, max_inflight_batches=32)
        self.h.post("/api/sources/alpha", {"reserved_batches": 100, "reserved_bytes": 1_000_000})
        accepted = 0
        for n in range(30):
            code, _, _ = self.h.ingest("alpha", self.h.batch("alpha", f"drain-{n}", 1))
            if code == 202:
                accepted += 1
        self.assertGreater(accepted, 0)
        self.h.start_worker()
        self.h.wait_for(
            lambda: self.h.status()["spool"]["live_batches"] == 0, timeout=30
        )
        counts = self.h.status()["totals"]["states"]
        self.assertEqual(counts["indexed"]["count"], accepted)


class QuarantineBudgetTests(ReceiverTestCase):
    with_worker = True

    def test_quarantine_budget_is_bounded_and_reports_full(self) -> None:
        self.h.set_settings(quarantine_max_batches=2)
        codes = []
        for n in range(4):
            bad = {
                "format": "pointwise-receiver/v1",
                "source_id": "alpha",
                "batch_id": f"bad-budget-{n}",
                "records": [{"id": "x", "value": [1, 2, 3]}],
            }
            codes.append(self.h.ingest("alpha", bad)[0])
        self.assertEqual(codes[:2], [422, 422])
        self.assertEqual(codes[2], 429)
        self.assertEqual(codes[3], 429)
        status = self.h.status()
        self.assertLessEqual(status["spool"]["quarantine_batches"], 2)

    def test_quarantine_does_not_consume_sender_credit(self) -> None:
        self.h.post("/api/sources/alpha", {"reserved_batches": 2})
        self.h.set_settings(quarantine_max_batches=4)
        for n in range(2):
            bad = {
                "format": "pointwise-receiver/v1",
                "source_id": "alpha",
                "batch_id": f"held-{n}",
                "records": [{"id": "x", "value": {"bad": True}}],
            }
            self.assertEqual(self.h.ingest("alpha", bad)[0], 422)
        # Held batches are not live work: alpha can still send valid batches.
        code, body, _ = self.h.ingest("alpha", self.h.batch("alpha", "valid-after-bad", 1))
        self.assertEqual(code, 202, body)


if __name__ == "__main__":
    unittest.main()
