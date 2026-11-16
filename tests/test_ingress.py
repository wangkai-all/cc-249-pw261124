"""Ingress contract tests: validation, acknowledgement levels, idempotency."""

from __future__ import annotations

import json
import time
import unittest

from harness import ReceiverTestCase


class IngressTests(ReceiverTestCase):
    with_worker = True

    def test_ack_is_spool_durable_and_names_its_level(self) -> None:
        payload = self.h.batch("alpha", "batch-ack", records=3)
        status, body, headers = self.h.ingest("alpha", payload)
        self.assertIngest(status, body, 202)
        self.assertEqual(body["ack"]["level"], "spool-durable")
        self.assertEqual(headers["X-Ack-Level"], "spool-durable")
        self.assertEqual(body["state"], "staged")
        # The batch is acknowledged at spool level, not index level: the
        # worker may not have touched it yet.
        self.assertEqual(body["ack"]["level"], "spool-durable")
        self.assertNotEqual(body["state"], "indexed")

    def test_wait_indexed_returns_indexed_ack(self) -> None:
        payload = self.h.batch("alpha", "batch-wait", records=2)
        status, body, headers = self.h.ingest(
            "alpha", payload, headers={"X-Wait-Indexed": "1"}
        )
        self.assertIngest(status, body, 200)
        self.assertEqual(body["state"], "indexed")
        self.assertEqual(body["ack"]["level"], "indexed")
        self.assertTrue(body["ack"]["durable"])
        self.assertEqual(headers["X-Ack-Level"], "indexed")
        self.assertIn("queryable", body["ack"]["meaning"])

    def test_duplicate_batch_reuses_existing_result(self) -> None:
        payload = self.h.batch("alpha", "batch-dup", records=4)
        first_status, first, _ = self.h.ingest("alpha", payload)
        self.assertIngest(first_status, first, 202)
        second_status, second, _ = self.h.ingest("alpha", payload)
        self.assertIngest(second_status, second, 200)
        self.assertTrue(second["duplicate"])
        self.assertEqual(second["batch_seq"], first["batch_seq"])
        self.h.wait_state("batch-dup", "indexed")
        index = self.h.get("/api/records?source=alpha")[1]
        self.assertEqual(index["total_indexed"], 4)
        states = self.h.status()["totals"]["states"]
        self.assertEqual(states.get("indexed", {}).get("count"), 1)

    def test_duplicate_during_live_indexing_is_not_reindexed(self) -> None:
        self.h.set_settings(index_delay_ms=250)
        payload = self.h.batch("alpha", "batch-race", records=2)
        status, first, _ = self.h.ingest("alpha", payload)
        self.assertIngest(status, first, 202)
        second_status, second, _ = self.h.ingest("alpha", payload)
        self.assertIngest(second_status, second, 200)
        self.assertTrue(second["duplicate"])
        self.h.wait_state("batch-race", "indexed")
        self.assertEqual(self.h.get("/api/records?source=alpha")[1]["total_indexed"], 2)
        self.h.set_settings(index_delay_ms=0)

    def test_conflicting_reuse_is_rejected_and_original_kept(self) -> None:
        payload = self.h.batch("alpha", "batch-conflict", records=2)
        status, _, _ = self.h.ingest("alpha", payload)
        self.assertEqual(status, 202)
        changed = dict(payload)
        changed["records"] = [{"id": "other", "value": 99}]
        status, body, _ = self.h.ingest("alpha", changed)
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "batch_conflict")
        self.h.wait_state("batch-conflict", "indexed")
        records = self.h.get("/api/records?source=alpha")[1]["records"]
        self.assertEqual({r["doc_id"] for r in records}, {"batch-conflict-0", "batch-conflict-1"})

    def test_malformed_framing_is_rejected(self) -> None:
        cases = [
            ({"source_id": "alpha", "batch_id": "x", "records": "nope"}, "invalid_records"),
            ({"source_id": "alpha", "batch_id": "x", "records": [], "record_count": 5}, "record_count_mismatch"),
            ({"source_id": "bad source", "batch_id": "x", "records": []}, "invalid_source_id"),
            ({"source_id": "alpha", "batch_id": "x,y", "records": []}, "invalid_batch_id"),
        ]
        for payload, code in cases:
            status, body, _ = self.h.ingest("alpha", payload)
            self.assertEqual(status, 422, msg=payload)
            self.assertEqual(body["error"]["code"], code, msg=payload)

    def test_non_json_body_is_rejected(self) -> None:
        request_status, body, _ = self.h._request(
            "POST", "/ingest/alpha", b'{"records": [', 10
        )
        self.assertEqual(request_status, 422)
        self.assertEqual(body["error"]["code"], "invalid_json")

    def test_oversized_declared_digest_is_rejected(self) -> None:
        payload = self.h.batch("alpha", "batch-hash", records=1)
        payload["content_sha256"] = "0" * 64
        status, body, _ = self.h.ingest("alpha", payload)
        self.assertEqual(status, 422)
        self.assertEqual(body["error"]["code"], "content_sha256_mismatch")

    def test_producer_canonical_digest_is_accepted(self) -> None:
        """A document cannot hash itself: the declared digest covers the body
        without the ``content_sha256`` field, in canonical form."""
        import hashlib

        payload = {
            "format": "pointwise-receiver/v1",
            "source_id": "alpha",
            "batch_id": "canon-digest",
            "records": [{"id": "canon-0", "value": "hello"}],
        }
        canonical = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
        payload["content_sha256"] = hashlib.sha256(canonical).hexdigest()
        status, body, _ = self.h.ingest("alpha", payload)
        self.assertEqual(status, 202, body)
        # Tampering with the records invalidates the declared digest.
        payload["records"][0]["value"] = "changed"
        payload["batch_id"] = "canon-digest-2"
        status, body, _ = self.h.ingest("alpha", payload)
        self.assertEqual(status, 422)
        self.assertEqual(body["error"]["code"], "content_sha256_mismatch")

    def test_invalid_record_quarantines_without_blocking_other_source(self) -> None:
        bad = {
            "format": "pointwise-receiver/v1",
            "source_id": "alpha",
            "batch_id": "bad-1",
            "records": [
                {"id": "ok", "value": 1},
                {"id": "bad", "value": {"nested": True}},
            ],
        }
        status, body, _ = self.h.ingest("alpha", bad)
        self.assertEqual(status, 422)
        self.assertEqual(body["state"], "quarantine")
        self.assertEqual(body["error"]["code"], "invalid_record")
        self.assertFalse(body["ack"]["durable"])
        # beta continues to index while alpha's bad batch sits in quarantine.
        ok = self.h.batch("beta", "beta-good", records=2)
        status, _, _ = self.h.ingest("beta", ok)
        self.assertEqual(status, 202)
        self.h.wait_state("beta-good", "indexed")
        rows = self.h.get("/api/records?source=beta")[1]
        self.assertEqual(rows["total_indexed"], 2)

    def test_unknown_source_and_paused_source(self) -> None:
        payload = self.h.batch("gamma", "g-1", records=1)
        status, body, _ = self.h.ingest("gamma", payload)
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["code"], "unknown_source")
        self.h.post("/api/sources/alpha", {"active": False})
        status, body, _ = self.h.ingest("alpha", self.h.batch("alpha", "p-1", 1))
        self.assertEqual(status, 423)
        self.assertEqual(body["error"]["code"], "source_paused")
        self.h.post("/api/sources/alpha", {"active": True})
        status, _, _ = self.h.ingest("alpha", self.h.batch("alpha", "p-2", 1))
        self.assertEqual(status, 202)

    def test_requeue_of_still_bad_batch_reports_reality(self) -> None:
        bad = {
            "format": "pointwise-receiver/v1",
            "source_id": "alpha",
            "batch_id": "requeue-me",
            "records": [{"id": "only", "value": [1, 2]}],
        }
        status, body, _ = self.h.ingest("alpha", bad)
        self.assertEqual(status, 422)
        self.assertEqual(body["state"], "quarantine")
        seq = body["batch_seq"]
        status, body, _ = self.h.post(f"/api/batches/{seq}/requeue", {})
        self.assertEqual(status, 200)
        self.assertTrue(body["requeued"])

        def quarantined_twice():
            events = self.h.get("/api/events?limit=200")[1]["events"]
            hits = [
                e
                for e in events
                if e["kind"] == "batch_quarantined"
                and e.get("batch_id") == "requeue-me"
            ]
            return len(hits) >= 2

        self.h.wait_for(quarantined_twice)
        rows = self.h.get("/api/batches")[1]["pending"]
        held = [r for r in rows if r["batch_id"] == "requeue-me"]
        self.assertEqual(len(held), 1)
        self.assertEqual(held[0]["state"], "quarantine")
        self.assertIn("invalid record", (held[0]["error"] or ""))



class CreditTests(ReceiverTestCase):
    with_worker = False

    def _ingest_until_throttled(self, source: str, limit: int = 200) -> tuple[int, dict]:
        accepted = 0
        last = {}
        for n in range(limit):
            status, body, _ = self.h.ingest(source, self.h.batch(source, f"{source}-{n}", 1))
            if status in (200, 202):
                accepted += 1
            else:
                last = body
                break
        return accepted, last

    def test_source_credit_stops_at_reservation(self) -> None:
        self.h.post(
            "/api/sources/alpha",
            {"reserved_batches": 3, "reserved_bytes": 64 * 1024},
        )
        self.h.set_settings(spool_cap_bytes=262144, max_inflight_batches=8)
        accepted, body = self._ingest_until_throttled("alpha")
        self.assertEqual(accepted, 3)
        self.assertIn(body["error"]["code"], {"reservation_exhausted", "spool_hard_cap"})
        self.assertEqual(int(body["credit"]["credit_batches"]), 0)

    def test_sources_cannot_consume_each_others_reservation(self) -> None:
        self.h.post("/api/sources/alpha", {"reserved_batches": 2, "reserved_bytes": 65536})
        self.h.post("/api/sources/beta", {"reserved_batches": 2, "reserved_bytes": 65536})
        self.h.set_settings(spool_cap_bytes=262144, max_inflight_batches=4)
        alpha_accepted, _ = self._ingest_until_throttled("alpha", limit=5)
        self.assertEqual(alpha_accepted, 2)
        # beta's own reservation is intact even though alpha is full.
        beta_accepted, _ = self._ingest_until_throttled("beta", limit=5)
        self.assertEqual(beta_accepted, 2)
        status = self.h.status()
        self.assertEqual(status["spool"]["live_batches"], 4)

    def test_soft_waterline_reduces_credit_to_reservation(self) -> None:
        self.h.post("/api/sources/alpha", {"reserved_batches": 4, "reserved_bytes": 65536})
        self.h.set_settings(
            spool_cap_bytes=131072,
            max_inflight_batches=8,
            soft_limit_pct=0.1,
        )
        self.h.ingest("alpha", self.h.batch("alpha", "w-1", 1, value_bytes=20_000))
        status = self.h.status()
        self.assertEqual(status["spool"]["pressure"], "soft_limit")
        credit = self.h.get("/api/credit/alpha")[1]
        self.assertEqual(credit["reason"], "soft_limit_reserved_only")
        # Reservation still grants room; borrowed slack does not.
        self.assertLessEqual(credit["credit_bytes"], 65536)
        self.assertLessEqual(credit["credit_batches"], 4)

    def test_hard_cap_returns_retry_after_and_writes_nothing(self) -> None:
        self.h.post("/api/sources/alpha", {"reserved_batches": 50, "reserved_bytes": 1_000_000})
        self.h.set_settings(spool_cap_bytes=65536, max_inflight_batches=50)
        big = self.h.batch("alpha", "big-1", records=100, value_bytes=400)
        status, body, headers = self.h.ingest("alpha", big)
        self.assertIn(status, (202, 429))
        # Fill it up until refusal.
        accepted = 0
        for n in range(40):
            status, body, headers = self.h.ingest("alpha", self.h.batch("alpha", f"fill-{n}", 20))
            if status == 429:
                break
            accepted += 1
        self.assertGreater(accepted, 0)
        self.assertEqual(status, 429)
        self.assertIn("Retry-After", headers)
        self.assertIn(
            body["error"]["code"],
            {
                "hard_cap",
                "reservation_exhausted",
                "fragment_too_small",
                "soft_limit_reserved_only",
            },
        )
        live = self.h.status()["spool"]
        self.assertLessEqual(live["used_bytes"], live["cap_bytes"])

    def test_credit_changes_when_worker_drains(self) -> None:
        self.h.post("/api/sources/alpha", {"reserved_batches": 3, "reserved_bytes": 65536})
        self.h.set_settings(spool_cap_bytes=262144, max_inflight_batches=4)
        for n in range(3):
            status, _, _ = self.h.ingest("alpha", self.h.batch("alpha", f"d-{n}", 1))
            self.assertEqual(status, 202)
        self.assertEqual(self.h.get("/api/credit/alpha")[1]["credit_batches"], 0)
        self.h.start_worker()
        self.h.wait_for(lambda: self.h.get("/api/credit/alpha")[1]["credit_batches"] > 0)

        def drained():
            return self.h.status()["spool"]["live_batches"] == 0

        self.h.wait_for(drained)
        self.assertEqual(self.h.status()["totals"]["states"]["indexed"]["count"], 3)

    def test_index_lag_withdraws_borrowed_credit_before_cap(self) -> None:
        """A slow indexer reduces credit while the spool is still small.

        The soft waterline is deliberately far away (90% of a 1 MiB cap); the
        limit that bites is backlog age, so the sender observes reduced credit
        well before the hard cap is anywhere near.
        """
        import time as _time

        self.h.post(
            "/api/sources/alpha",
            {"reserved_batches": 2, "reserved_bytes": 65536},
        )
        self.h.set_settings(
            spool_cap_bytes=1_048_576,
            max_inflight_batches=200,
            soft_limit_pct=0.9,
            backlog_reduce_secs=1,
        )
        # One live batch is enough to set the lag clock; no worker drains it.
        self.assertEqual(
            self.h.ingest("alpha", self.h.batch("alpha", "lag-1", 1))[0], 202
        )
        _time.sleep(1.2)
        credit = self.h.get("/api/credit/alpha")[1]
        self.assertEqual(credit["reason"], "index_lag_reduce")
        self.assertLessEqual(credit["credit_batches"], 2)
        self.assertLessEqual(credit["credit_bytes"], 65536)
        # The reserved room is still usable: the reduction is not a 429 wall.
        self.assertEqual(
            self.h.ingest("alpha", self.h.batch("alpha", "lag-2", 1))[0], 202
        )
        # A worker that catches up restores borrowing without a restart.
        self.h.start_worker()
        self.h.wait_for(
            lambda: self.h.get("/api/credit/alpha")[1]["reason"] == "ok", timeout=25
        )


if __name__ == "__main__":
    unittest.main()
