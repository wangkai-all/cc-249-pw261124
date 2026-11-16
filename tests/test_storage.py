"""Tiered storage: hot → cold promotion, compaction, and integrity.

These tests exercise the storage path through the worker's real maintenance
loop and verify payloads by reading them back, not by checking counters alone.
"""

from __future__ import annotations

import gzip
import json
import time
import unittest

from harness import ReceiverHarness, ReceiverTestCase


class PromotionTests(ReceiverTestCase):
    with_worker = True

    def test_hot_batches_are_promoted_to_cold_and_still_readable(self) -> None:
        self.h.set_settings(
            hot_window_batches=2,
            promotion_max_batches=16,
            sample_interval_secs=0.5,
        )
        ids = [f"promote-{n}" for n in range(6)]
        for batch_id in ids:
            code, _, _ = self.h.ingest("alpha", self.h.batch("alpha", batch_id, records=3))
            self.assertEqual(code, 202)
        for batch_id in ids:
            self.h.wait_state(batch_id, "indexed")

        def promoted():
            # hot_window_batches=2 means the two newest stay hot and the
            # older four move to cold.
            recent = self.h.get("/api/batches")[1]["recent"]
            levels = [r["storage_level"] for r in recent if r["batch_id"] in ids]
            return levels.count("cold") >= 4

        self.h.wait_for(promoted, timeout=25)
        status = self.h.status()
        self.assertGreaterEqual(status["totals"]["storage"]["cold_batches"], 4)
        self.assertLessEqual(status["totals"]["storage"]["hot_batches"], 2)
        self.assertGreaterEqual(status["totals"]["storage"]["segment_files"], 1)
        # Hot files for promoted batches are gone; cold files exist and verify.
        hot_files = list((self.h.data_dir / "hot").rglob("*.json"))
        self.assertEqual(len(hot_files), status["totals"]["storage"]["hot_batches"])
        cold_files = list((self.h.data_dir / "cold").glob("*.json.gz"))
        self.assertEqual(len(cold_files), status["totals"]["storage"]["segment_files"])
        for path in cold_files:
            doc = json.loads(gzip.decompress(path.read_bytes()).decode())
            self.assertEqual(doc["format"], "pointwise-receiver/cold-segment-v1")
            for batch in doc["batches"]:
                self.assertGreaterEqual(len(batch["records"]), 1)
        report = self.h.post("/api/control/integrity")[1]["report"]
        self.assertTrue(report["ok"], report)
        self.assertEqual(report["record_index_count"], 18)

    def test_compaction_bounds_segment_count(self) -> None:
        self.h.set_settings(
            hot_window_batches=1,
            promotion_max_batches=1,
            cold_compact_threshold=3,
            compaction_max_segments=8,
            compact_after_secs=0,
            sample_interval_secs=0.5,
        )
        for n in range(7):
            self.h.ingest("alpha", self.h.batch("alpha", f"compact-{n}", records=2))
        for n in range(7):
            self.h.wait_state(f"compact-{n}", "indexed")

        def settled():
            storage = self.h.status()["totals"]["storage"]
            return storage["cold_batches"] >= 6 and storage["segment_files"] <= 2

        self.h.wait_for(settled, timeout=60)
        status = self.h.status()
        self.assertGreaterEqual(status["totals"]["storage"]["cold_batches"], 6)
        # Every record is still indexed exactly once after the merge.
        self.assertEqual(status["totals"]["indexed_records"], 14)
        report = self.h.post("/api/control/integrity")[1]["report"]
        self.assertTrue(report["ok"], report)

    def test_promotion_keeps_one_copy_per_batch(self) -> None:
        self.h.set_settings(hot_window_batches=1, promotion_max_batches=8)
        self.h.ingest("alpha", self.h.batch("alpha", "older", records=4))
        self.h.wait_state("older", "indexed")
        self.h.ingest("alpha", self.h.batch("alpha", "newer", records=2))
        self.h.wait_state("newer", "indexed")

        def cold():
            return self.status_level("older") == "cold"

        self.h.wait_for(cold, timeout=20)
        recent = self.h.get("/api/batches")[1]["recent"]
        older = next(r for r in recent if r["batch_id"] == "older")
        newer = next(r for r in recent if r["batch_id"] == "newer")
        self.assertEqual(older["storage_level"], "cold")
        self.assertEqual(newer["storage_level"], "hot")
        self.assertEqual(self.h.get("/api/records?source=alpha")[1]["total_indexed"], 6)
        self.assertFalse(list((self.h.data_dir / "hot").rglob("older.json")))
        self.assertTrue(list((self.h.data_dir / "hot").rglob("newer.json")))

    def status_level(self, batch_id: str) -> str | None:
        recent = self.h.get("/api/batches")[1]["recent"]
        for row in recent:
            if row["batch_id"] == batch_id:
                return row["storage_level"]
        return None


class IntegrityDetectionTests(ReceiverTestCase):
    with_worker = True

    def test_integrity_detects_tampered_hot_payload(self) -> None:
        self.h.ingest("alpha", self.h.batch("alpha", "tamper", records=3))
        self.h.wait_state("tamper", "indexed")
        self.assertTrue(self.h.post("/api/control/integrity")[1]["report"]["ok"])
        path = next((self.h.data_dir / "hot").rglob("tamper.json"))
        doc = json.loads(path.read_text())
        doc["records"][0]["value"] = "tampered"
        path.write_text(json.dumps(doc))
        report = self.h.post("/api/control/integrity")[1]["report"]
        self.assertFalse(report["ok"])
        kinds = {issue["kind"] for issue in report["issues"]}
        self.assertIn("records_digest_mismatch", kinds)

    def test_integrity_detects_deleted_payload(self) -> None:
        self.h.ingest("alpha", self.h.batch("alpha", "gone", records=2))
        self.h.wait_state("gone", "indexed")
        path = next((self.h.data_dir / "hot").rglob("gone.json"))
        path.unlink()
        report = self.h.post("/api/control/integrity")[1]["report"]
        self.assertFalse(report["ok"])
        self.assertIn("missing_file", {i["kind"] for i in report["issues"]})

    def test_integrity_detects_index_count_drift(self) -> None:
        self.h.ingest("alpha", self.h.batch("alpha", "drift", records=3))
        self.h.wait_state("drift", "indexed")
        import sqlite3

        conn = sqlite3.connect(self.h.data_dir / "meta.db")
        try:
            conn.execute("DELETE FROM record_index WHERE doc_id='drift-0'")
            conn.commit()
        finally:
            conn.close()
        report = self.h.post("/api/control/integrity")[1]["report"]
        self.assertFalse(report["ok"])
        self.assertIn("index_count_mismatch", {i["kind"] for i in report["issues"]})


class SpoolRetentionTests(ReceiverTestCase):
    with_worker = False

    def test_spool_files_are_removed_after_indexing(self) -> None:
        self.h.set_settings(sample_interval_secs=0.5)
        self.h.ingest("alpha", self.h.batch("alpha", "spool-cleared", records=2))
        spool = list((self.h.data_dir / "spool").glob("*.json"))
        self.assertEqual(len(spool), 1)
        self.h.start_worker()
        self.h.wait_state("spool-cleared", "indexed")
        self.h.wait_for(
            lambda: list((self.h.data_dir / "spool").glob("*.json")) == [], timeout=10
        )
        self.assertEqual(list((self.h.data_dir / "tmp").glob("*")), [])


if __name__ == "__main__":
    unittest.main()
