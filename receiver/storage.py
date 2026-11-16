"""On-disk formats and fsynced writes for spool, hot batches and cold tiers.

Spool files keep the exact received request body so a retried byte-identical
batch can be detected and answered from the existing batch rather than being
duplicated.  Hot and cold files are canonical record documents: a header with
batch metadata and a ``records`` array in receive order.

All writes go through ``durable_write``: write to ``tmp/``, fsync the file,
``os.replace`` into place, fsync the directory.  Callers commit metadata only
after this returns, which is the invariant that makes an acknowledgement
truthful.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any, Iterable

from .config import Layout, disk_usage, durable_write, fsync_dir, sha256_hex


class StorageError(Exception):
    def __init__(self, message: str, *, kind: str = "storage_error"):
        super().__init__(message)
        self.kind = kind


class Storage:
    def __init__(self, layout: Layout):
        self.layout = layout

    # -- spool -------------------------------------------------------------
    def spool_batch(self, source_id: str, batch_id: str, body: bytes) -> str:
        """Fsync raw bytes to the spool; return the relative path.

        The path is unique per *request*, not per batch id.  Two concurrent
        requests for the same batch id would otherwise race: the loser could
        unlink the winner's spool file when it rolls back.  The winner is
        decided by the database, and the losing path is always safe to delete.
        """
        name = f"{source_id}__{batch_id}__{uuid.uuid4().hex[:12]}.json"
        path = self.layout.spool / name
        durable_write(path, body, self.layout.tmp)
        return self.layout.relpath(path)

    def spool_path(self, relative: str) -> Path:
        return self.layout.abs_path(relative)

    def spool_read(self, relative: str) -> bytes:
        return self.layout.abs_path(relative).read_bytes()

    def spool_unlink(self, relative: str) -> bool:
        try:
            path = self.layout.abs_path(relative)
        except ValueError:
            return False
        try:
            path.unlink()
            return True
        except FileNotFoundError:
            return False

    # -- hot tier ----------------------------------------------------------
    def hot_relpath(self, source_id: str, batch_id: str) -> str:
        return str(Path("hot") / source_id / f"{batch_id}.json")

    def write_hot(
        self,
        source_id: str,
        batch_id: str,
        *,
        seq: int,
        digest: str,
        records: list[dict[str, Any]],
    ) -> tuple[str, str]:
        """Write one batch's canonical hot file.  Returns (relpath, sha256)."""
        doc = {
            "format": "pointwise-receiver/hot-v1",
            "source_id": source_id,
            "batch_id": batch_id,
            "batch_seq": seq,
            "content_sha256": digest,
            "records": records,
        }
        payload = json.dumps(doc, ensure_ascii=False, separators=(",", ":")).encode()
        rel = self.hot_relpath(source_id, batch_id)
        path = self.layout.abs_path(rel)
        durable_write(path, payload, self.layout.tmp)
        return rel, sha256_hex(payload)

    def remove_hot(self, relative: str) -> None:
        try:
            path = self.layout.abs_path(relative)
        except ValueError:
            return
        try:
            path.unlink()
        except FileNotFoundError:
            return
        parent = path.parent
        try:
            parent.rmdir()
        except OSError:
            pass
        fsync_dir(path.parent.parent if path.parent != self.layout.hot else self.layout.hot)

    # -- cold tier ---------------------------------------------------------
    def write_cold_segment(self, batches: Iterable[dict[str, Any]]) -> dict[str, Any]:
        """Pack batch documents into one compressed cold segment."""
        docs = []
        total_records = 0
        sources: set[str] = set()
        for batch in batches:
            records = self.read_hot(batch["hot_path"])
            docs.append(
                {
                    "source_id": batch["source_id"],
                    "batch_id": batch["batch_id"],
                    "batch_seq": batch["seq"],
                    "content_sha256": batch["content_sha256"],
                    "records": records,
                }
            )
            total_records += len(records)
            sources.add(batch["source_id"])
        if not docs:
            raise StorageError("refusing to write an empty cold segment")
        doc = {
            "format": "pointwise-receiver/cold-segment-v1",
            "created_epoch": __import__("time").time(),
            "batches": docs,
        }
        raw = json.dumps(doc, ensure_ascii=False, separators=(",", ":")).encode()
        compressed = gzip.compress(raw, compresslevel=6, mtime=0)
        rel = str(Path("cold") / f"seg-{uuid.uuid4().hex[:16]}.json.gz")
        path = self.layout.abs_path(rel)
        durable_write(path, compressed, self.layout.tmp)
        return {
            "relpath": rel,
            "sha256": sha256_hex(compressed),
            "records": total_records,
            "bytes": len(compressed),
            "source_count": len(sources),
            "batch_seqs": [d["batch_seq"] for d in docs],
        }

    def write_merged_cold(
        self, docs: Iterable[dict[str, Any]], *, name_hint: str = "merge"
    ) -> dict[str, Any]:
        """Write a cold segment from already-loaded canonical batch docs."""
        batches = list(docs)
        if not batches:
            raise StorageError("refusing to write an empty compacted segment")
        doc = {
            "format": "pointwise-receiver/cold-segment-v1",
            "created_epoch": __import__("time").time(),
            "batches": batches,
        }
        raw = json.dumps(doc, ensure_ascii=False, separators=(",", ":")).encode()
        compressed = gzip.compress(raw, compresslevel=6, mtime=0)
        rel = str(Path("cold") / f"seg-{uuid.uuid4().hex[:16]}.json.gz")
        durable_write(self.layout.abs_path(rel), compressed, self.layout.tmp)
        return {
            "relpath": rel,
            "sha256": sha256_hex(compressed),
            "records": sum(len(d["records"]) for d in batches),
            "bytes": len(compressed),
            "source_count": len({d["source_id"] for d in batches}),
            "batch_seqs": [d["batch_seq"] for d in batches],
            "name_hint": name_hint,
        }

    def read_hot(self, relative: str) -> list[dict[str, Any]]:
        path = self.layout.abs_path(relative)
        try:
            doc = json.loads(path.read_bytes().decode("utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise StorageError(
                f"hot payload {relative} is unreadable: {exc}", kind="corrupt_payload"
            ) from exc
        if not isinstance(doc, dict) or not isinstance(doc.get("records"), list):
            raise StorageError(
                f"hot payload {relative} has no records array", kind="corrupt_payload"
            )
        return doc["records"]

    def read_cold_batches(self, relative: str) -> list[dict[str, Any]]:
        path = self.layout.abs_path(relative)
        try:
            raw = gzip.decompress(path.read_bytes())
            doc = json.loads(raw.decode("utf-8"))
        except (OSError, OSError, json.JSONDecodeError, UnicodeDecodeError, EOFError) as exc:
            raise StorageError(
                f"cold segment {relative} is unreadable: {exc}", kind="corrupt_payload"
            ) from exc
        if not isinstance(doc, dict) or not isinstance(doc.get("batches"), list):
            raise StorageError(
                f"cold segment {relative} is malformed", kind="corrupt_payload"
            )
        return doc["batches"]

    def remove_cold(self, relative: str) -> None:
        try:
            path = self.layout.abs_path(relative)
        except ValueError:
            return
        try:
            path.unlink()
        except FileNotFoundError:
            return

    def file_sha256(self, relative: str) -> str:
        path = self.layout.abs_path(relative)
        digest = hashlib.sha256()
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    # -- disk --------------------------------------------------------------
    def disk_free(self) -> int:
        free, _total = disk_usage(self.layout.data_dir)
        return free

    def disk(self) -> dict[str, int]:
        free, total = disk_usage(self.layout.data_dir)
        return {"free_bytes": free, "total_bytes": total}

    # -- recovery ----------------------------------------------------------
    def cleanup_tmp(self, *, min_age_secs: float = 120.0) -> int:
        """Remove abandoned partial writes, but never a live one.

        The API writes ``tmp/*.part`` files while the worker may be starting
        up.  Deleting those would make ``os.replace`` fail with ENOENT and
        turn an ordinary ingest into a 500, so only files older than
        ``min_age_secs`` are treated as abandoned.
        """
        removed = 0
        cutoff = time.time() - min_age_secs
        try:
            entries = list(self.layout.tmp.iterdir())
        except FileNotFoundError:
            return 0
        for entry in entries:
            try:
                if not entry.is_file():
                    continue
                if entry.stat().st_mtime > cutoff:
                    continue
                entry.unlink()
                removed += 1
            except FileNotFoundError:
                continue
        return removed

    def spool_files(self) -> set[str]:
        found: set[str] = set()
        for root, _dirs, files in os.walk(self.layout.spool):
            for name in files:
                found.add(name)
        return found

    def hot_files(self) -> set[str]:
        found: set[str] = set()
        for root, _dirs, files in os.walk(self.layout.hot):
            for name in files:
                rel = Path(os.path.relpath(os.path.join(root, name), self.layout.data_dir))
                found.add(str(rel))
        return found

    def cold_files(self) -> set[str]:
        found: set[str] = set()
        for root, _dirs, files in os.walk(self.layout.cold):
            for name in files:
                rel = Path(os.path.relpath(os.path.join(root, name), self.layout.data_dir))
                found.add(str(rel))
        return found
