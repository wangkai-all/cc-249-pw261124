"""Metadata schema and the only code allowed to mutate receiver state.

Every transition is a short ``BEGIN IMMEDIATE`` transaction.  The ordering
that makes the system crash-safe is enforced here and in ``storage.py``:

1. payload bytes are fsynced, then a batch row is committed, then the sender
   is acknowledged as ``spool-durable``;
2. the indexer fsyncs a hot copy and commits the ``indexed`` transition in
   one transaction, so acknowledgement never runs ahead of the data it names;
3. a cold copy is fsynced before the batch is pointed at it, and the hot copy
   is unlinked only after the pointer update commits;
4. leases return abandoned ``indexing`` rows to ``staged``.  Re-indexing is
   idempotent because a batch's hot path is deterministic and replaced
   atomically.

A source cannot occupy more than its reservation, so one producer's backlog
cannot exhaust another producer's credit.
"""

from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator

from .config import (
    DEFAULT_MAX_RECORD_BYTES,
    DEFAULT_RESERVED_BATCHES,
    DEFAULT_RESERVED_BYTES,
    DEFAULT_SOURCE_MAX_BATCH,
    SEED_SOURCES,
    Layout,
    default_settings,
    normalize_settings,
    utc_now,
)

SCHEMA_VERSION = 1

SCHEMA_STATEMENTS = (
    """CREATE TABLE IF NOT EXISTS schema_version (
        version INTEGER NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS sources (
        source_id TEXT PRIMARY KEY,
        label TEXT NOT NULL,
        max_batch_bytes INTEGER NOT NULL,
        reserved_bytes INTEGER NOT NULL,
        reserved_batches INTEGER NOT NULL,
        max_record_bytes INTEGER NOT NULL,
        active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS batches (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id TEXT NOT NULL,
        source_id TEXT NOT NULL REFERENCES sources(source_id),
        content_sha256 TEXT NOT NULL,
        records_sha256 TEXT NOT NULL,
        record_count INTEGER NOT NULL,
        payload_bytes INTEGER NOT NULL,
        spool_path TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN
            ('staged','indexing','indexed','quarantine')),
        quarantined_at_ingest INTEGER NOT NULL DEFAULT 0,
        ack_level TEXT NOT NULL,
        ack_epoch REAL NOT NULL,
        received_at TEXT NOT NULL,
        received_epoch REAL NOT NULL,
        indexed_at TEXT,
        indexed_epoch REAL,
        attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_epoch REAL NOT NULL DEFAULT 0,
        lease_until REAL NOT NULL DEFAULT 0,
        lease_owner TEXT,
        error TEXT,
        error_kind TEXT,
        hot_path TEXT,
        cold_path TEXT,
        storage_level TEXT NOT NULL DEFAULT 'none',
        integrity_checked_at TEXT,
        integrity_ok INTEGER,
        UNIQUE (source_id, batch_id)
    )""",
    "CREATE INDEX IF NOT EXISTS batches_ready ON batches (state, next_attempt_epoch, seq)",
    "CREATE INDEX IF NOT EXISTS batches_source ON batches (source_id, state)",
    "CREATE INDEX IF NOT EXISTS batches_lease ON batches (state, lease_until)",
    "CREATE INDEX IF NOT EXISTS batches_cold ON batches (cold_path)",
    """CREATE TABLE IF NOT EXISTS record_index (
        doc_id TEXT NOT NULL,
        source_id TEXT NOT NULL,
        batch_seq INTEGER NOT NULL REFERENCES batches(seq),
        value TEXT NOT NULL,
        PRIMARY KEY (source_id, doc_id)
    )""",
    "CREATE INDEX IF NOT EXISTS record_index_batch ON record_index (batch_seq)",
    """CREATE TABLE IF NOT EXISTS segment_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        level TEXT NOT NULL CHECK (level IN ('hot','cold')),
        relpath TEXT NOT NULL UNIQUE,
        sha256 TEXT NOT NULL,
        records INTEGER NOT NULL,
        bytes INTEGER NOT NULL,
        source_count INTEGER NOT NULL,
        created_epoch REAL NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS segment_members (
        segment_id INTEGER NOT NULL REFERENCES segment_files(id),
        batch_seq INTEGER NOT NULL REFERENCES batches(seq),
        PRIMARY KEY (segment_id, batch_seq)
    )""",
    "CREATE INDEX IF NOT EXISTS segment_members_batch ON segment_members (batch_seq)",
    """CREATE TABLE IF NOT EXISTS replies (
        request_id TEXT PRIMARY KEY,
        source_id TEXT NOT NULL,
        created_epoch REAL NOT NULL,
        body TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        epoch REAL NOT NULL,
        at TEXT NOT NULL,
        level TEXT NOT NULL,
        kind TEXT NOT NULL,
        source_id TEXT,
        batch_id TEXT,
        message TEXT NOT NULL,
        detail TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS events_epoch ON events (epoch DESC)",
    """CREATE TABLE IF NOT EXISTS samples (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        epoch REAL NOT NULL,
        payload TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS samples_epoch ON samples (epoch DESC)",
)


class Meta:
    def __init__(self, layout: Layout):
        self.layout = layout
        self.path = layout.meta_db

    # -- connection helpers -------------------------------------------------
    def connect(self) -> sqlite3.Connection:
        self.layout.ensure()
        conn = sqlite3.connect(
            self.path, timeout=30.0, isolation_level=None, check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def initialize(self, *, reset: bool = False) -> None:
        self.layout.ensure()
        if reset:
            for suffix in ("", "-wal", "-shm"):
                try:
                    Path(str(self.path) + suffix).unlink()
                except FileNotFoundError:
                    pass
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            for statement in SCHEMA_STATEMENTS:
                conn.execute(statement)
            row = conn.execute("SELECT version FROM schema_version").fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
                )
            elif row["version"] != SCHEMA_VERSION:
                raise RuntimeError(
                    f"data directory schema v{row['version']} != {SCHEMA_VERSION}"
                )
            seeded = conn.execute("SELECT COUNT(*) AS n FROM sources").fetchone()["n"]
            if seeded == 0:
                now = utc_now()
                for source_id, label in SEED_SOURCES:
                    conn.execute(
                        """INSERT INTO sources
                           (source_id, label, max_batch_bytes, reserved_bytes,
                            reserved_batches, max_record_bytes, active, created_at)
                           VALUES (?,?,?,?,?,?,1,?)""",
                        (
                            source_id,
                            label,
                            DEFAULT_SOURCE_MAX_BATCH,
                            DEFAULT_RESERVED_BYTES,
                            DEFAULT_RESERVED_BATCHES,
                            DEFAULT_MAX_RECORD_BYTES,
                            now,
                        ),
                    )
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()
        defaults = default_settings()
        existing = self.get_settings()
        missing = {k: v for k, v in defaults.items() if k not in existing}
        if missing:
            self.set_settings(missing)

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            yield conn
        finally:
            conn.close()

    # -- settings -----------------------------------------------------------
    def get_settings(self) -> dict[str, Any]:
        with self.read() as conn:
            rows = conn.execute("SELECT key, value FROM settings").fetchall()
        result: dict[str, Any] = {}
        for row in rows:
            try:
                result[row["key"]] = json.loads(row["value"])
            except json.JSONDecodeError:
                continue
        return result

    def set_settings(self, values: dict[str, Any]) -> dict[str, Any]:
        normalized = normalize_settings(values)
        with self.tx() as conn:
            for key, value in normalized.items():
                conn.execute(
                    "INSERT INTO settings (key, value) VALUES (?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (key, json.dumps(value)),
                )
        return normalized

    # -- sources ------------------------------------------------------------
    def list_sources(self) -> list[dict[str, Any]]:
        with self.read() as conn:
            sources = [dict(r) for r in conn.execute(
                "SELECT * FROM sources ORDER BY source_id"
            ).fetchall()]
            states = conn.execute(
                "SELECT source_id, state, COUNT(*) n, COALESCE(SUM(record_count),0) records, "
                "COALESCE(SUM(payload_bytes),0) bytes FROM batches "
                "GROUP BY source_id, state"
            ).fetchall()
            levels = conn.execute(
                "SELECT source_id, storage_level, COUNT(*) n FROM batches "
                "WHERE state='indexed' GROUP BY source_id, storage_level"
            ).fetchall()
        level_map = {
            (row["source_id"], row["storage_level"]): row["n"] for row in levels
        }
        by_source: dict[str, dict[str, Any]] = {}
        for d in sources:
            d["active"] = bool(d["active"])
            d["states"] = {}
            d["indexed_hot_batches"] = level_map.get((d["source_id"], "hot"), 0)
            d["indexed_cold_batches"] = level_map.get((d["source_id"], "cold"), 0)
            by_source[d["source_id"]] = d
        for row in states:
            src = by_source.get(row["source_id"])
            if src is not None:
                src["states"][row["state"]] = {
                    "count": row["n"],
                    "records": row["records"],
                    "bytes": row["bytes"],
                }
        return list(by_source.values())

    def source_live_usage(self, source_id: str) -> dict[str, int]:
        """Bytes and batches a source currently occupies in the spool.

        Credit must be computed from committed rows, not from the source
        configuration row, or the window never shrinks.
        """
        with self.read() as conn:
            row = conn.execute(
                "SELECT COUNT(*) n, COALESCE(SUM(payload_bytes),0) b FROM batches "
                "WHERE source_id=? AND state IN ('staged','indexing')",
                (source_id,),
            ).fetchone()
        return {"batches": int(row["n"]), "bytes": int(row["b"])}

    def reservation_totals(self) -> dict[str, int]:
        with self.read() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(reserved_bytes),0) b, "
                "COALESCE(SUM(reserved_batches),0) n FROM sources WHERE active=1"
            ).fetchone()
        return {"bytes": int(row["b"]), "batches": int(row["n"])}

    def get_source(self, source_id: str) -> dict[str, Any] | None:
        with self.read() as conn:
            row = conn.execute(
                "SELECT * FROM sources WHERE source_id=?", (source_id,)
            ).fetchone()
        return dict(row) if row else None

    def create_source(
        self,
        source_id: str,
        *,
        label: str,
        reserved_bytes: int,
        reserved_batches: int,
        max_batch_bytes: int,
        max_record_bytes: int,
    ) -> None:
        with self.tx() as conn:
            conn.execute(
                """INSERT INTO sources
                   (source_id, label, max_batch_bytes, reserved_bytes,
                    reserved_batches, max_record_bytes, active, created_at)
                   VALUES (?,?,?,?,?,?,1,?)""",
                (
                    source_id,
                    label,
                    max_batch_bytes,
                    reserved_bytes,
                    reserved_batches,
                    max_record_bytes,
                    utc_now(),
                ),
            )

    def update_source(self, source_id: str, **fields: Any) -> None:
        allowed = {
            "label",
            "max_batch_bytes",
            "reserved_bytes",
            "reserved_batches",
            "max_record_bytes",
            "active",
        }
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError(f"unknown source fields: {sorted(unknown)}")
        if not fields:
            return
        cols = ", ".join(f"{key}=?" for key in fields)
        with self.tx() as conn:
            cur = conn.execute(
                f"UPDATE sources SET {cols} WHERE source_id=?",
                (*fields.values(), source_id),
            )
            if cur.rowcount == 0:
                raise KeyError(source_id)

    # -- batch ingest -------------------------------------------------------
    def admit_live_batch(
        self,
        *,
        source_id: str,
        batch_id: str,
        digest: str,
        records_digest: str,
        record_count: int,
        payload_bytes: int,
        spool_path: str,
        max_inflight: int,
        spool_cap_bytes: int,
        reserved_batches: int,
        reserved_bytes: int,
    ) -> tuple[dict[str, Any] | None, str]:
        """Atomically check budgets and insert, or return a refusal reason.

        Status is one of ``created``, ``duplicate``, ``conflict``,
        ``limit_source`` or ``limit_global``.
        """
        now = utc_now()
        now_epoch = time.time()
        with self.tx() as conn:
            existing = conn.execute(
                "SELECT * FROM batches WHERE source_id=? AND batch_id=?",
                (source_id, batch_id),
            ).fetchone()
            if existing is not None:
                if existing["content_sha256"] != digest:
                    return dict(existing), "conflict"
                return dict(existing), "duplicate"
            live = conn.execute(
                "SELECT COUNT(*) n, COALESCE(SUM(payload_bytes),0) b FROM batches "
                "WHERE state IN ('staged','indexing')"
            ).fetchone()
            mine = conn.execute(
                "SELECT COUNT(*) n, COALESCE(SUM(payload_bytes),0) b FROM batches "
                "WHERE source_id=? AND state IN ('staged','indexing')",
                (source_id,),
            ).fetchone()
            if (
                live["n"] + 1 > max_inflight
                or live["b"] + payload_bytes > spool_cap_bytes
            ):
                return None, "limit_global"
            if (
                mine["n"] + 1 > reserved_batches
                or mine["b"] + payload_bytes > reserved_bytes
            ):
                return None, "limit_source"
            conn.execute(
                """INSERT INTO batches
                   (batch_id, source_id, content_sha256, records_sha256, record_count,
                    payload_bytes, spool_path, state, ack_level, ack_epoch,
                    received_at, received_epoch)
                   VALUES (?,?,?,?,?,?,?, 'staged', 'spool-durable', ?, ?, ?)""",
                (
                    batch_id,
                    source_id,
                    digest,
                    records_digest,
                    record_count,
                    payload_bytes,
                    spool_path,
                    now_epoch,
                    now,
                    now_epoch,
                ),
            )
            seq = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            row = conn.execute("SELECT * FROM batches WHERE seq=?", (seq,)).fetchone()
        return dict(row), "created"

    def admit_quarantine_batch(
        self,
        *,
        source_id: str,
        batch_id: str,
        digest: str,
        records_digest: str,
        record_count: int,
        payload_bytes: int,
        spool_path: str,
        error: str,
        error_kind: str,
        max_batches: int,
        max_bytes: int,
    ) -> tuple[dict[str, Any] | None, str]:
        now = utc_now()
        now_epoch = time.time()
        with self.tx() as conn:
            existing = conn.execute(
                "SELECT * FROM batches WHERE source_id=? AND batch_id=?",
                (source_id, batch_id),
            ).fetchone()
            if existing is not None:
                if existing["content_sha256"] != digest:
                    return dict(existing), "conflict"
                return dict(existing), "duplicate"
            held = conn.execute(
                "SELECT COUNT(*) n, COALESCE(SUM(payload_bytes),0) b FROM batches "
                "WHERE state='quarantine'"
            ).fetchone()
            if held["n"] + 1 > max_batches or held["b"] + payload_bytes > max_bytes:
                return None, "limit_quarantine"
            conn.execute(
                """INSERT INTO batches
                   (batch_id, source_id, content_sha256, records_sha256, record_count,
                    payload_bytes, spool_path, state, quarantined_at_ingest,
                    ack_level, ack_epoch, received_at, received_epoch, error,
                    error_kind)
                   VALUES (?,?,?,?,?,?,?, 'quarantine', 1, 'quarantined', ?, ?, ?, ?, ?)""",
                (
                    batch_id,
                    source_id,
                    digest,
                    records_digest,
                    record_count,
                    payload_bytes,
                    spool_path,
                    now_epoch,
                    now,
                    now_epoch,
                    error,
                    error_kind,
                ),
            )
            seq = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            row = conn.execute("SELECT * FROM batches WHERE seq=?", (seq,)).fetchone()
        return dict(row), "created"

    def get_batch(self, source_id: str, batch_id: str) -> dict[str, Any] | None:
        with self.read() as conn:
            row = conn.execute(
                "SELECT * FROM batches WHERE source_id=? AND batch_id=?",
                (source_id, batch_id),
            ).fetchone()
        return dict(row) if row else None

    def get_batch_by_seq(self, seq: int) -> dict[str, Any] | None:
        with self.read() as conn:
            row = conn.execute("SELECT * FROM batches WHERE seq=?", (seq,)).fetchone()
        return dict(row) if row else None

    # -- indexing worker ----------------------------------------------------
    def recover_all_leases(self) -> int:
        """At worker startup, no worker owns an indexing lease (lock held)."""
        with self.tx() as conn:
            cur = conn.execute(
                "UPDATE batches SET state='staged', lease_owner=NULL, lease_until=0, "
                "next_attempt_epoch=0 WHERE state='indexing'"
            )
            return cur.rowcount

    def recover_expired_leases(self, now: float | None = None) -> int:
        now = time.time() if now is None else now
        with self.tx() as conn:
            cur = conn.execute(
                "UPDATE batches SET state='staged', lease_owner=NULL, lease_until=0, "
                "next_attempt_epoch=0 WHERE state='indexing' AND lease_until < ?",
                (now,),
            )
            return cur.rowcount

    def lease_next_batch(
        self, owner: str, lease_seconds: int
    ) -> dict[str, Any] | None:
        """Claim the oldest ready batch.

        Reserved credit is enforced at admission; the worker must never
        refuse work because the spool is full, or the queue would deadlock
        (only indexing can drain it).  Quarantined batches are not in
        ``staged`` and therefore cannot occupy this single-threaded path.
        """
        with self.tx() as conn:
            conn.execute(
                "UPDATE batches SET state='staged', lease_owner=NULL, lease_until=0 "
                "WHERE state='indexing' AND lease_until < ?",
                (time.time(),),
            )
            row = conn.execute(
                "SELECT * FROM batches WHERE state='staged' AND next_attempt_epoch <= ? "
                "ORDER BY seq LIMIT 1",
                (time.time(),),
            ).fetchone()
            if row is None:
                return None
            now = time.time()
            cur = conn.execute(
                "UPDATE batches SET state='indexing', lease_owner=?, lease_until=? "
                "WHERE seq=? AND state='staged'",
                (owner, now + lease_seconds, row["seq"]),
            )
            if cur.rowcount == 0:
                return None
            claimed = conn.execute(
                "SELECT * FROM batches WHERE seq=?", (row["seq"],)
            ).fetchone()
        return dict(claimed)

    def renew_lease(self, seq: int, owner: str, lease_seconds: int) -> bool:
        with self.tx() as conn:
            cur = conn.execute(
                "UPDATE batches SET lease_until=? WHERE seq=? AND lease_owner=? "
                "AND state='indexing'",
                (time.time() + lease_seconds, seq, owner),
            )
            return cur.rowcount > 0

    def owns_lease(self, seq: int, owner: str) -> bool:
        with self.read() as conn:
            row = conn.execute(
                "SELECT 1 FROM batches WHERE seq=? AND lease_owner=? AND state='indexing'",
                (seq, owner),
            ).fetchone()
        return row is not None

    def finish_indexed(
        self, seq: int, *, hot_path: str, records: list[dict[str, Any]]
    ) -> None:
        """Atomically publish the index rows and the ``indexed`` state.

        The hot file has already been fsynced by the caller.  The batch is
        only acknowledged as indexed after this transaction commits, which
        also makes the record index visible in the same atomic step.
        """
        now = utc_now()
        epoch = time.time()
        with self.tx() as conn:
            row = conn.execute(
                "SELECT state, source_id, content_sha256, hot_path FROM batches WHERE seq=?",
                (seq,),
            ).fetchone()
            if row is None:
                raise KeyError(seq)
            if row["state"] not in ("staged", "indexing"):
                raise RuntimeError(f"batch {seq} is {row['state']}, cannot finish")
            if row["hot_path"] is not None:
                raise RuntimeError(f"batch {seq} already has a hot file")
            conn.execute(
                """UPDATE batches SET state='indexed', ack_level='indexed',
                   ack_epoch=?, indexed_at=?, indexed_epoch=?,
                   hot_path=?, storage_level='hot', lease_until=0, lease_owner=NULL,
                   next_attempt_epoch=0, error=NULL, error_kind=NULL,
                   integrity_checked_at=?, integrity_ok=1 WHERE seq=?""",
                (epoch, now, epoch, hot_path, now, seq),
            )
            # Upsert so replaying a batch after a crash is idempotent.
            for record in records:
                conn.execute(
                    """INSERT INTO record_index (doc_id, source_id, batch_seq, value)
                       VALUES (?,?,?,?)
                       ON CONFLICT(source_id, doc_id) DO UPDATE SET
                         batch_seq=excluded.batch_seq, value=excluded.value""",
                    (
                        record["id"],
                        row["source_id"],
                        seq,
                        json.dumps(record["value"], ensure_ascii=False),
                    ),
                )

    def fail_attempt(
        self, seq: int, *, error: str, error_kind: str, max_attempts: int
    ) -> tuple[str, int]:
        with self.tx() as conn:
            row = conn.execute(
                "SELECT attempts FROM batches WHERE seq=?", (seq,)
            ).fetchone()
            if row is None:
                raise KeyError(seq)
            attempts = int(row["attempts"]) + 1
            if attempts >= max_attempts:
                state = "quarantine"
                conn.execute(
                    "UPDATE batches SET state='quarantine', ack_level='quarantined', "
                    "ack_epoch=?, error=?, error_kind=?, attempts=?, lease_until=0, "
                    "lease_owner=NULL, next_attempt_epoch=0 WHERE seq=?",
                    (time.time(), error, error_kind, attempts, seq),
                )
            else:
                state = "staged"
                delay = min(30.0, 0.5 * (2 ** (attempts - 1)))
                conn.execute(
                    "UPDATE batches SET state='staged', error=?, error_kind=?, "
                    "attempts=?, lease_until=0, lease_owner=NULL, "
                    "next_attempt_epoch=? WHERE seq=?",
                    (error, error_kind, attempts, time.time() + delay, seq),
                )
        return state, attempts

    def release_lease(self, seq: int, owner: str) -> None:
        with self.tx() as conn:
            conn.execute(
                "UPDATE batches SET state='staged', lease_until=0, lease_owner=NULL "
                "WHERE seq=? AND state='indexing' AND lease_owner=?",
                (seq, owner),
            )

    def transition_to_quarantine(
        self, seq: int, *, error: str, error_kind: str
    ) -> bool:
        """Quarantine a batch in the worker path, keeping its ack truthful."""
        with self.tx() as conn:
            cur = conn.execute(
                "UPDATE batches SET state='quarantine', ack_level='quarantined', "
                "ack_epoch=?, error=?, error_kind=?, lease_until=0, lease_owner=NULL, "
                "next_attempt_epoch=0 WHERE seq=? AND state IN ('staged','indexing')",
                (time.time(), error, error_kind, seq),
            )
            return cur.rowcount > 0

    def requeue_batch(self, seq: int) -> bool:
        """Return a held batch to the index queue.

        Ingest-time rejections are requeued too: the operator may have fixed
        the producer, and the worker re-validates the stored bytes before
        accepting them.  A still-bad batch is quarantined again with a fresh
        event rather than pretending to succeed.
        """
        with self.tx() as conn:
            cur = conn.execute(
                "UPDATE batches SET state='staged', error=NULL, error_kind=NULL, "
                "attempts=0, next_attempt_epoch=0, lease_until=0, lease_owner=NULL, "
                "quarantined_at_ingest=0 WHERE seq=? AND state='quarantine'",
                (seq,),
            )
            return cur.rowcount > 0

    def query_records(
        self, source_id: str | None = None, limit: int = 50, offset: int = 0
    ) -> list[dict[str, Any]]:
        with self.read() as conn:
            if source_id:
                rows = conn.execute(
                    "SELECT ri.doc_id, ri.source_id, ri.value, b.batch_id "
                    "FROM record_index ri JOIN batches b ON b.seq=ri.batch_seq "
                    "WHERE ri.source_id=? ORDER BY ri.doc_id LIMIT ? OFFSET ?",
                    (source_id, limit, offset),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT ri.doc_id, ri.source_id, ri.value, b.batch_id "
                    "FROM record_index ri JOIN batches b ON b.seq=ri.batch_seq "
                    "ORDER BY ri.source_id, ri.doc_id LIMIT ? OFFSET ?",
                    (limit, offset),
                ).fetchall()
        out = []
        for row in rows:
            d = dict(row)
            try:
                d["value"] = json.loads(d["value"])
            except json.JSONDecodeError:
                pass
            out.append(d)
        return out

    def record_index_count(self) -> int:
        with self.read() as conn:
            row = conn.execute("SELECT COUNT(*) n FROM record_index").fetchone()
        return int(row["n"])

    # -- tiered storage -----------------------------------------------------
    def set_batch_cold(self, seq: int, *, cold_path: str) -> bool:
        with self.tx() as conn:
            cur = conn.execute(
                "UPDATE batches SET cold_path=?, hot_path=NULL, storage_level='cold' "
                "WHERE seq=? AND state='indexed' AND storage_level='hot'",
                (cold_path, seq),
            )
            return cur.rowcount > 0

    def record_cold_segment(
        self,
        *,
        relpath: str,
        sha256: str,
        records: int,
        byte_count: int,
        source_count: int,
        batch_seqs: list[int],
    ) -> int:
        """Make a fully written cold segment visible and move its batches to it."""
        with self.tx() as conn:
            cur = conn.execute(
                "INSERT INTO segment_files "
                "(level, relpath, sha256, records, bytes, source_count, created_epoch) "
                "VALUES ('cold',?,?,?,?,?,?)",
                (relpath, sha256, records, byte_count, source_count, time.time()),
            )
            segment_id = int(cur.lastrowid)
            for seq in batch_seqs:
                conn.execute(
                    "INSERT OR IGNORE INTO segment_members (segment_id, batch_seq) "
                    "VALUES (?,?)",
                    (segment_id, seq),
                )
                conn.execute(
                    "UPDATE batches SET cold_path=?, hot_path=NULL, storage_level='cold' "
                    "WHERE seq=? AND state='indexed'",
                    (relpath, seq),
                )
            return segment_id

    def list_segment_files(self) -> list[dict[str, Any]]:
        with self.read() as conn:
            rows = conn.execute("SELECT * FROM segment_files ORDER BY id").fetchall()
        return [dict(row) for row in rows]

    def cold_segment_files(self) -> list[dict[str, Any]]:
        with self.read() as conn:
            rows = conn.execute(
                "SELECT * FROM segment_files WHERE level='cold' ORDER BY id"
            ).fetchall()
        return [dict(row) for row in rows]

    def segment_members(self, segment_id: int) -> list[int]:
        with self.read() as conn:
            rows = conn.execute(
                "SELECT batch_seq FROM segment_members WHERE segment_id=? ORDER BY batch_seq",
                (segment_id,),
            ).fetchall()
        return [row["batch_seq"] for row in rows]

    def hot_batches_for_promotion(self, limit: int) -> list[dict[str, Any]]:
        with self.read() as conn:
            rows = conn.execute(
                "SELECT * FROM batches WHERE state='indexed' AND storage_level='hot' "
                "AND hot_path IS NOT NULL ORDER BY indexed_epoch ASC, seq ASC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def hot_batch_count(self) -> int:
        with self.read() as conn:
            row = conn.execute(
                "SELECT COUNT(*) n FROM batches WHERE state='indexed' "
                "AND storage_level='hot'"
            ).fetchone()
        return int(row["n"])

    def replace_cold_segments(
        self,
        *,
        consumed_ids: list[int],
        relpath: str,
        sha256: str,
        records: int,
        byte_count: int,
        source_count: int,
        batch_seqs: list[int],
    ) -> int:
        """Publish a compacted segment and make it the sole cold copy."""
        with self.tx() as conn:
            cur = conn.execute(
                "INSERT INTO segment_files "
                "(level, relpath, sha256, records, bytes, source_count, created_epoch) "
                "VALUES ('cold',?,?,?,?,?,?)",
                (relpath, sha256, records, byte_count, source_count, time.time()),
            )
            new_id = int(cur.lastrowid)
            for seq in batch_seqs:
                conn.execute(
                    "INSERT OR IGNORE INTO segment_members (segment_id, batch_seq) "
                    "VALUES (?,?)",
                    (new_id, seq),
                )
                conn.execute(
                    "UPDATE batches SET cold_path=? WHERE seq=? AND state='indexed' "
                    "AND storage_level='cold'",
                    (relpath, seq),
                )
            placeholders = ",".join("?" for _ in consumed_ids)
            conn.execute(
                f"DELETE FROM segment_members WHERE segment_id IN ({placeholders})",
                consumed_ids,
            )
            conn.execute(
                f"DELETE FROM segment_files WHERE id IN ({placeholders})", consumed_ids
            )
            return new_id

    def segment_batch_rows(self, segment_id: int) -> list[dict[str, Any]]:
        with self.read() as conn:
            rows = conn.execute(
                "SELECT b.* FROM batches b JOIN segment_members sm "
                "ON sm.batch_seq=b.seq WHERE sm.segment_id=? ORDER BY b.seq",
                (segment_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def storage_totals(self) -> dict[str, int]:
        with self.read() as conn:
            hot = conn.execute(
                "SELECT COUNT(*) n, COALESCE(SUM(record_count),0) r FROM batches "
                "WHERE state='indexed' AND storage_level='hot'"
            ).fetchone()
            cold = conn.execute(
                "SELECT COUNT(*) n, COALESCE(SUM(record_count),0) r FROM batches "
                "WHERE state='indexed' AND storage_level='cold'"
            ).fetchone()
            files = conn.execute(
                "SELECT COUNT(*) n, COALESCE(SUM(bytes),0) b FROM segment_files"
            ).fetchone()
        return {
            "hot_batches": hot["n"],
            "hot_records": hot["r"],
            "cold_batches": cold["n"],
            "cold_records": cold["r"],
            "segment_files": files["n"],
            "segment_bytes": files["b"],
        }

    def oldest_indexed_hot_epoch(self) -> float | None:
        with self.read() as conn:
            row = conn.execute(
                "SELECT MIN(indexed_epoch) e FROM batches "
                "WHERE state='indexed' AND storage_level='hot'"
            ).fetchone()
        return row["e"]

    # -- replies ------------------------------------------------------------
    def get_reply(self, request_id: str) -> dict[str, Any] | None:
        with self.read() as conn:
            row = conn.execute(
                "SELECT * FROM replies WHERE request_id=?", (request_id,)
            ).fetchone()
        return dict(row) if row else None

    def save_reply(self, request_id: str, source_id: str, body: dict[str, Any]) -> None:
        with self.tx() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO replies (request_id, source_id, created_epoch, body) "
                "VALUES (?,?,?,?)",
                (request_id, source_id, time.time(), json.dumps(body, sort_keys=True)),
            )

    # -- events / samples ---------------------------------------------------
    def add_event(
        self,
        level: str,
        kind: str,
        message: str,
        *,
        source_id: str | None = None,
        batch_id: str | None = None,
        detail: Any = None,
    ) -> None:
        with self.tx() as conn:
            conn.execute(
                "INSERT INTO events (epoch, at, level, kind, source_id, batch_id, "
                "message, detail) VALUES (?,?,?,?,?,?,?,?)",
                (
                    time.time(),
                    utc_now(),
                    level,
                    kind,
                    source_id,
                    batch_id,
                    message,
                    json.dumps(detail) if detail is not None else None,
                ),
            )
            conn.execute(
                "DELETE FROM events WHERE id < (SELECT MAX(id) FROM events) - 2000"
            )

    def list_events(self, limit: int = 60) -> list[dict[str, Any]]:
        with self.read() as conn:
            rows = conn.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        out = []
        for row in rows:
            d = dict(row)
            if d["detail"]:
                try:
                    d["detail"] = json.loads(d["detail"])
                except json.JSONDecodeError:
                    pass
            out.append(d)
        return out

    def event_counts(self, since_epoch: float) -> dict[str, int]:
        with self.read() as conn:
            rows = conn.execute(
                "SELECT kind, COUNT(*) n FROM events WHERE epoch > ? GROUP BY kind",
                (since_epoch,),
            ).fetchall()
        return {row["kind"]: row["n"] for row in rows}

    def prune_samples(self, before_epoch: float) -> None:
        with self.tx() as conn:
            conn.execute("DELETE FROM samples WHERE epoch < ?", (before_epoch,))

    def add_sample(self, payload: dict[str, Any], *, retain: int = 900) -> None:
        with self.tx() as conn:
            conn.execute(
                "INSERT INTO samples (epoch, payload) VALUES (?,?)",
                (time.time(), json.dumps(payload, separators=(",", ":"))),
            )
            conn.execute(
                "DELETE FROM samples WHERE id < (SELECT MAX(id) FROM samples) - ?",
                (retain,),
            )

    def list_samples(self, limit: int = 240) -> list[dict[str, Any]]:
        with self.read() as conn:
            rows = conn.execute(
                "SELECT epoch, payload FROM samples ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [
            {"epoch": row["epoch"], **json.loads(row["payload"])}
            for row in reversed(rows)
        ]

    # -- aggregates ---------------------------------------------------------
    def state_totals(self) -> dict[str, dict[str, int]]:
        with self.read() as conn:
            rows = conn.execute(
                "SELECT state, COUNT(*) n, COALESCE(SUM(record_count),0) records, "
                "COALESCE(SUM(payload_bytes),0) bytes FROM batches GROUP BY state"
            ).fetchall()
        return {
            row["state"]: {
                "count": row["n"],
                "records": row["records"],
                "bytes": row["bytes"],
            }
            for row in rows
        }

    def live_spool(self) -> dict[str, int]:
        with self.read() as conn:
            row = conn.execute(
                "SELECT COUNT(*) n, COALESCE(SUM(payload_bytes),0) b, "
                "MIN(received_epoch) oldest FROM batches "
                "WHERE state IN ('staged','indexing')"
            ).fetchone()
            q = conn.execute(
                "SELECT COUNT(*) n, COALESCE(SUM(payload_bytes),0) b FROM batches "
                "WHERE state='quarantine'"
            ).fetchone()
        return {
            "live_batches": row["n"],
            "live_bytes": row["b"],
            "oldest_live_epoch": row["oldest"],
            "quarantine_batches": q["n"],
            "quarantine_bytes": q["b"],
        }

    def pending_batches(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.read() as conn:
            rows = conn.execute(
                "SELECT seq, source_id, batch_id, state, record_count, payload_bytes, "
                "attempts, error, error_kind, received_at, indexed_at, storage_level, "
                "ack_level FROM batches "
                "WHERE state IN ('staged','indexing','quarantine') "
                "ORDER BY seq LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def recent_batches(self, limit: int = 25) -> list[dict[str, Any]]:
        with self.read() as conn:
            rows = conn.execute(
                "SELECT seq, source_id, batch_id, state, record_count, payload_bytes, "
                "storage_level, attempts, error, received_at, indexed_at, ack_level "
                "FROM batches ORDER BY seq DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def all_batches(self) -> list[dict[str, Any]]:
        with self.read() as conn:
            rows = conn.execute("SELECT * FROM batches ORDER BY seq").fetchall()
        return [dict(row) for row in rows]

    def db_stats(self) -> dict[str, int]:
        with self.read() as conn:
            page = conn.execute("PRAGMA page_count").fetchone()[0]
            size = conn.execute("PRAGMA page_size").fetchone()[0]
        wal = 0
        try:
            wal = Path(str(self.path) + "-wal").stat().st_size
        except FileNotFoundError:
            pass
        return {"db_bytes": page * size, "wal_bytes": wal}

    def checkpoint(self) -> None:
        conn = self.connect()
        try:
            conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        except sqlite3.Error:
            pass
        finally:
            conn.close()
