"""Per-source credit and the staging/confirmation decision.

Credit is derived from committed state, never from an in-memory counter, so
restarting the API cannot over-grant.  The numbers mean:

* ``reserved_bytes`` / ``reserved_batches`` — a guarantee.  The sum of all
  reservations is kept within the spool cap, so no other source can consume
  this source's room.
* **slack** — ``spool_cap - sum(reservations)``.  A source may borrow slack
  while the spool is below the soft waterline.  Borrowing is conveniences,
  not guarantee; at the waterline the source falls back to its reservation.
* **soft waterline** (``soft_limit_pct`` × cap) — stop granting borrowed
  credit.  Send within your reservation still proceeds.  This is the
  "reduce the quota" decision, and it is visible to senders as lower
  ``credit_bytes`` rather than an immediate error.
* **index lag** (``backlog_reduce_secs``) — if the oldest live batch has been
  waiting longer than this, the indexer is falling behind even though the
  spool may not be full yet.  Borrowed credit is withdrawn early (same shape
  as the soft waterline) so the backlog stops growing before the hard cap is
  reached.
* **hard cap / free-disk floor** — respond ``429`` with ``Retry-After`` and
  write nothing.  This is what prevents a stalled indexer from filling the
  disk.

A sender sends a batch only while ``credit_bytes >= len(body)`` and
``credit_batches >= 1``.  The receiver re-checks all budgets inside the
insert transaction, so a stale credit view cannot overfill the spool.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Any


# Smallest batch worth accepting.  A source's ``max_batch_bytes`` is an upper
# bound, not the size the receiver expects; senders should split work to fit
# the advertised window.
MIN_BATCH_BYTES = 4096


@dataclass
class Credit:
    source_id: str
    granted_bytes: int
    granted_batches: int
    reserved_bytes: int
    reserved_batches: int
    used_bytes: int
    used_batches: int
    batch_size_bytes: int
    reason: str
    retry_after: float
    global_limited: bool
    borrowing: bool
    slack_bytes: int
    slack_batches: int
    pressure: str

    def to_wire(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "credit_bytes": max(0, self.granted_bytes),
            "credit_batches": max(0, self.granted_batches),
            "reserved_bytes": self.reserved_bytes,
            "reserved_batches": self.reserved_batches,
            "used_bytes": self.used_bytes,
            "used_batches": self.used_batches,
            "batch_size_bytes": self.batch_size_bytes,
            "reason": self.reason,
            "retry_after": round(max(0.0, self.retry_after), 2),
            "global_limited": self.global_limited,
            "borrowing": self.borrowing,
            "slack_bytes": self.slack_bytes,
            "slack_batches": self.slack_batches,
            "pressure": self.pressure,
        }


def pressure_state(
    *,
    live_bytes: int,
    live_batches: int,
    spool_cap: int,
    max_inflight: int,
    soft_pct: float,
    disk_free: int,
    min_free: int,
) -> str:
    if disk_free <= min_free:
        return "disk_free_floor"
    if live_bytes >= spool_cap or live_batches >= max_inflight:
        return "hard_cap"
    if live_bytes >= spool_cap * soft_pct or live_batches >= max_inflight * soft_pct:
        return "soft_limit"
    return "normal"


def slack_of(
    spool_cap: int, max_inflight: int, reservations_bytes: int, reservations_batches: int
) -> tuple[int, int]:
    return (
        max(0, spool_cap - reservations_bytes),
        max(0, max_inflight - reservations_batches),
    )


def compute_credit(
    *,
    source: dict[str, Any],
    live_spool_bytes: int,
    live_spool_batches: int,
    reservations_bytes: int,
    reservations_batches: int,
    source_bytes: int,
    source_batches: int,
    settings: dict[str, Any],
    disk_free: int,
    oldest_live_age: float | None = None,
) -> Credit:
    cap = int(settings["spool_cap_bytes"])
    max_inflight = int(settings["max_inflight_batches"])
    soft_pct = float(settings["soft_limit_pct"])
    min_free = int(settings["min_free_bytes"])
    reserved_bytes = max(4096, int(source["reserved_bytes"]))
    reserved_batches = max(1, int(source["reserved_batches"]))
    batch_size = max(4096, int(source["max_batch_bytes"]))
    slack_bytes, slack_batches = slack_of(
        cap, max_inflight, reservations_bytes, reservations_batches
    )
    pressure = pressure_state(
        live_bytes=live_spool_bytes,
        live_batches=live_spool_batches,
        spool_cap=cap,
        max_inflight=max_inflight,
        soft_pct=soft_pct,
        disk_free=disk_free,
        min_free=min_free,
    )

    # Index lag is a leading indicator: the spool may still be under the soft
    # waterline while the indexer is visibly falling behind.  Withdraw borrowed
    # credit now, while reserved room continues to work, so the cap is reached
    # by a slow curl rather than a sudden wall.
    lag_limited = (
        pressure == "normal"
        and oldest_live_age is not None
        and oldest_live_age >= float(settings["backlog_reduce_secs"])
    )
    if lag_limited:
        pressure = "soft_limit"

    # Limit = own reservation plus shared slack.  At the soft waterline the
    # source may only use room it actually reserved.
    if pressure == "normal":
        limit_bytes = min(cap, reserved_bytes + slack_bytes)
        limit_batches = min(max_inflight, reserved_batches + slack_batches)
    elif pressure == "soft_limit":
        limit_bytes = min(cap, reserved_bytes)
        limit_batches = min(max_inflight, reserved_batches)
    else:
        limit_bytes = limit_batches = 0

    global_bytes = max(0, cap - live_spool_bytes)
    global_batches = max(0, max_inflight - live_spool_batches)
    own_bytes = max(0, limit_bytes - source_bytes)
    own_batches = max(0, limit_batches - source_batches)
    disk_room = max(0, disk_free - min_free)
    granted_bytes = min(own_bytes, global_bytes, disk_room)
    granted_batches = min(own_batches, global_batches)
    borrowing = pressure == "normal" and (slack_bytes > 0 or slack_batches > 0)

    if not source.get("active", 1):
        reason = "source_paused"
        granted_bytes = granted_batches = 0
    elif pressure in ("disk_free_floor", "hard_cap"):
        reason = pressure
        granted_bytes = granted_batches = 0
    elif lag_limited:
        reason = "index_lag_reduce"
    elif pressure == "soft_limit":
        reason = "soft_limit_reserved_only"
    elif granted_batches <= 0:
        reason = "reservation_exhausted"
    elif granted_bytes < MIN_BATCH_BYTES:
        # Credit too small for even a tiny batch: report zero so senders hold
        # instead of sending a header-only payload that then gets refused.
        reason = "fragment_too_small"
        granted_batches = 0
    else:
        reason = "ok"

    retry = {"disk_free_floor": 15.0, "hard_cap": 5.0, "soft_limit": 2.0}.get(
        pressure, 2.0
    )
    global_limited = (
        (global_bytes < own_bytes and granted_bytes >= global_bytes)
        or (global_batches < own_batches and granted_batches >= global_batches)
    )
    if not source.get("active", 1):
        global_limited = False
    return Credit(
        source_id=source["source_id"],
        granted_bytes=granted_bytes,
        granted_batches=granted_batches,
        reserved_bytes=reserved_bytes,
        reserved_batches=reserved_batches,
        used_bytes=source_bytes,
        used_batches=source_batches,
        batch_size_bytes=batch_size,
        reason=reason,
        retry_after=retry,
        global_limited=global_limited,
        borrowing=bool(borrowing),
        slack_bytes=slack_bytes,
        slack_batches=slack_batches,
        pressure=pressure,
    )


def refusal_retry_after(credit: Credit) -> int:
    return max(1, int(math.ceil(credit.retry_after)))
