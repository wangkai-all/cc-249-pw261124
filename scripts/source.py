#!/usr/bin/env python3
"""Local source simulator that honours the receiver's credit window.

The receiver never pushes work into itself; producers decide how much to send
based on the advertised credit and local backlog.  This tool demonstrates the
contract a real producer should follow:

1. read ``GET /api/credit/<source>``;
2. send at most ``credit_batches`` batches, each no larger than
   ``min(credit_bytes, max_batch_bytes)``;
3. on ``429`` sleep for ``Retry-After`` and re-read credit;
4. always reuse the same ``batch_id`` for bytes that are being retried, so a
   lost response cannot duplicate records;
5. never generate new batch ids while holding an unacknowledged one, which is
   what keeps a source inside its reservation.

Usage:
  python3 scripts/source.py --source alpha --count 200 \\
      --receiver http://127.0.0.1:8787 --rate 20
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import sys
import time
import urllib.error
import urllib.request
import uuid


def http_json(request: urllib.request.Request) -> tuple[int, dict, dict]:
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.loads(response.read()), dict(response.headers)
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = {"raw": raw.decode("utf-8", "replace")}
        return exc.code, body, dict(exc.headers)


def get_credit(receiver: str, source: str) -> dict:
    request = urllib.request.Request(f"{receiver}/api/credit/{source}")
    status, body, _ = http_json(request)
    if status != 200:
        raise SystemExit(f"cannot read credit: {status} {body}")
    return body


def post_batch(receiver: str, payload: dict, *, wait: bool = False) -> tuple[int, dict, dict]:
    data = json.dumps(payload, separators=(",", ":")).encode()
    headers = {
        "Content-Type": "application/json",
        "X-Request-Id": f"src-{uuid.uuid4().hex}",
    }
    if wait:
        headers["X-Wait-Indexed"] = "1"
    request = urllib.request.Request(
        f"{receiver}/ingest/{payload['source_id']}",
        data=data,
        method="POST",
        headers=headers,
    )
    return http_json(request)


def make_batch(source: str, index: int, *, records: int, value_bytes: int) -> dict:
    """Build a batch whose declared digest matches the receiver's check.

    The receiver validates ``content_sha256`` over the canonical JSON of the
    document *without* the digest field (a document cannot hash itself), so
    the producer hashes exactly that.
    """
    batch_id = f"{source}-{index:08d}-{time.time_ns() % 100000:05d}"
    payload = {
        "format": "pointwise-receiver/v1",
        "source_id": source,
        "batch_id": batch_id,
        "records": [
            {
                "id": f"doc-{index:08d}-{n:05d}",
                "value": "v" * max(1, value_bytes) + f":{index}:{n}",
            }
            for n in range(records)
        ],
    }
    canonical = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    payload["content_sha256"] = hashlib.sha256(canonical).hexdigest()
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="credit-aware local producer")
    parser.add_argument("--receiver", default="http://127.0.0.1:8787")
    parser.add_argument("--source", default="alpha")
    parser.add_argument("--count", type=int, default=50, help="batches to create")
    parser.add_argument("--records", type=int, default=10)
    parser.add_argument("--value-bytes", type=int, default=64)
    parser.add_argument("--rate", type=float, default=10.0, help="max batches/second")
    parser.add_argument("--wait-indexed", action="store_true")
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args(argv)

    random.seed(args.seed)
    sent = 0
    created = 0
    throttles = 0
    failures = 0
    pending: dict[str, dict] = {}
    next_batch = 0
    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    start = time.monotonic()
    last = 0.0

    while created < args.count:
        credit = get_credit(args.receiver, args.source)
        if credit["credit_batches"] <= 0 or credit["credit_bytes"] < 4096:
            throttles += 1
            delay = max(0.2, float(credit.get("retry_after") or 1.0))
            print(
                f"[{args.source}] hold: {credit['reason']} "
                f"(credit {credit['credit_batches']} batches, "
                f"{credit['credit_bytes']} bytes) sleep {delay:.1f}s",
                flush=True,
            )
            time.sleep(delay)
            continue
        if not pending and next_batch < args.count:
            payload = make_batch(
                args.source,
                next_batch,
                records=args.records,
                value_bytes=args.value_bytes,
            )
            pending[payload["batch_id"]] = payload
            next_batch += 1
        if not pending:
            break
        batch_id = next(iter(pending))
        payload = pending[batch_id]
        # Pace against elapsed seconds since start; ``last`` must be in the
        # same clock as ``elapsed`` or the sleep grows to the machine uptime.
        elapsed = time.monotonic() - start
        if elapsed < last + interval:
            time.sleep(last + interval - elapsed)
        last = time.monotonic() - start
        status, body, headers = post_batch(
            args.receiver, payload, wait=args.wait_indexed
        )
        sent += 1
        if status in (200, 202):
            created += 1
            del pending[batch_id]
            ack = body.get("ack", {})
            print(
                f"[{args.source}] {status} {payload['batch_id']} "
                f"state={body.get('state')} ack={ack.get('level')} "
                f"credit_after={body.get('credit', {}).get('credit_batches')}",
                flush=True,
            )
        elif status in (409, 422):
            # Invalid content is terminal; identical replay is deduplicated.
            if body.get("duplicate"):
                created += 1
                del pending[batch_id]
                continue
            failures += 1
            del pending[batch_id]
            print(f"[{args.source}] rejected {payload['batch_id']}: {body}", flush=True)
            # A terminal rejection must not wedge the producer: drop this
            # batch and let the next one be generated, otherwise the caller
            # waits forever for ``--count`` successes.
            if not pending:
                continue
        else:
            retry = int(headers.get("Retry-After") or 1)
            print(
                f"[{args.source}] {status} backoff {retry}s: "
                f"{body.get('error', {}).get('code')}",
                flush=True,
            )
            throttles += 1
            time.sleep(retry)
    print(
        json.dumps(
            {
                "source": args.source,
                "requests": sent,
                "created": created,
                "failures": failures,
                "holds": throttles,
                "seconds": round(time.monotonic() - start, 2),
            }
        ),
        flush=True,
    )
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
