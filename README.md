# Pointwise receiver

A local, credit-driven batch receiver with tiered storage and an operations
page.  Everything runs from the Python standard library; no external services
are required.

```
sources ──credit──▶ API process ──▶ spool (fsynced bytes + SQLite row)
                        │                        │
                        │  ack: spool-durable    ▼
                        │                  worker process
                        │                  validate → hot file → index rows
                        ▼                        │
                   dashboard                     ▼  ack: indexed
                   / (html)                 hot/ then cold/ segments
```

The API and the worker are **separate OS processes**.  The API never indexes,
and the worker is not needed for the API to accept or refuse work.  This is
what lets you slow or kill indexing from the dashboard without losing
acknowledgements.

## Running it

```bash
# API + supervised worker + dashboard on http://127.0.0.1:8787/
python3 -m receiver.server --data-dir ./data --port 8787 --with-worker

# In another terminal, push batches under the advertised credit window:
python3 scripts/source.py --source alpha --count 200 --rate 20
python3 scripts/source.py --source beta  --count 200 --rate 20
```

`alpha` and `beta` are seeded sources.  Register more from the dashboard or
with `POST /api/sources/<id>`.

Without `--with-worker`, the API refuses nothing but nothing drains either;
start the worker from the dashboard ("Start worker") or directly:

```bash
python3 -m receiver.worker --data-dir ./data
```

Run the tests (they spawn real API and worker processes and SIGKILL them):

```bash
cd tests
python3 -m unittest test_ingress test_crash test_storage test_backpressure
```

## The acknowledgement levels

A confirmation names exactly one layer, and the response says which:

| level | HTTP | means | survives a crash of |
|---|---|---|---|
| `spool-durable` | 202 | request bytes are fsynced in `spool/` **and** a metadata row naming them is committed | the API, immediately after the ack |
| `indexed` | 200 (only with `X-Wait-Indexed: 1`) | the hot file is fsynced and the `record_index` rows are committed in the same SQLite transaction; the records are queryable | the worker, immediately after the ack |
| `quarantined` | 422 | invalid content was stored and recorded but will never be indexed | — |

The ordering that makes these true:

1. write to `tmp/`, `fsync(file)`, `os.replace`, `fsync(dir)`;
2. commit the metadata row (SQLite WAL, `synchronous=FULL`, `BEGIN IMMEDIATE`);
3. only then send the HTTP response.

`X-Ack-Level` repeats the level in a header so a proxy or client can assert it.

**Crash between confirmation and persistence** is tested, not assumed:
`tests/test_crash.py` kills a real process with `os._exit` or `SIGKILL` at
`after-spool-write`, `after-hot-write` and `after-cold-write`
(`POINTWISE_CRASH_AFTER`), then restarts and checks what the sender was told:
no ack means no row (retry is a single copy); an `indexed` ack means the index
rows and hot payload both survive; a crash mid-index leaves the batch in
`indexing`, and the restarted worker replays it through idempotent upserts; a
crash after a cold write leaves the hot copy authoritative because the cold
pointer was never committed.

## Credit: when to reduce, when to refuse

Each source has a **reservation** (`reserved_batches` / `reserved_bytes`).
The sum of reservations is kept under the spool cap, so one source cannot
consume another's guaranteed room.  The rest of the cap is **slack**,
borrowable while the spool is below the waterline.

| state | sender sees | receiver does |
|---|---|---|
| normal | reservation + slack | admit within credit |
| soft waterline (`soft_limit_pct` × cap) | reservation only, `reason=soft_limit_reserved_only` | stop granting borrowed credit; reserved sends still proceed |
| index lag (`backlog_reduce_secs`) | reservation only, `reason=index_lag_reduce` | withdraw borrowing early, before the spool fills, while the oldest live batch is older than the threshold |
| hard cap / batch cap / free-disk floor | `429` + `Retry-After`, credit 0 | write nothing; no spool file, no row |
| reservation used up | `credit_batches=0`, `reason=reservation_exhausted` | refuse this source only |

Credit is recomputed from committed rows on every request, so an API restart
cannot over-grant.  The admission transaction re-checks the same budgets, so a
sender with a stale credit view still cannot overfill the spool.  This is the
bound that keeps a stalled indexer from filling the disk: the spool tops out
at `spool_cap_bytes`, no matter how long indexing is paused.

Duplicate handling happens **before** the credit gate: a retry of an already
accepted batch returns the existing result (200, `duplicate: true`) even when
the spool is full or the source is out of credit.  A reused batch id with
different bytes is a 409.  That is what lets a sender recover from an
ambiguous timeout without being punished by backpressure.

The worker never refuses its own queue, because indexing is what drains the
spool; backpressure exists only between senders and the API.

## Validation and quarantine

Framing problems (bad JSON, missing/duplicate ids, oversized batches) are
rejected with 422 and nothing is stored.  Record-level problems (non-scalar
value, oversized record) are stored in quarantine with `ack.level =
quarantined`: the batch is excluded from the worker path and does not consume
sender credit, so one poisoned producer cannot wedge the single indexer.
Quarantine itself is bounded (`quarantine_max_batches` / `_bytes`); when full,
further bad batches get 429 until an operator requeues or discards them from
the dashboard.

A requeued batch is re-validated when it is actually indexed, and if it is
still bad it goes back to quarantine.  The API reports what happened rather
than claiming success.

## Tiered storage and integrity

* **spool/** — raw request bodies, deleted once the batch is `indexed`.
* **hot/** — one canonical JSON file per batch, written by the worker and
  fsynced before the index commit.
* **cold/** — gzip segments holding batches older than `hot_window_batches`.
  Promotion copies and fsyncs the segment, commits the pointer, *then*
  unlinks the hot files.  Compaction merges segments above
  `cold_compact_threshold`.

`POST /api/control/integrity` re-reads hot and cold payloads and checks file
digests, record digests, record counts and the `record_index` row count
against the batch metadata.  The dashboard shows the report, including a list
of issues.  It reads real files; it is not a summary of counters.

## Dashboard

`http://127.0.0.1:8787/` polls `/api/status` (the SQLite state and worker
samples) and offers:

* sliders for `index_delay_ms`, `storage_delay_ms` and `spool_cap_bytes`
  (real settings rows, read by the worker loop);
* **Stop worker (SIGKILL)** / **Start worker** — real process control, so you
  can watch backlog grow against the cap and shrink again on recovery;
* **Send bad batch** — re-enters the real `/ingest` validation path in-process
  with invalid records, so the result cannot be faked;
* **Run integrity check**, source pause/resume, and per-quarantine-batch
  requeue;
* backlog, spool meter with the soft/hard waterlines marked, disk and DB
  usage, per-source credit and usage, hot/cold tier counts, recent indexed
  records and the event log.

## API quick reference

```
POST /ingest/<source>            accept a batch (X-Wait-Indexed: 1 to wait)
GET  /api/credit/<source>        current credit window and its reason
GET  /api/status                 everything the dashboard shows
GET  /api/batches                pending + recent batches
GET  /api/records?source=&q=     indexed records
POST /api/settings               {setting: value} validated against SETTINGS
POST /api/sources/<id>           {reserved_batches, reserved_bytes, active, ...}
POST /api/control/worker         {"action": "start"|"stop"|"kill"|"status"}
POST /api/control/integrity      run an integrity check
POST /api/control/inject-bad     send a genuinely invalid batch
POST /api/batches/<seq>/requeue  re-admit a quarantined batch
POST /api/batches/<seq>/discard  delete a quarantined batch
GET  /api/events  /api/logs  /healthz
```

Settings are listed with their bounds in `receiver/config.py`.  A source's
`max_batch_bytes` is an upper bound, not the size the receiver expects; senders
should split work to fit `credit_bytes`.
