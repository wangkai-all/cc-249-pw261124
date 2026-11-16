"""Batch parsing and validation.

Two deliberately separate layers:

* **Framing validation** (strict, cheap, always rejects with HTTP 422):
  JSON shape, required fields, source/batch id syntax, duplicate record ids.
* **Record validation** (quarantine, never rejects the whole source): each
  record must be an object with a string ``id`` and a ``value`` that is a
  JSON scalar.  A batch with any invalid record is durably staged first and
  then quarantined, so a poisoned producer cannot wedge the single indexer
  queue and starve every other source.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

from .config import BATCH_ID_RE, DOC_ID_RE, SOURCE_ID_RE


class Reject(Exception):
    """Request-level rejection with an HTTP status and stable machine code."""

    def __init__(self, status: int, code: str, message: str, **extra: Any):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.extra = extra

    def body(self) -> dict[str, Any]:
        return {"ok": False, "error": {"code": self.code, "message": self.message, **self.extra}}


@dataclass
class ParsedBatch:
    source_id: str
    batch_id: str
    records: list[dict[str, Any]]
    content: bytes
    content_sha256: str
    invalid: list[dict[str, Any]] = field(default_factory=list)

    @property
    def record_count(self) -> int:
        return len(self.records)


def _require_pair(fields: dict[str, Any], key: str) -> tuple[Any, Any]:
    value = fields.get(key)
    if not isinstance(value, list) or len(value) != 2 or not isinstance(value[0], str):
        raise Reject(422, "bad_field", f"{key} must be a [header, value] pair")
    return value[0], value[1]


def parse_batch(
    body: bytes,
    *,
    header_source: str | None,
    path_source: str | None,
    header_batch: str | None,
    path_batch: str | None,
    max_batch_bytes: int,
    max_batch_records: int,
    max_record_bytes: int,
    allowed_source: str | None = None,
) -> ParsedBatch:
    import json

    if len(body) > max_batch_bytes:
        raise Reject(
            413,
            "batch_too_large",
            f"batch is {len(body)} bytes; limit is {max_batch_bytes}",
            limit=max_batch_bytes,
        )
    try:
        doc = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Reject(422, "invalid_json", f"body is not valid UTF-8 JSON: {exc}") from exc
    if not isinstance(doc, dict):
        raise Reject(422, "invalid_json", "body must be a JSON object")
    if doc.get("format") not in (None, "pointwise-receiver/v1"):
        raise Reject(422, "unsupported_format", "format must be pointwise-receiver/v1")

    source_id = doc.get("source_id") or header_source or path_source
    batch_id = doc.get("batch_id") or header_batch or path_batch
    if not isinstance(source_id, str) or not SOURCE_ID_RE.match(source_id):
        raise Reject(422, "invalid_source_id", "source_id is missing or malformed")
    if allowed_source is not None and source_id != allowed_source:
        raise Reject(
            409, "source_mismatch", "source_id does not match the request path"
        )
    if not isinstance(batch_id, str) or not BATCH_ID_RE.match(batch_id):
        raise Reject(422, "invalid_batch_id", "batch_id is missing or malformed")

    records = doc.get("records")
    if not isinstance(records, list):
        raise Reject(422, "invalid_records", "records must be a JSON array")
    if len(records) > max_batch_records:
        raise Reject(
            422,
            "too_many_records",
            f"batch has {len(records)} records; limit is {max_batch_records}",
            limit=max_batch_records,
        )

    invalid: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, record in enumerate(records):
        problem = _validate_record(record, max_record_bytes)
        if problem is not None:
            invalid.append({"index": index, "problem": problem})
            continue
        if record["id"] in seen:
            invalid.append(
                {"index": index, "problem": f"duplicate record id {record['id']!r}"}
            )
            continue
        seen.add(record["id"])

    if doc.get("record_count") is not None:
        declared = doc["record_count"]
        if not isinstance(declared, int) or declared != len(records):
            raise Reject(
                422,
                "record_count_mismatch",
                f"record_count {declared!r} does not equal records length {len(records)}",
            )
    digest = doc.get("content_sha256")
    actual = hashlib.sha256(body).hexdigest()
    if digest is not None and digest != actual:
        # Producers cannot hash the envelope that carries the hash itself, so
        # the declared value is checked against the canonical document with
        # the ``content_sha256`` field removed.  Re-serialisation makes the
        # check independent of wire whitespace and key order.
        from .config import canonical_json

        declared_body = {k: v for k, v in doc.items() if k != "content_sha256"}
        canonical_actual = hashlib.sha256(canonical_json(declared_body)).hexdigest()
        if digest != canonical_actual:
            raise Reject(
                422,
                "content_sha256_mismatch",
                "declared content_sha256 does not match the body",
                declared=digest,
                actual=canonical_actual,
            )

    return ParsedBatch(
        source_id=source_id,
        batch_id=batch_id,
        records=records,
        content=body,
        content_sha256=actual,
        invalid=invalid,
    )


def _validate_record(record: Any, max_record_bytes: int) -> str | None:
    import json

    if not isinstance(record, dict):
        return "record must be a JSON object"
    record_id = record.get("id")
    if not isinstance(record_id, str) or not DOC_ID_RE.match(record_id):
        return "id must be a non-empty [A-Za-z0-9][A-Za-z0-9_.:/-]* string"
    if "value" not in record:
        return "value is required"
    value = record["value"]
    if isinstance(value, (dict, list)):
        return "value must be a JSON scalar (string, number, boolean or null)"
    if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
        return "value must be finite"
    try:
        encoded = json.dumps(value, ensure_ascii=False).encode("utf-8")
    except (TypeError, ValueError):
        return "value is not JSON serialisable"
    if len(encoded) > max_record_bytes:
        return f"value is {len(encoded)} bytes; limit is {max_record_bytes}"
    return None
