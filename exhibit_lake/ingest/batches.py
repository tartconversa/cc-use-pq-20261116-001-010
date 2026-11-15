"""事件批次的不可变原始落地和幂等接收。"""
from __future__ import annotations

import json
import sqlite3
import uuid

from exhibit_lake.catalog.registry import now, require_id
from exhibit_lake.catalog.store import Store
from exhibit_lake.errors import Conflict, InvalidInput, NotFound
from exhibit_lake.ingest.validation import canonical, digest, event


class Batches:
    def __init__(self, store: Store):
        self.store = store

    def ingest(self, source_id: str, contract_id: str, request_key: str, rows: list[object]) -> dict:
        require_id(source_id, "source_id")
        require_id(contract_id, "contract_id")
        require_id(request_key, "request_key")
        if not isinstance(rows, list) or not 1 <= len(rows) <= 5000:
            raise InvalidInput("batch must contain between 1 and 5000 events")
        try:
            payload_hash = digest(rows)
        except (TypeError, ValueError):
            raise InvalidInput("batch must contain JSON-compatible finite values") from None
        with self.store.transaction() as conn:
            previous = Store.one(conn, "SELECT * FROM batches WHERE source_id=? AND request_key=?", (source_id, request_key))
            if previous:
                if previous["payload_hash"] != payload_hash or previous["contract_id"] != contract_id:
                    raise Conflict("request key already used for different content")
                return self._report(conn, previous["batch_id"], replay=True)
            source = Store.one(conn, "SELECT * FROM sources WHERE source_id=?", (source_id,))
            if not source:
                raise NotFound("source not found")
            if not source["active"]:
                raise Conflict("source inactive")
            contract = Store.one(conn, "SELECT * FROM contracts WHERE contract_id=? AND source_id=?", (contract_id, source_id))
            if not contract:
                raise NotFound("contract not found for source")
            venue = Store.one(conn, "SELECT * FROM venues WHERE venue_id=?", (source["venue_id"],))
            exhibits = {x["exhibit_id"]: x for x in Store.many(conn, "SELECT * FROM exhibits WHERE venue_id=?", (source["venue_id"],))}
            batch_id = uuid.uuid4().hex
            conn.execute("INSERT INTO batches VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                         (batch_id, source_id, request_key, payload_hash, contract_id, "landing", now(), None, len(rows), 0, 0))
            valid = 0
            rejected = 0
            for row_no, row in enumerate(rows, 1):
                body = canonical(row)
                row_hash = digest(row)
                conn.execute("INSERT INTO raw_rows VALUES(?,?,?,?)", (batch_id, row_no, body, row_hash))
                normalized, problem = event(row, contract, source, venue, exhibits)
                if normalized is not None:
                    existing = Store.one(conn, "SELECT payload_hash FROM accepted_events WHERE source_id=? AND event_id=?",
                                         (source_id, normalized["event_id"]))
                    if existing:
                        problem = {"code": "duplicate_event" if existing["payload_hash"] == row_hash else "event_conflict",
                                   "detail": "event identity was already accepted"}
                    else:
                        conn.execute("INSERT INTO accepted_events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                     (normalized["event_id"], source_id, batch_id, row_no, normalized["venue_id"],
                                      normalized["exhibit_id"], normalized["event_at"], normalized["local_day"],
                                      normalized["kind"], normalized["value_num"], normalized["category"],
                                      normalized["visitor_hash"], normalized["payload_hash"], None))
                        valid += 1
                if problem is not None:
                    conn.execute("INSERT INTO rejected_rows VALUES(?,?,?,?)",
                                 (batch_id, row_no, problem["code"], problem["detail"]))
                    rejected += 1
            conn.execute("UPDATE batches SET status='committed',committed_at=?,valid_rows=?,rejected_rows=? WHERE batch_id=?",
                         (now(), valid, rejected, batch_id))
            Store.audit(conn, "batch.committed", batch_id, {"source_id": source_id, "valid": valid, "rejected": rejected}, now())
            return self._report(conn, batch_id, replay=False)

    def _report(self, conn: sqlite3.Connection, batch_id: str, *, replay: bool) -> dict:
        batch = Store.one(conn, "SELECT * FROM batches WHERE batch_id=?", (batch_id,))
        rejected = Store.many(conn, "SELECT row_no,code,detail FROM rejected_rows WHERE batch_id=? ORDER BY row_no", (batch_id,))
        return {"batch_id": batch_id, "source_id": batch["source_id"], "request_key": batch["request_key"],
                "status": batch["status"], "total_rows": batch["total_rows"], "valid_rows": batch["valid_rows"],
                "rejected_rows": batch["rejected_rows"], "rejections": rejected, "replay": replay}

    def report(self, batch_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            if not Store.one(conn, "SELECT batch_id FROM batches WHERE batch_id=?", (batch_id,)):
                raise NotFound("batch not found")
            return self._report(conn, batch_id, replay=False)

    def raw(self, batch_id: str, *, offset: int = 0, limit: int = 100) -> dict:
        if offset < 0 or not 1 <= limit <= 500:
            raise InvalidInput("invalid raw row page")
        with self.store.transaction(write=False) as conn:
            if not Store.one(conn, "SELECT batch_id FROM batches WHERE batch_id=?", (batch_id,)):
                raise NotFound("batch not found")
            rows = Store.many(conn, "SELECT row_no,payload_json,payload_hash FROM raw_rows WHERE batch_id=? ORDER BY row_no LIMIT ? OFFSET ?",
                              (batch_id, limit, offset))
        return {"batch_id": batch_id, "rows": [{"row_no": x["row_no"], "payload": json.loads(x["payload_json"]),
                                                 "payload_hash": x["payload_hash"]} for x in rows]}
