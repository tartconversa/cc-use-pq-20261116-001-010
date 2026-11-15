"""不修改状态的来源批次及事件标识冲突预检。"""
from __future__ import annotations

from collections import Counter

from exhibit_lake.catalog.registry import require_id
from exhibit_lake.catalog.store import Store
from exhibit_lake.errors import Conflict, InvalidInput, NotFound
from exhibit_lake.ingest.validation import digest, event


class Preview:
    def __init__(self, store: Store):
        self.store = store

    def inspect(self, source_id: str, contract_id: str, rows: list[object]) -> dict:
        require_id(source_id, "source_id")
        require_id(contract_id, "contract_id")
        if not isinstance(rows, list) or not 1 <= len(rows) <= 5000:
            raise InvalidInput("batch must contain between 1 and 5000 events")
        try:
            batch_hash = digest(rows)
        except (TypeError, ValueError):
            raise InvalidInput("batch must contain JSON-compatible finite values") from None
        with self.store.transaction(write=False) as conn:
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
            seen = {}
            outcomes = []
            partitions = Counter()
            for row_no, row in enumerate(rows, 1):
                normalized, problem = event(row, contract, source, venue, exhibits)
                if normalized:
                    identity = normalized["event_id"]
                    if identity in seen:
                        problem = {"code": "duplicate_in_batch", "detail": f"same event identity as row {seen[identity]}"}
                    else:
                        seen[identity] = row_no
                        previous = Store.one(conn, "SELECT payload_hash FROM accepted_events WHERE source_id=? AND event_id=?",
                                             (source_id, identity))
                        if previous:
                            problem = {"code": "duplicate_event" if previous["payload_hash"] == normalized["payload_hash"] else "event_conflict",
                                       "detail": "event identity was already accepted"}
                if problem:
                    outcomes.append({"row_no": row_no, "accepted": False, **problem})
                else:
                    partitions[normalized["local_day"]] += 1
                    outcomes.append({"row_no": row_no, "accepted": True, "event_id": normalized["event_id"],
                                     "local_day": normalized["local_day"], "exhibit_id": normalized["exhibit_id"]})
        return {"source_id": source_id, "contract_id": contract_id, "payload_hash": batch_hash,
                "total_rows": len(rows), "would_accept": sum(x["accepted"] for x in outcomes),
                "would_reject": sum(not x["accepted"] for x in outcomes),
                "partitions": dict(sorted(partitions.items())), "rows": outcomes}
