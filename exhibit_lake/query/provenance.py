"""将已发布观测追溯到不可变原始行和契约。"""
from __future__ import annotations

import json

from exhibit_lake.catalog.store import Store
from exhibit_lake.errors import NotFound


class Provenance:
    def __init__(self, store: Store):
        self.store = store

    def event(self, source_id: str, event_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            event = Store.one(conn, "SELECT * FROM accepted_events WHERE source_id=? AND event_id=?",
                              (source_id, event_id))
            if not event:
                raise NotFound("accepted event not found")
            raw = Store.one(conn, "SELECT payload_json,payload_hash FROM raw_rows WHERE batch_id=? AND row_no=?",
                            (event["batch_id"], event["row_no"]))
            batch = Store.one(conn, "SELECT * FROM batches WHERE batch_id=?", (event["batch_id"],))
            contract = Store.one(conn, "SELECT contract_id,revision,fields_json,effective_from,effective_to "
                                 "FROM contracts WHERE contract_id=?", (batch["contract_id"],))
            publications = Store.many(conn, "SELECT s.snapshot_id,s.generation,s.local_day,s.status,s.published_at "
                                      "FROM snapshot_members m JOIN snapshots s ON s.snapshot_id=m.snapshot_id "
                                      "WHERE m.source_id=? AND m.event_id=? ORDER BY s.local_day,s.generation",
                                      (source_id, event_id))
            return {"event": event, "raw": json.loads(raw["payload_json"]),
                    "raw_hash": raw["payload_hash"], "batch": batch,
                    "contract": {**contract, "fields": json.loads(contract.pop("fields_json"))},
                    "snapshots": publications}

    def batch(self, batch_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            batch = Store.one(conn, "SELECT * FROM batches WHERE batch_id=?", (batch_id,))
            if not batch:
                raise NotFound("batch not found")
            source = Store.one(conn, "SELECT * FROM sources WHERE source_id=?", (batch["source_id"],))
            contract = Store.one(conn, "SELECT * FROM contracts WHERE contract_id=?", (batch["contract_id"],))
            counts = Store.many(conn, "SELECT local_day,COUNT(*) AS events FROM accepted_events WHERE batch_id=? "
                                "GROUP BY local_day ORDER BY local_day", (batch_id,))
            rejection_codes = Store.many(conn, "SELECT code,COUNT(*) AS rows FROM rejected_rows WHERE batch_id=? "
                                         "GROUP BY code ORDER BY code", (batch_id,))
            snapshots = Store.many(conn, "SELECT DISTINCT s.snapshot_id,s.local_day,s.generation,s.status "
                                   "FROM accepted_events e JOIN snapshot_members m ON m.source_id=e.source_id AND m.event_id=e.event_id "
                                   "JOIN snapshots s ON s.snapshot_id=m.snapshot_id WHERE e.batch_id=? "
                                   "ORDER BY s.local_day,s.generation", (batch_id,))
            return {"batch": batch, "source": source, "contract_id": contract["contract_id"],
                    "contract_revision": contract["revision"], "accepted_by_day": counts,
                    "rejected_by_code": rejection_codes, "snapshots": snapshots}
