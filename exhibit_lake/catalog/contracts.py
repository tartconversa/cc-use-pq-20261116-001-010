"""检查契约时间线并按事件时间解析修订版本。"""
from __future__ import annotations

import json

from exhibit_lake.catalog.registry import iso_time
from exhibit_lake.catalog.store import Store
from exhibit_lake.errors import Conflict, NotFound


class Contracts:
    def __init__(self, store: Store):
        self.store = store

    def describe(self, contract_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            contract = Store.one(conn, "SELECT * FROM contracts WHERE contract_id=?", (contract_id,))
            if not contract:
                raise NotFound("contract not found")
            source = Store.one(conn, "SELECT source_id,venue_id,kind,active FROM sources WHERE source_id=?",
                               (contract["source_id"],))
            accepted = Store.one(conn, "SELECT COUNT(*) AS n FROM accepted_events e JOIN batches b ON b.batch_id=e.batch_id "
                                 "WHERE b.contract_id=?", (contract_id,))["n"]
            rejected = Store.one(conn, "SELECT COUNT(*) AS n FROM rejected_rows r JOIN batches b ON b.batch_id=r.batch_id "
                                 "WHERE b.contract_id=?", (contract_id,))["n"]
            contract["fields"] = json.loads(contract.pop("fields_json"))
            return {"contract": contract, "source": source, "accepted_events": accepted,
                    "rejected_rows": rejected}

    def timeline(self, source_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            source = Store.one(conn, "SELECT * FROM sources WHERE source_id=?", (source_id,))
            if not source:
                raise NotFound("source not found")
            rows = Store.many(conn, "SELECT * FROM contracts WHERE source_id=? ORDER BY effective_from,revision",
                              (source_id,))
            for row in rows:
                row["fields"] = json.loads(row.pop("fields_json"))
            return {"source": source, "revisions": rows}

    def resolve(self, source_id: str, event_at: str) -> dict:
        stamp = iso_time(event_at, "event_at")
        with self.store.transaction(write=False) as conn:
            source = Store.one(conn, "SELECT * FROM sources WHERE source_id=?", (source_id,))
            if not source:
                raise NotFound("source not found")
            rows = Store.many(conn, "SELECT * FROM contracts WHERE source_id=? AND effective_from<=? "
                              "AND (effective_to IS NULL OR effective_to>?) ORDER BY revision", (source_id, stamp, stamp))
            if len(rows) > 1:
                raise Conflict("ambiguous contract timeline")
            if not rows:
                raise NotFound("no contract covers event time")
            row = rows[0]
            row["fields"] = json.loads(row.pop("fields_json"))
            return row

    def compatibility(self, left_id: str, right_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            left = Store.one(conn, "SELECT * FROM contracts WHERE contract_id=?", (left_id,))
            right = Store.one(conn, "SELECT * FROM contracts WHERE contract_id=?", (right_id,))
            if not left or not right:
                raise NotFound("contract not found")
            if left["source_id"] != right["source_id"]:
                raise Conflict("contracts belong to different sources")
            old = set(json.loads(left["fields_json"]))
            new = set(json.loads(right["fields_json"]))
            return {"source_id": left["source_id"], "left_id": left_id, "right_id": right_id,
                    "left_revision": left["revision"], "right_revision": right["revision"],
                    "added_fields": sorted(new - old), "removed_fields": sorted(old - new),
                    "same_schema": old == new}
