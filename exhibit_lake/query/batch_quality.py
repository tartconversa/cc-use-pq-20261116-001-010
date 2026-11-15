"""比较原始来源质量与当前已发布的每日视图。"""
from __future__ import annotations

from collections import defaultdict

from exhibit_lake.catalog.registry import iso_day
from exhibit_lake.catalog.store import Store
from exhibit_lake.errors import NotFound


class BatchQuality:
    def __init__(self, store: Store):
        self.store = store

    def source(self, source_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            source = Store.one(conn, "SELECT * FROM sources WHERE source_id=?", (source_id,))
            if not source:
                raise NotFound("source not found")
            batches = Store.many(conn, "SELECT batch_id,request_key,contract_id,status,received_at,total_rows,valid_rows,rejected_rows "
                                 "FROM batches WHERE source_id=? ORDER BY received_at,batch_id", (source_id,))
            codes = Store.many(conn, "SELECT r.code,COUNT(*) AS rows FROM rejected_rows r JOIN batches b ON b.batch_id=r.batch_id "
                               "WHERE b.source_id=? GROUP BY r.code ORDER BY r.code", (source_id,))
            totals = {"batches": len(batches), "raw_rows": sum(x["total_rows"] for x in batches),
                      "accepted": sum(x["valid_rows"] for x in batches),
                      "rejected": sum(x["rejected_rows"] for x in batches)}
            return {"source_id": source_id, "venue_id": source["venue_id"], "kind": source["kind"],
                    "totals": totals, "rejection_codes": codes, "batches": batches}

    def partition(self, venue_id: str, day: str) -> dict:
        iso_day(day, "local_day")
        with self.store.transaction(write=False) as conn:
            if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
                raise NotFound("venue not found")
            part = Store.one(conn, "SELECT * FROM partitions WHERE venue_id=? AND local_day=?", (venue_id, day))
            rows = Store.many(conn, "SELECT s.source_id,s.kind,e.batch_id,COUNT(*) AS events "
                              "FROM accepted_events e JOIN sources s ON s.source_id=e.source_id "
                              "WHERE e.venue_id=? AND e.local_day=? GROUP BY s.source_id,s.kind,e.batch_id "
                              "ORDER BY s.source_id,e.batch_id", (venue_id, day))
            groups = defaultdict(lambda: {"accepted": 0, "published": 0, "batches": []})
            for row in rows:
                group = groups[row["source_id"]]
                group["accepted"] += row["events"]
                group["batches"].append({"batch_id": row["batch_id"], "accepted": row["events"]})
            published = Store.many(conn, "SELECT e.source_id,COUNT(*) AS n FROM snapshot_members m "
                                   "JOIN accepted_events e ON e.source_id=m.source_id AND e.event_id=m.event_id "
                                   "WHERE m.snapshot_id=? GROUP BY e.source_id", (part["head_id"] if part else "",))
            for row in published:
                groups[row["source_id"]]["published"] = row["n"]
            return {"venue_id": venue_id, "local_day": day, "head_id": part["head_id"] if part else None,
                    "sources": [{"source_id": source_id, **group,
                                 "pending": group["accepted"] - group["published"]}
                                for source_id, group in sorted(groups.items())]}
