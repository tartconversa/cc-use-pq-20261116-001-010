"""基于隔离快照统计访客流量与展品体验。"""
from __future__ import annotations

import statistics
from collections import defaultdict

from exhibit_lake.catalog.registry import iso_day
from exhibit_lake.catalog.store import Store
from exhibit_lake.errors import InvalidInput, NotFound, StateError
from exhibit_lake.publish.snapshots import Snapshots


class Analytics:
    def __init__(self, store: Store, snapshots: Snapshots):
        self.store = store
        self.snapshots = snapshots

    def _resolve(self, conn, venue_id: str, day: str, snapshot_id: str | None) -> dict:
        iso_day(day, "local_day")
        if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
            raise NotFound("venue not found")
        if snapshot_id is None:
            part = Store.one(conn, "SELECT * FROM partitions WHERE venue_id=? AND local_day=?", (venue_id, day))
            if not part or not part["head_id"]:
                raise NotFound("published partition not found")
            snapshot_id = part["head_id"]
        snapshot = Store.one(conn, "SELECT * FROM snapshots WHERE snapshot_id=?", (snapshot_id,))
        if not snapshot or snapshot["venue_id"] != venue_id or snapshot["local_day"] != day:
            raise NotFound("snapshot not found in requested partition")
        if snapshot["status"] != "published":
            raise StateError("snapshot is not published")
        return snapshot

    def _records(self, conn, snapshot_id: str, kind: str | None = None) -> list[dict]:
        sql = "SELECT e.event_id,e.source_id,e.exhibit_id,e.event_at,e.local_day,e.kind,e.value_num,e.category,e.visitor_hash "
        sql += "FROM snapshot_members m JOIN accepted_events e ON e.source_id=m.source_id AND e.event_id=m.event_id "
        sql += "WHERE m.snapshot_id=?"
        args = [snapshot_id]
        if kind:
            sql += " AND e.kind=?"
            args.append(kind)
        sql += " ORDER BY e.event_at,e.source_id,e.event_id"
        return Store.many(conn, sql, tuple(args))

    def day(self, venue_id: str, day: str, snapshot_id: str | None = None) -> dict:
        with self.store.transaction(write=False) as conn:
            snapshot = self._resolve(conn, venue_id, day, snapshot_id)
            records = self._records(conn, snapshot["snapshot_id"])
            exhibits = {x["exhibit_id"]: x for x in Store.many(conn, "SELECT exhibit_id,name,maker,launched_on,withdrawn_on FROM exhibits WHERE venue_id=?", (venue_id,))}
            groups = defaultdict(lambda: {"visit_events": 0, "visitors": 0, "feedback_events": 0,
                                          "rating_sum": 0, "rating_average": None, "demo_events": 0,
                                          "demo_seconds": 0.0, "feedback_visitors": set(), "channels": defaultdict(int)})
            for record in records:
                group = groups[record["exhibit_id"]]
                if record["kind"] == "visit":
                    group["visit_events"] += 1
                    group["visitors"] += int(record["value_num"])
                elif record["kind"] == "feedback":
                    group["feedback_events"] += 1
                    group["rating_sum"] += int(record["value_num"])
                    group["feedback_visitors"].add(record["visitor_hash"])
                else:
                    group["demo_events"] += 1
                    group["demo_seconds"] += record["value_num"]
                if record["category"]:
                    group["channels"][record["category"]] += 1
            result = []
            for exhibit_id in sorted(groups):
                group = groups[exhibit_id]
                feedback = group["feedback_events"]
                result.append({"exhibit_id": exhibit_id, "name": exhibits[exhibit_id]["name"],
                               "maker": exhibits[exhibit_id]["maker"], "visit_events": group["visit_events"],
                               "visitors": group["visitors"], "feedback_events": feedback,
                               "unique_feedback_visitors": len(group["feedback_visitors"]),
                               "rating_average": round(group["rating_sum"] / feedback, 4) if feedback else None,
                               "demo_events": group["demo_events"], "demo_seconds": round(group["demo_seconds"], 3),
                               "channels": dict(sorted(group["channels"].items()))})
            return {"venue_id": venue_id, "local_day": day, "snapshot_id": snapshot["snapshot_id"],
                    "generation": snapshot["generation"], "event_count": len(records), "exhibits": result,
                    "total_visitors": sum(x["visitors"] for x in result),
                    "total_feedback": sum(x["feedback_events"] for x in result),
                    "total_demos": sum(x["demo_events"] for x in result)}

    def events(self, venue_id: str, day: str, snapshot_id: str | None = None,
               *, exhibit_id: str | None = None, kind: str | None = None,
               offset: int = 0, limit: int = 100) -> dict:
        if kind not in (None, "visit", "feedback", "demo"):
            raise InvalidInput("invalid event kind")
        if offset < 0 or not 1 <= limit <= 500:
            raise InvalidInput("invalid page")
        with self.store.transaction(write=False) as conn:
            snapshot = self._resolve(conn, venue_id, day, snapshot_id)
            records = self._records(conn, snapshot["snapshot_id"], kind)
            if exhibit_id:
                records = [x for x in records if x["exhibit_id"] == exhibit_id]
            return {"snapshot_id": snapshot["snapshot_id"], "total": len(records),
                    "offset": offset, "limit": limit, "events": records[offset:offset + limit]}

    def compare(self, venue_id: str, day: str, left_id: str, right_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            left = self._resolve(conn, venue_id, day, left_id)
            right = self._resolve(conn, venue_id, day, right_id)
            left_set = {(x["source_id"], x["event_id"]) for x in self._records(conn, left_id)}
            right_set = {(x["source_id"], x["event_id"]) for x in self._records(conn, right_id)}
            return {"venue_id": venue_id, "local_day": day, "left_id": left_id, "right_id": right_id,
                    "left_generation": left["generation"], "right_generation": right["generation"],
                    "added": [{"source_id": s, "event_id": e} for s, e in sorted(right_set - left_set)],
                    "removed": [{"source_id": s, "event_id": e} for s, e in sorted(left_set - right_set)],
                    "unchanged": len(left_set & right_set)}

    def trend(self, venue_id: str, start_day: str, end_day: str) -> dict:
        iso_day(start_day, "start_day")
        iso_day(end_day, "end_day")
        if end_day < start_day:
            raise InvalidInput("end day precedes start day")
        with self.store.transaction(write=False) as conn:
            if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
                raise NotFound("venue not found")
            heads = Store.many(conn, "SELECT local_day,head_id,generation FROM partitions WHERE venue_id=? AND local_day BETWEEN ? AND ? "
                               "AND head_id IS NOT NULL ORDER BY local_day", (venue_id, start_day, end_day))
            rows = []
            for head in heads:
                events = self._records(conn, head["head_id"])
                ratings = [x["value_num"] for x in events if x["kind"] == "feedback"]
                rows.append({"local_day": head["local_day"], "snapshot_id": head["head_id"],
                             "generation": head["generation"], "events": len(events),
                             "visitors": sum(int(x["value_num"]) for x in events if x["kind"] == "visit"),
                             "feedback": len(ratings), "rating_average": round(statistics.mean(ratings), 4) if ratings else None,
                             "demos": sum(x["kind"] == "demo" for x in events)})
            return {"venue_id": venue_id, "start_day": start_day, "end_day": end_day, "days": rows}

    def lineage(self, snapshot_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            snapshot = Store.one(conn, "SELECT * FROM snapshots WHERE snapshot_id=?", (snapshot_id,))
            if not snapshot:
                raise NotFound("snapshot not found")
            batches = Store.many(conn, "SELECT b.batch_id,b.source_id,b.contract_id,COUNT(*) AS accepted_rows "
                                 "FROM snapshot_members m JOIN accepted_events e ON e.source_id=m.source_id AND e.event_id=m.event_id "
                                 "JOIN batches b ON b.batch_id=e.batch_id WHERE m.snapshot_id=? "
                                 "GROUP BY b.batch_id,b.source_id,b.contract_id ORDER BY b.source_id,b.batch_id", (snapshot_id,))
            contracts = Store.many(conn, "SELECT DISTINCT c.contract_id,c.revision,c.kind,c.effective_from,c.effective_to "
                                   "FROM snapshot_members m JOIN accepted_events e ON e.source_id=m.source_id AND e.event_id=m.event_id "
                                   "JOIN batches b ON b.batch_id=e.batch_id JOIN contracts c ON c.contract_id=b.contract_id "
                                   "WHERE m.snapshot_id=? ORDER BY c.contract_id", (snapshot_id,))
            return {"snapshot_id": snapshot_id, "parent_id": snapshot["parent_id"], "digest": snapshot["digest"],
                    "batches": batches, "contracts": contracts}
