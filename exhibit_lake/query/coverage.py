"""按本地日历日期观察覆盖情况与输入拒绝趋势。"""
from __future__ import annotations

from datetime import date

from exhibit_lake.catalog.registry import iso_day
from exhibit_lake.catalog.store import Store
from exhibit_lake.errors import InvalidInput, NotFound


class Coverage:
    def __init__(self, store: Store):
        self.store = store

    def day(self, venue_id: str, day: str) -> dict:
        iso_day(day, "local_day")
        with self.store.transaction(write=False) as conn:
            venue = Store.one(conn, "SELECT * FROM venues WHERE venue_id=?", (venue_id,))
            if not venue:
                raise NotFound("venue not found")
            sources = Store.many(conn, "SELECT * FROM sources WHERE venue_id=? ORDER BY source_id", (venue_id,))
            part = Store.one(conn, "SELECT * FROM partitions WHERE venue_id=? AND local_day=?", (venue_id, day))
            source_results = []
            for source in sources:
                accepted = Store.one(conn, "SELECT COUNT(*) AS n,COUNT(DISTINCT batch_id) AS batches "
                                     "FROM accepted_events WHERE source_id=? AND local_day=?", (source["source_id"], day))
                published = Store.one(conn, "SELECT COUNT(*) AS n FROM snapshot_members m "
                                      "JOIN accepted_events e ON e.source_id=m.source_id AND e.event_id=m.event_id "
                                      "WHERE m.snapshot_id=? AND e.source_id=?",
                                      (part["head_id"] if part else "", source["source_id"]))
                contracts = Store.many(conn, "SELECT contract_id,revision,effective_from,effective_to FROM contracts "
                                       "WHERE source_id=? ORDER BY revision", (source["source_id"],))
                rejected = Store.many(conn, "SELECT r.code,COUNT(*) AS n FROM rejected_rows r JOIN batches b "
                                      "ON b.batch_id=r.batch_id WHERE b.source_id=? AND substr(b.received_at,1,10)=? "
                                      "GROUP BY r.code ORDER BY r.code", (source["source_id"], day))
                source_results.append({"source_id": source["source_id"], "kind": source["kind"],
                                       "active": bool(source["active"]), "accepted_events": accepted["n"],
                                       "batches": accepted["batches"], "published_events": published["n"],
                                       "pending_events": accepted["n"] - published["n"], "contracts": contracts,
                                       "rejected_by_receive_day": rejected})
            return {"venue_id": venue_id, "local_day": day,
                    "head_id": part["head_id"] if part else None,
                    "sources": source_results,
                    "pending_events": sum(x["pending_events"] for x in source_results)}

    def range(self, venue_id: str, start_day: str, end_day: str) -> dict:
        iso_day(start_day, "start_day")
        iso_day(end_day, "end_day")
        days = (date.fromisoformat(end_day) - date.fromisoformat(start_day)).days
        if days < 0 or days > 31:
            raise InvalidInput("coverage range must span at most 31 days")
        with self.store.transaction(write=False) as conn:
            if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
                raise NotFound("venue not found")
            rows = Store.many(conn, "SELECT local_day,head_id,generation FROM partitions "
                              "WHERE venue_id=? AND local_day BETWEEN ? AND ? ORDER BY local_day", (venue_id, start_day, end_day))
            ingested = Store.many(conn, "SELECT local_day,COUNT(*) AS n FROM accepted_events "
                                  "WHERE venue_id=? AND local_day BETWEEN ? AND ? GROUP BY local_day ORDER BY local_day",
                                  (venue_id, start_day, end_day))
            counts = {x["local_day"]: x["n"] for x in ingested}
            result = []
            for row in rows:
                published = Store.one(conn, "SELECT COUNT(*) AS n FROM snapshot_members WHERE snapshot_id=?",
                                      (row["head_id"] or "",))["n"]
                result.append({"local_day": row["local_day"], "head_id": row["head_id"],
                               "generation": row["generation"], "accepted": counts.pop(row["local_day"], 0),
                               "published": published})
            for day, n in counts.items():
                result.append({"local_day": day, "head_id": None, "generation": 0, "accepted": n, "published": 0})
            result.sort(key=lambda x: x["local_day"])
            return {"venue_id": venue_id, "start_day": start_day, "end_day": end_day,
                    "days": result, "unpublished_days": [x["local_day"] for x in result if x["accepted"] > x["published"]]}
