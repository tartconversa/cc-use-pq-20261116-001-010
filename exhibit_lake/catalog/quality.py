"""入湖及每日发布前的场馆级来源和契约检查。"""
from __future__ import annotations

import json

from exhibit_lake.catalog.registry import iso_day
from exhibit_lake.catalog.store import Store
from exhibit_lake.errors import NotFound


class Quality:
    def __init__(self, store: Store):
        self.store = store

    def sources(self, venue_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            venue = Store.one(conn, "SELECT * FROM venues WHERE venue_id=?", (venue_id,))
            if not venue:
                raise NotFound("venue not found")
            sources = Store.many(conn, "SELECT * FROM sources WHERE venue_id=? ORDER BY source_id", (venue_id,))
            result = []
            for source in sources:
                contracts = Store.many(conn, "SELECT * FROM contracts WHERE source_id=? ORDER BY revision",
                                       (source["source_id"],))
                gaps = []
                for earlier, later in zip(contracts, contracts[1:]):
                    if earlier["effective_to"] and earlier["effective_to"] < later["effective_from"]:
                        gaps.append({"from": earlier["effective_to"], "to": later["effective_from"]})
                    elif earlier["effective_to"] is None:
                        gaps.append({"from": earlier["effective_from"], "to": later["effective_from"],
                                     "warning": "open interval followed by another revision"})
                latest = contracts[-1] if contracts else None
                result.append({"source_id": source["source_id"], "kind": source["kind"],
                               "active": bool(source["active"]), "contract_count": len(contracts),
                               "latest_revision": latest["revision"] if latest else None,
                               "latest_fields": json.loads(latest["fields_json"]) if latest else None,
                               "gaps": gaps, "ready": bool(source["active"] and contracts)})
            return {"venue_id": venue_id, "timezone": venue["timezone"], "sources": result,
                    "ready_sources": sum(x["ready"] for x in result)}

    def partition(self, venue_id: str, day: str) -> dict:
        iso_day(day, "local_day")
        with self.store.transaction(write=False) as conn:
            if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
                raise NotFound("venue not found")
            exhibits = Store.many(conn, "SELECT exhibit_id,name,launched_on,withdrawn_on FROM exhibits "
                                  "WHERE venue_id=? ORDER BY exhibit_id", (venue_id,))
            available = [x for x in exhibits if x["launched_on"] <= day and (not x["withdrawn_on"] or day <= x["withdrawn_on"])]
            source_rows = Store.many(conn, "SELECT s.source_id,s.kind,s.active,COUNT(c.contract_id) AS contracts "
                                     "FROM sources s LEFT JOIN contracts c ON c.source_id=s.source_id "
                                     "WHERE s.venue_id=? GROUP BY s.source_id,s.kind,s.active ORDER BY s.source_id", (venue_id,))
            pending = Store.one(conn, "SELECT COUNT(*) AS n FROM accepted_events e WHERE e.venue_id=? AND e.local_day=? "
                                "AND e.published_at IS NULL", (venue_id, day))["n"]
            rejected = Store.many(conn, "SELECT r.code,COUNT(*) AS n FROM rejected_rows r JOIN batches b ON b.batch_id=r.batch_id "
                                  "JOIN sources s ON s.source_id=b.source_id WHERE s.venue_id=? AND substr(b.received_at,1,10)=? "
                                  "GROUP BY r.code ORDER BY r.code", (venue_id, day))
            return {"venue_id": venue_id, "local_day": day, "exhibits_available": len(available),
                    "available_exhibit_ids": [x["exhibit_id"] for x in available],
                    "pending_events": pending, "sources": source_rows, "rejected_by_receive_day": rejected}
