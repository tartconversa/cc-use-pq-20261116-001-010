"""构建可复现日期分区、暂存候选并以条件交换发布。"""
from __future__ import annotations

import json
import sqlite3
import uuid
from collections import defaultdict
from datetime import datetime

from exhibit_lake.catalog.registry import iso_day, now
from exhibit_lake.catalog.store import Store
from exhibit_lake.errors import Conflict, IntegrityFailure, InvalidInput, NotFound, StateError
from exhibit_lake.ingest.validation import digest


def _statistics(events: list[dict]) -> dict:
    by_kind = defaultdict(int)
    by_exhibit = defaultdict(lambda: {"events": 0, "visits": 0, "feedback": 0, "demos": 0})
    for item in events:
        by_kind[item["kind"]] += 1
        group = by_exhibit[item["exhibit_id"]]
        group["events"] += 1
        if item["kind"] == "visit":
            group["visits"] += int(item["value_num"])
        elif item["kind"] == "feedback":
            group["feedback"] += 1
        elif item["kind"] == "demo":
            group["demos"] += 1
    return {"events": len(events), "by_kind": dict(sorted(by_kind.items())),
            "by_exhibit": dict(sorted(by_exhibit.items()))}


class Snapshots:
    def __init__(self, store: Store):
        self.store = store

    def _partition(self, conn: sqlite3.Connection, venue_id: str, day: str) -> dict:
        part = Store.one(conn, "SELECT * FROM partitions WHERE venue_id=? AND local_day=?", (venue_id, day))
        if not part:
            if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
                raise NotFound("venue not found")
            conn.execute("INSERT INTO partitions(venue_id,local_day,generation,head_id) VALUES(?,?,0,NULL)", (venue_id, day))
            part = Store.one(conn, "SELECT * FROM partitions WHERE venue_id=? AND local_day=?", (venue_id, day))
        return part

    def head(self, venue_id: str, day: str) -> dict:
        iso_day(day, "local_day")
        with self.store.transaction(write=False) as conn:
            if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
                raise NotFound("venue not found")
            part = Store.one(conn, "SELECT * FROM partitions WHERE venue_id=? AND local_day=?", (venue_id, day))
            if not part:
                return {"venue_id": venue_id, "local_day": day, "generation": 0, "head_id": None}
            return part

    def stage(self, venue_id: str, day: str, expected_head: str | None = None) -> dict:
        iso_day(day, "local_day")
        with self.store.transaction() as conn:
            part = self._partition(conn, venue_id, day)
            if part["head_id"] != expected_head:
                raise Conflict("expected head differs from current partition", details={"head_id": part["head_id"]})
            events = Store.many(conn, "SELECT source_id,event_id,exhibit_id,event_at,local_day,kind,value_num,category,visitor_hash,payload_hash "
                               "FROM accepted_events WHERE venue_id=? AND local_day=? ORDER BY source_id,event_id", (venue_id, day))
            membership = [{"source_id": x["source_id"], "event_id": x["event_id"], "payload_hash": x["payload_hash"]}
                          for x in events]
            manifest = {"venue_id": venue_id, "local_day": day, "generation": part["generation"] + 1,
                        "parent_id": part["head_id"], "members": membership, "statistics": _statistics(events)}
            checksum = digest(manifest)
            duplicate = Store.one(conn, "SELECT snapshot_id FROM snapshots WHERE venue_id=? AND local_day=? AND generation=? AND status='staged'",
                                  (venue_id, day, manifest["generation"]))
            if duplicate:
                raise Conflict("candidate already staged for this generation")
            snapshot_id = uuid.uuid4().hex
            conn.execute("INSERT INTO snapshots VALUES(?,?,?,?,?,?,?,?,?,?)",
                         (snapshot_id, venue_id, day, manifest["generation"], part["head_id"], "staged",
                          json.dumps(manifest, sort_keys=True), checksum, now(), None))
            conn.executemany("INSERT INTO snapshot_members VALUES(?,?,?,?)",
                             [(snapshot_id, item["source_id"], item["event_id"], item["payload_hash"]) for item in membership])
            Store.audit(conn, "snapshot.staged", snapshot_id,
                        {"venue_id": venue_id, "local_day": day, "members": len(membership)}, now())
            return self._describe(conn, snapshot_id)

    def _describe(self, conn: sqlite3.Connection, snapshot_id: str) -> dict:
        snapshot = Store.one(conn, "SELECT * FROM snapshots WHERE snapshot_id=?", (snapshot_id,))
        if not snapshot:
            raise NotFound("snapshot not found")
        return {"snapshot_id": snapshot_id, "venue_id": snapshot["venue_id"], "local_day": snapshot["local_day"],
                "generation": snapshot["generation"], "parent_id": snapshot["parent_id"],
                "status": snapshot["status"], "digest": snapshot["digest"],
                "manifest": json.loads(snapshot["manifest_json"]), "published_at": snapshot["published_at"]}

    def describe(self, snapshot_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            return self._describe(conn, snapshot_id)

    def verify(self, snapshot_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            return self.verify_in(conn, snapshot_id)

    def verify_in(self, conn: sqlite3.Connection, snapshot_id: str) -> dict:
        """校验快照并返回其成员摘要，供同一事务中的下游计算复用。"""
        snapshot = self._describe(conn, snapshot_id)
        manifest = snapshot["manifest"]
        members = Store.many(conn, "SELECT source_id,event_id,payload_hash FROM snapshot_members WHERE snapshot_id=? ORDER BY source_id,event_id",
                             (snapshot_id,))
        if members != manifest["members"]:
            raise IntegrityFailure("snapshot membership differs from manifest")
        if digest(manifest) != snapshot["digest"]:
            raise IntegrityFailure("snapshot digest differs from manifest")
        if manifest["venue_id"] != snapshot["venue_id"] or manifest["local_day"] != snapshot["local_day"]:
            raise IntegrityFailure("snapshot partition differs from manifest")
        if manifest["generation"] != snapshot["generation"] or manifest["parent_id"] != snapshot["parent_id"]:
            raise IntegrityFailure("snapshot lineage differs from manifest")
        if len({(x["source_id"], x["event_id"]) for x in members}) != len(members):
            raise IntegrityFailure("duplicate snapshot membership")
        records = self._events(conn, snapshot_id)
        if len(records) != len(members):
            raise IntegrityFailure("snapshot event missing")
        for item, record in zip(members, records):
            if item["source_id"] != record["source_id"] or item["event_id"] != record["event_id"]:
                raise IntegrityFailure("snapshot event order differs")
            if item["payload_hash"] != record["payload_hash"]:
                raise IntegrityFailure("source event changed since staging")
            if record["venue_id"] != snapshot["venue_id"] or record["local_day"] != snapshot["local_day"]:
                raise IntegrityFailure("snapshot event belongs to another partition")
        if manifest["statistics"] != _statistics(records):
            raise IntegrityFailure("snapshot statistics differ from members")
        return {"snapshot_id": snapshot_id, "valid": True, "events": len(records), "digest": snapshot["digest"]}

    def members_in(self, conn: sqlite3.Connection, snapshot_id: str) -> list[dict]:
        return Store.many(conn, "SELECT source_id,event_id,payload_hash FROM snapshot_members "
                          "WHERE snapshot_id=? ORDER BY source_id,event_id", (snapshot_id,))

    def records_in(self, conn: sqlite3.Connection, snapshot_id: str) -> list[dict]:
        return self._events(conn, snapshot_id)

    def _events(self, conn: sqlite3.Connection, snapshot_id: str) -> list[dict]:
        return Store.many(conn, "SELECT e.source_id,e.event_id,e.exhibit_id,e.event_at,e.local_day,e.venue_id,"
                          "e.kind,e.value_num,e.category,e.visitor_hash,e.payload_hash FROM snapshot_members m "
                          "JOIN accepted_events e ON e.source_id=m.source_id AND e.event_id=m.event_id "
                          "WHERE m.snapshot_id=? ORDER BY m.source_id,m.event_id", (snapshot_id,))

    def publish(self, snapshot_id: str, expected_head: str | None) -> dict:
        self.verify(snapshot_id)
        with self.store.transaction() as conn:
            snapshot = Store.one(conn, "SELECT * FROM snapshots WHERE snapshot_id=?", (snapshot_id,))
            if not snapshot:
                raise NotFound("snapshot not found")
            part = self._partition(conn, snapshot["venue_id"], snapshot["local_day"])
            if snapshot["status"] == "published" and part["head_id"] == snapshot_id:
                return self._describe(conn, snapshot_id)
            if snapshot["status"] != "staged":
                raise StateError("only staged snapshots can become head")
            if part["head_id"] != expected_head or snapshot["parent_id"] != expected_head:
                raise Conflict("head changed since candidate staging", details={"head_id": part["head_id"]})
            if snapshot["generation"] != part["generation"] + 1:
                raise Conflict("snapshot generation is stale")
            stamp = now()
            result = conn.execute("UPDATE partitions SET head_id=?,generation=? WHERE venue_id=? AND local_day=? AND head_id IS ? AND generation=?",
                                  (snapshot_id, snapshot["generation"], snapshot["venue_id"], snapshot["local_day"],
                                   expected_head, part["generation"]))
            if result.rowcount != 1:
                raise Conflict("concurrent head update")
            conn.execute("UPDATE snapshots SET status='published',published_at=? WHERE snapshot_id=?", (stamp, snapshot_id))
            conn.execute("UPDATE accepted_events SET published_at=COALESCE(published_at,?) WHERE (source_id,event_id) "
                         "IN (SELECT source_id,event_id FROM snapshot_members WHERE snapshot_id=?)", (stamp, snapshot_id))
            Store.audit(conn, "snapshot.published", snapshot_id,
                        {"parent_id": expected_head, "generation": snapshot["generation"]}, stamp)
            return self._describe(conn, snapshot_id)

    def abandon(self, snapshot_id: str) -> dict:
        with self.store.transaction() as conn:
            snapshot = Store.one(conn, "SELECT * FROM snapshots WHERE snapshot_id=?", (snapshot_id,))
            if not snapshot:
                raise NotFound("snapshot not found")
            if snapshot["status"] != "staged":
                raise StateError("only staged snapshots can be abandoned")
            conn.execute("UPDATE snapshots SET status='abandoned' WHERE snapshot_id=?", (snapshot_id,))
            Store.audit(conn, "snapshot.abandoned", snapshot_id, {}, now())
            return self._describe(conn, snapshot_id)

    def list(self, venue_id: str, day: str, *, include_staged: bool = False) -> list[dict]:
        iso_day(day, "local_day")
        with self.store.transaction(write=False) as conn:
            sql = "SELECT snapshot_id FROM snapshots WHERE venue_id=? AND local_day=?"
            if not include_staged:
                sql += " AND status='published'"
            sql += " ORDER BY generation"
            ids = [row["snapshot_id"] for row in Store.many(conn, sql, (venue_id, day))]
            return [self._describe(conn, item) for item in ids]

    def inventory(self, venue_id: str, day: str) -> dict:
        iso_day(day, "local_day")
        with self.store.transaction(write=False) as conn:
            if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
                raise NotFound("venue not found")
            rows = Store.many(conn, "SELECT kind,COUNT(*) AS events,COUNT(DISTINCT batch_id) AS batches "
                              "FROM accepted_events WHERE venue_id=? AND local_day=? GROUP BY kind ORDER BY kind", (venue_id, day))
            part = Store.one(conn, "SELECT * FROM partitions WHERE venue_id=? AND local_day=?", (venue_id, day))
            unpublished = Store.one(conn, "SELECT COUNT(*) AS n FROM accepted_events e WHERE e.venue_id=? AND e.local_day=? "
                                    "AND NOT EXISTS(SELECT 1 FROM snapshot_members m WHERE m.snapshot_id=? AND m.source_id=e.source_id AND m.event_id=e.event_id)",
                                    (venue_id, day, part["head_id"] if part else ""))
            return {"venue_id": venue_id, "local_day": day, "head_id": part["head_id"] if part else None,
                    "generation": part["generation"] if part else 0, "unpublished_events": unpublished["n"], "sources": rows}
