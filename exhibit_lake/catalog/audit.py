"""只读的运行审计与一致性检查。"""
from __future__ import annotations

import json

from exhibit_lake.catalog.store import Store
from exhibit_lake.errors import InvalidInput, NotFound


class Audit:
    def __init__(self, store: Store):
        self.store = store

    def trail(self, *, action: str | None = None, subject: str | None = None,
              offset: int = 0, limit: int = 100) -> dict:
        if offset < 0 or not 1 <= limit <= 500:
            raise InvalidInput("invalid audit page")
        sql = "SELECT * FROM audits WHERE 1=1"
        args = []
        if action:
            sql += " AND action=?"
            args.append(action)
        if subject:
            sql += " AND subject=?"
            args.append(subject)
        with self.store.transaction(write=False) as conn:
            total = Store.one(conn, sql.replace("SELECT *", "SELECT COUNT(*) AS n"), tuple(args))["n"]
            rows = Store.many(conn, sql + " ORDER BY audit_id LIMIT ? OFFSET ?", tuple(args + [limit, offset]))
        for row in rows:
            row["detail"] = json.loads(row.pop("detail_json"))
        return {"total": total, "offset": offset, "limit": limit, "audits": rows}

    def batch_integrity(self, batch_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            batch = Store.one(conn, "SELECT * FROM batches WHERE batch_id=?", (batch_id,))
            if not batch:
                raise NotFound("batch not found")
            raw = Store.one(conn, "SELECT COUNT(*) AS n FROM raw_rows WHERE batch_id=?", (batch_id,))["n"]
            accepted = Store.one(conn, "SELECT COUNT(*) AS n FROM accepted_events WHERE batch_id=?", (batch_id,))["n"]
            rejected = Store.one(conn, "SELECT COUNT(*) AS n FROM rejected_rows WHERE batch_id=?", (batch_id,))["n"]
            issues = []
            if raw != batch["total_rows"]:
                issues.append("raw_count")
            if accepted != batch["valid_rows"]:
                issues.append("accepted_count")
            if rejected != batch["rejected_rows"]:
                issues.append("rejected_count")
            if raw != accepted + rejected:
                issues.append("classification_count")
            overlap = Store.one(conn, "SELECT COUNT(*) AS n FROM accepted_events e JOIN rejected_rows r "
                                "ON e.batch_id=r.batch_id AND e.row_no=r.row_no WHERE e.batch_id=?", (batch_id,))["n"]
            if overlap:
                issues.append("accepted_rejected_overlap")
            missing = Store.one(conn, "SELECT COUNT(*) AS n FROM raw_rows r LEFT JOIN accepted_events e "
                                "ON e.batch_id=r.batch_id AND e.row_no=r.row_no LEFT JOIN rejected_rows j "
                                "ON j.batch_id=r.batch_id AND j.row_no=r.row_no WHERE r.batch_id=? "
                                "AND e.row_no IS NULL AND j.row_no IS NULL", (batch_id,))["n"]
            if missing:
                issues.append("unclassified_raw_rows")
            return {"batch_id": batch_id, "valid": not issues, "issues": issues,
                    "raw_rows": raw, "accepted_rows": accepted, "rejected_rows": rejected}

    def venue_integrity(self, venue_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
                raise NotFound("venue not found")
            parts = Store.many(conn, "SELECT * FROM partitions WHERE venue_id=? ORDER BY local_day", (venue_id,))
            issues = []
            for part in parts:
                if part["head_id"]:
                    snapshot = Store.one(conn, "SELECT * FROM snapshots WHERE snapshot_id=?", (part["head_id"],))
                    if not snapshot or snapshot["status"] != "published":
                        issues.append({"local_day": part["local_day"], "code": "missing_published_head"})
                    elif snapshot["venue_id"] != venue_id or snapshot["local_day"] != part["local_day"] or snapshot["generation"] != part["generation"]:
                        issues.append({"local_day": part["local_day"], "code": "head_partition_mismatch"})
                elif part["generation"] != 0:
                    issues.append({"local_day": part["local_day"], "code": "orphan_generation"})
            batches = Store.many(conn, "SELECT b.batch_id FROM batches b JOIN sources s ON s.source_id=b.source_id "
                                 "WHERE s.venue_id=? ORDER BY b.batch_id", (venue_id,))
        damaged = [self.batch_integrity(x["batch_id"]) for x in batches]
        issues.extend({"batch_id": x["batch_id"], "code": "batch_integrity"} for x in damaged if not x["valid"])
        return {"venue_id": venue_id, "valid": not issues, "partitions": len(parts),
                "batches": len(batches), "issues": issues}
