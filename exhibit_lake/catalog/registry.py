"""管理场馆、产品、来源登记及分时生效的契约修订。"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from exhibit_lake.catalog.store import Store
from exhibit_lake.errors import Conflict, InvalidInput, NotFound


KINDS = {"visit", "feedback", "demo"}
REQUIRED = {
    "visit": {"event_id", "exhibit_id", "event_at", "count"},
    "feedback": {"event_id", "exhibit_id", "event_at", "rating", "visitor_id"},
    "demo": {"event_id", "exhibit_id", "event_at", "duration_seconds"},
}
OPTIONAL = {"visit": {"channel"}, "feedback": {"comment", "channel"},
            "demo": {"host_id", "channel"}}


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def require_id(value: str, label: str) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= 80 or not all(
            ch.isalnum() or ch in "_-:." for ch in value):
        raise InvalidInput(f"{label} must be a nonempty identifier of at most 80 characters")
    return value


def iso_day(value: str, label: str) -> str:
    try:
        if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
            raise ValueError()
    except ValueError:
        raise InvalidInput(f"{label} must be an ISO date") from None
    return value


def iso_time(value: str, label: str) -> str:
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.utcoffset() is None:
            raise ValueError()
    except (TypeError, ValueError):
        raise InvalidInput(f"{label} must be an offset-aware ISO timestamp") from None
    return parsed.astimezone(timezone.utc).isoformat()


class Registry:
    def __init__(self, store: Store):
        self.store = store

    def venue(self, venue_id: str, name: str, timezone_name: str) -> dict:
        require_id(venue_id, "venue_id")
        if not isinstance(name, str) or not name.strip():
            raise InvalidInput("venue name is required")
        try:
            ZoneInfo(timezone_name)
        except (TypeError, ZoneInfoNotFoundError):
            raise InvalidInput("unknown venue timezone") from None
        with self.store.transaction() as conn:
            if Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
                raise Conflict("venue already exists")
            conn.execute("INSERT INTO venues VALUES(?,?,?,?)", (venue_id, name.strip(), timezone_name, now()))
            Store.audit(conn, "venue.created", venue_id, {"timezone": timezone_name}, now())
        return {"venue_id": venue_id, "name": name.strip(), "timezone": timezone_name}

    def exhibit(self, exhibit_id: str, venue_id: str, name: str, maker: str,
                launched_on: str, withdrawn_on: str | None = None) -> dict:
        require_id(exhibit_id, "exhibit_id")
        require_id(venue_id, "venue_id")
        iso_day(launched_on, "launched_on")
        if withdrawn_on and iso_day(withdrawn_on, "withdrawn_on") < launched_on:
            raise InvalidInput("withdrawal precedes launch")
        if not all(isinstance(x, str) and x.strip() for x in (name, maker)):
            raise InvalidInput("exhibit name and maker are required")
        with self.store.transaction() as conn:
            if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
                raise NotFound("venue not found")
            if Store.one(conn, "SELECT exhibit_id FROM exhibits WHERE exhibit_id=?", (exhibit_id,)):
                raise Conflict("exhibit already exists")
            conn.execute("INSERT INTO exhibits VALUES(?,?,?,?,?,?,?)",
                         (exhibit_id, venue_id, name.strip(), maker.strip(), launched_on, withdrawn_on, now()))
            Store.audit(conn, "exhibit.created", exhibit_id, {"venue_id": venue_id}, now())
        return {"exhibit_id": exhibit_id, "venue_id": venue_id, "launched_on": launched_on}

    def withdraw(self, exhibit_id: str, day: str) -> dict:
        iso_day(day, "withdrawn_on")
        with self.store.transaction() as conn:
            exhibit = Store.one(conn, "SELECT * FROM exhibits WHERE exhibit_id=?", (exhibit_id,))
            if not exhibit:
                raise NotFound("exhibit not found")
            if day < exhibit["launched_on"]:
                raise InvalidInput("withdrawal precedes launch")
            if exhibit["withdrawn_on"] is not None:
                raise Conflict("exhibit already withdrawn")
            conn.execute("UPDATE exhibits SET withdrawn_on=? WHERE exhibit_id=?", (day, exhibit_id))
            Store.audit(conn, "exhibit.withdrawn", exhibit_id, {"day": day}, now())
        return {"exhibit_id": exhibit_id, "withdrawn_on": day}

    def source(self, source_id: str, venue_id: str, kind: str) -> dict:
        require_id(source_id, "source_id")
        if kind not in KINDS:
            raise InvalidInput("unknown source kind")
        with self.store.transaction() as conn:
            if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
                raise NotFound("venue not found")
            if Store.one(conn, "SELECT source_id FROM sources WHERE source_id=?", (source_id,)):
                raise Conflict("source already exists")
            conn.execute("INSERT INTO sources VALUES(?,?,?,?,?)", (source_id, venue_id, kind, 1, now()))
            Store.audit(conn, "source.created", source_id, {"venue_id": venue_id, "kind": kind}, now())
        return {"source_id": source_id, "venue_id": venue_id, "kind": kind}

    def deactivate_source(self, source_id: str) -> dict:
        with self.store.transaction() as conn:
            source = Store.one(conn, "SELECT * FROM sources WHERE source_id=?", (source_id,))
            if not source:
                raise NotFound("source not found")
            if not source["active"]:
                raise Conflict("source already inactive")
            conn.execute("UPDATE sources SET active=0 WHERE source_id=?", (source_id,))
            Store.audit(conn, "source.deactivated", source_id, {}, now())
        return {"source_id": source_id, "active": False}

    def contract(self, contract_id: str, source_id: str, fields: list[str],
                 effective_from: str, effective_to: str | None = None) -> dict:
        require_id(contract_id, "contract_id")
        iso_time(effective_from, "effective_from")
        if effective_to and iso_time(effective_to, "effective_to") <= iso_time(effective_from, "effective_from"):
            raise InvalidInput("contract interval is empty")
        with self.store.transaction() as conn:
            source = Store.one(conn, "SELECT * FROM sources WHERE source_id=?", (source_id,))
            if not source:
                raise NotFound("source not found")
            if not source["active"]:
                raise Conflict("source inactive")
            if not isinstance(fields, list) or len(fields) != len(set(fields)) or not all(isinstance(f, str) for f in fields):
                raise InvalidInput("fields must be unique strings")
            if not REQUIRED[source["kind"]] <= set(fields) or set(fields) - (REQUIRED[source["kind"]] | OPTIONAL[source["kind"]]):
                raise InvalidInput("contract fields do not match source kind")
            existing = Store.many(conn, "SELECT * FROM contracts WHERE source_id=? ORDER BY revision", (source_id,))
            start = iso_time(effective_from, "effective_from")
            end = iso_time(effective_to, "effective_to") if effective_to else None
            for old in existing:
                if start < (old["effective_to"] or "9999") and old["effective_from"] < (end or "9999"):
                    raise Conflict("contract effective intervals overlap")
            if existing and start < existing[-1]["effective_from"]:
                raise Conflict("contract revisions must be chronological")
            if Store.one(conn, "SELECT contract_id FROM contracts WHERE contract_id=?", (contract_id,)):
                raise Conflict("contract already exists")
            revision = len(existing) + 1
            conn.execute("INSERT INTO contracts VALUES(?,?,?,?,?,?,?,?)",
                         (contract_id, source_id, revision, source["kind"], json.dumps(fields), start, end, now()))
            Store.audit(conn, "contract.created", contract_id, {"source_id": source_id, "revision": revision}, now())
        return {"contract_id": contract_id, "source_id": source_id, "revision": revision,
                "fields": fields, "effective_from": start, "effective_to": end}

    def describe(self, venue_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            venue = Store.one(conn, "SELECT * FROM venues WHERE venue_id=?", (venue_id,))
            if not venue:
                raise NotFound("venue not found")
            exhibits = Store.many(conn, "SELECT * FROM exhibits WHERE venue_id=? ORDER BY exhibit_id", (venue_id,))
            sources = Store.many(conn, "SELECT * FROM sources WHERE venue_id=? ORDER BY source_id", (venue_id,))
        return {"venue": venue, "exhibits": exhibits, "sources": sources}
