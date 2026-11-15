"""带版本的事件契约与业务时间校验。"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from exhibit_lake.catalog.registry import iso_time, require_id
from exhibit_lake.errors import InvalidInput


def canonical(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: object) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def fail(code: str, detail: str) -> tuple[None, dict]:
    return None, {"code": code, "detail": detail}


def event(row: object, contract: dict, source: dict, venue: dict, exhibits: dict[str, dict]) -> tuple[dict | None, dict | None]:
    if not isinstance(row, dict):
        return fail("not_object", "event must be an object")
    fields = set(json.loads(contract["fields_json"]))
    if set(row) != fields:
        return fail("field_mismatch", "event fields do not match the contract")
    try:
        event_id = require_id(row["event_id"], "event_id")
        exhibit_id = require_id(row["exhibit_id"], "exhibit_id")
        stamp = iso_time(row["event_at"], "event_at")
    except InvalidInput as exc:
        return fail("invalid_identity_or_time", str(exc))
    instant = datetime.fromisoformat(stamp)
    if stamp < contract["effective_from"] or (contract["effective_to"] and stamp >= contract["effective_to"]):
        return fail("outside_contract", "event time is outside contract interval")
    exhibit = exhibits.get(exhibit_id)
    if not exhibit or exhibit["venue_id"] != source["venue_id"]:
        return fail("unknown_exhibit", "exhibit is not registered at this venue")
    day = instant.astimezone(ZoneInfo(venue["timezone"])).date().isoformat()
    if day < exhibit["launched_on"] or (exhibit["withdrawn_on"] and day > exhibit["withdrawn_on"]):
        return fail("inactive_exhibit", "exhibit was not available on local event day")
    if "channel" in row and (not isinstance(row["channel"], str) or not 1 <= len(row["channel"].strip()) <= 40):
        return fail("invalid_channel", "channel must be a nonempty short string")
    value_num = None
    category = None
    visitor_hash = None
    if source["kind"] == "visit":
        count = row["count"]
        if type(count) is not int or not 1 <= count <= 100000:
            return fail("invalid_count", "visit count must be an integer from 1 to 100000")
        value_num = count
        category = row.get("channel")
    elif source["kind"] == "feedback":
        rating = row["rating"]
        if type(rating) is not int or not 1 <= rating <= 5:
            return fail("invalid_rating", "rating must be an integer from 1 to 5")
        try:
            require_id(row["visitor_id"], "visitor_id")
        except InvalidInput as exc:
            return fail("invalid_visitor", str(exc))
        comment = row.get("comment")
        if comment is not None and (not isinstance(comment, str) or len(comment) > 1000):
            return fail("invalid_comment", "comment must be text of at most 1000 characters")
        value_num = rating
        category = row.get("channel")
        visitor_hash = hashlib.sha256((source["venue_id"] + ":" + row["visitor_id"]).encode()).hexdigest()
    elif source["kind"] == "demo":
        duration = row["duration_seconds"]
        if isinstance(duration, bool) or not isinstance(duration, (int, float)) or not math.isfinite(duration) or not 0 < duration <= 86400:
            return fail("invalid_duration", "demo duration must be finite and within one day")
        if "host_id" in row:
            try:
                require_id(row["host_id"], "host_id")
            except InvalidInput as exc:
                return fail("invalid_host", str(exc))
        value_num = float(duration)
        category = row.get("channel")
    return {"event_id": event_id, "source_id": source["source_id"], "venue_id": source["venue_id"],
            "exhibit_id": exhibit_id, "event_at": stamp, "local_day": day, "kind": source["kind"],
            "value_num": value_num, "category": category, "visitor_hash": visitor_hash,
            "payload_hash": digest(row)}, None
