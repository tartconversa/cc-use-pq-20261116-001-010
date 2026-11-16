"""不可变的指标定义及其版本登记。"""
from __future__ import annotations

import json

from exhibit_lake.catalog.registry import KINDS, now, require_id
from exhibit_lake.catalog.store import Store
from exhibit_lake.errors import InvalidInput, NotFound
from exhibit_lake.ingest.validation import canonical, digest


MEASURES = {"events", "unique_visitors", "value_sum", "value_mean"}
MEASURE_ALIASES = {"count": "events", "distinct": "unique_visitors", "unique": "unique_visitors",
                   "sum": "value_sum", "mean": "value_mean", "average": "value_mean"}
SPEC_KEYS = {"numerator", "denominator", "round"}


def _side(spec: dict, label: str) -> dict:
    side = spec.get(label)
    if not isinstance(side, dict) or set(side) != {"kind", "measure"}:
        raise InvalidInput(f"{label} must contain exactly kind and measure")
    if side["kind"] not in KINDS:
        raise InvalidInput(f"{label} kind must be one of {sorted(KINDS)}")
    measure = MEASURE_ALIASES.get(side["measure"], side["measure"])
    if measure not in MEASURES:
        raise InvalidInput(f"{label} measure must be one of {sorted(MEASURES)}")
    if measure == "unique_visitors" and side["kind"] != "feedback":
        raise InvalidInput("unique_visitors requires feedback events with visitor identity")
    return {"kind": side["kind"], "measure": measure}


def validate_spec(spec: object) -> dict:
    """校验并规范化指标口径，拒绝未知字段以免拼写错误被静默接受。"""
    if not isinstance(spec, dict):
        raise InvalidInput("metric spec must be an object")
    unknown = set(spec) - SPEC_KEYS
    if unknown:
        raise InvalidInput(f"unknown metric spec fields: {sorted(unknown)}")
    if not {"numerator", "denominator"} <= set(spec):
        raise InvalidInput("metric spec requires numerator and denominator")
    round_to = spec.get("round", 6)
    if type(round_to) is not int or not 0 <= round_to <= 12:
        raise InvalidInput("round must be an integer from 0 to 12")
    return {"numerator": _side(spec, "numerator"),
            "denominator": _side(spec, "denominator"), "round": round_to}


def definition_digest(name: str, spec: dict, description: str | None = None) -> str:
    return digest({"name": name, "spec": spec, "description": description})


def _describe(row: dict, *, latest: int | None = None) -> dict:
    return {"metric_key": row["metric_key"], "version": row["version"], "name": row["name"],
            "description": row["description"], "spec": json.loads(row["spec_json"]),
            "digest": row["digest"], "created_at": row["created_at"],
            "latest": latest is not None and row["version"] == latest}


class MetricDefinitions:
    def __init__(self, store: Store):
        self.store = store

    def register(self, metric_key: str, name: str, spec: object,
                 description: str | None = None) -> dict:
        require_id(metric_key, "metric_key")
        if not isinstance(name, str) or not 1 <= len(name.strip()) <= 120:
            raise InvalidInput("metric name must be a nonempty string of at most 120 characters")
        if description is not None and (not isinstance(description, str) or len(description) > 500):
            raise InvalidInput("metric description must be text of at most 500 characters")
        normalized = validate_spec(spec)
        normalized_name = name.strip()
        checksum = definition_digest(normalized_name, normalized, description)
        with self.store.transaction() as conn:
            rows = Store.many(conn, "SELECT * FROM metric_definitions WHERE metric_key=? ORDER BY version",
                              (metric_key,))
            if rows and rows[-1]["digest"] == checksum:
                # 相同内容重复登记保持幂等，不制造语义重复的版本。
                return {**_describe(rows[-1], latest=rows[-1]["version"]), "created": False}
            version = rows[-1]["version"] + 1 if rows else 1
            created_at = now()
            conn.execute("INSERT INTO metric_definitions(metric_key,version,name,description,spec_json,digest,created_at) "
                         "VALUES(?,?,?,?,?,?,?)",
                         (metric_key, version, normalized_name, description,
                          canonical(normalized), checksum, created_at))
            Store.audit(conn, "metric.definition.registered", metric_key,
                        {"version": version, "digest": checksum, "name": normalized_name}, created_at)
            row = Store.one(conn, "SELECT * FROM metric_definitions WHERE metric_key=? AND version=?",
                            (metric_key, version))
            return {**_describe(row, latest=version), "created": True}

    def latest(self, metric_key: str) -> dict:
        with self.store.transaction(write=False) as conn:
            return self._latest(conn, metric_key)

    def _latest(self, conn, metric_key: str) -> dict:
        row = Store.one(conn, "SELECT * FROM metric_definitions WHERE metric_key=? ORDER BY version DESC LIMIT 1",
                        (metric_key,))
        if not row:
            raise NotFound("metric definition not found")
        return _describe(row, latest=row["version"])

    def get(self, metric_key: str, version: int) -> dict:
        with self.store.transaction(write=False) as conn:
            return self._get(conn, metric_key, version)

    def _get(self, conn, metric_key: str, version: int) -> dict:
        row = Store.one(conn, "SELECT * FROM metric_definitions WHERE metric_key=? AND version=?",
                        (metric_key, version))
        if not row:
            raise NotFound("metric definition not found")
        latest = Store.one(conn, "SELECT MAX(version) AS version FROM metric_definitions WHERE metric_key=?",
                           (metric_key,))["version"]
        return _describe(row, latest=latest)

    def resolve(self, metric_key: str, version: int | None) -> dict:
        with self.store.transaction(write=False) as conn:
            return self._resolve(conn, metric_key, version)

    def _resolve(self, conn, metric_key: str, version: int | None) -> dict:
        if version is None:
            return self._latest(conn, metric_key)
        if type(version) is not int or version < 1:
            raise InvalidInput("definition_version must be a positive integer")
        return self._get(conn, metric_key, version)

    def versions(self, metric_key: str) -> dict:
        with self.store.transaction(write=False) as conn:
            rows = Store.many(conn, "SELECT * FROM metric_definitions WHERE metric_key=? ORDER BY version",
                              (metric_key,))
            if not rows:
                raise NotFound("metric definition not found")
            latest = rows[-1]["version"]
            return {"metric_key": metric_key,
                    "versions": [_describe(row, latest=latest) for row in rows]}

    def catalog(self) -> dict:
        with self.store.transaction(write=False) as conn:
            return {"metrics": Store.many(conn, "SELECT metric_key,MAX(version) AS latest_version,"
                                          "COUNT(*) AS versions FROM metric_definitions "
                                          "GROUP BY metric_key ORDER BY metric_key")}

