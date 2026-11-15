"""SQLite 事务边界与持久化目录结构。"""
from __future__ import annotations

import contextlib
import json
import sqlite3
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS venues (
    venue_id TEXT PRIMARY KEY, name TEXT NOT NULL, timezone TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS exhibits (
    exhibit_id TEXT PRIMARY KEY, venue_id TEXT NOT NULL REFERENCES venues(venue_id),
    name TEXT NOT NULL, maker TEXT NOT NULL, launched_on TEXT NOT NULL,
    withdrawn_on TEXT, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sources (
    source_id TEXT PRIMARY KEY, venue_id TEXT NOT NULL REFERENCES venues(venue_id),
    kind TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS contracts (
    contract_id TEXT PRIMARY KEY, source_id TEXT NOT NULL REFERENCES sources(source_id),
    revision INTEGER NOT NULL, kind TEXT NOT NULL, fields_json TEXT NOT NULL,
    effective_from TEXT NOT NULL, effective_to TEXT, created_at TEXT NOT NULL,
    UNIQUE(source_id, revision)
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY, source_id TEXT NOT NULL REFERENCES sources(source_id),
    request_key TEXT NOT NULL, payload_hash TEXT NOT NULL, contract_id TEXT NOT NULL REFERENCES contracts(contract_id),
    status TEXT NOT NULL, received_at TEXT NOT NULL, committed_at TEXT,
    total_rows INTEGER NOT NULL, valid_rows INTEGER NOT NULL, rejected_rows INTEGER NOT NULL,
    UNIQUE(source_id, request_key)
);
CREATE TABLE IF NOT EXISTS raw_rows (
    batch_id TEXT NOT NULL REFERENCES batches(batch_id), row_no INTEGER NOT NULL,
    payload_json TEXT NOT NULL, payload_hash TEXT NOT NULL,
    PRIMARY KEY(batch_id,row_no)
);
CREATE TABLE IF NOT EXISTS accepted_events (
    event_id TEXT NOT NULL, source_id TEXT NOT NULL REFERENCES sources(source_id),
    batch_id TEXT NOT NULL REFERENCES batches(batch_id), row_no INTEGER NOT NULL,
    venue_id TEXT NOT NULL, exhibit_id TEXT NOT NULL, event_at TEXT NOT NULL,
    local_day TEXT NOT NULL, kind TEXT NOT NULL, value_num REAL, category TEXT,
    visitor_hash TEXT, payload_hash TEXT NOT NULL, published_at TEXT,
    PRIMARY KEY(source_id,event_id), UNIQUE(batch_id,row_no)
);
CREATE INDEX IF NOT EXISTS ix_events_day ON accepted_events(venue_id,local_day,exhibit_id,kind);
CREATE INDEX IF NOT EXISTS ix_events_batch ON accepted_events(batch_id);
CREATE TABLE IF NOT EXISTS rejected_rows (
    batch_id TEXT NOT NULL REFERENCES batches(batch_id), row_no INTEGER NOT NULL,
    code TEXT NOT NULL, detail TEXT NOT NULL,
    PRIMARY KEY(batch_id,row_no)
);
CREATE TABLE IF NOT EXISTS partitions (
    venue_id TEXT NOT NULL REFERENCES venues(venue_id), local_day TEXT NOT NULL,
    generation INTEGER NOT NULL DEFAULT 0, head_id TEXT,
    PRIMARY KEY(venue_id, local_day)
);
CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id TEXT PRIMARY KEY, venue_id TEXT NOT NULL, local_day TEXT NOT NULL,
    generation INTEGER NOT NULL, parent_id TEXT, status TEXT NOT NULL,
    manifest_json TEXT NOT NULL, digest TEXT NOT NULL, created_at TEXT NOT NULL,
    published_at TEXT
);
CREATE TABLE IF NOT EXISTS snapshot_members (
    snapshot_id TEXT NOT NULL REFERENCES snapshots(snapshot_id),
    source_id TEXT NOT NULL, event_id TEXT NOT NULL, payload_hash TEXT NOT NULL,
    PRIMARY KEY(snapshot_id,source_id,event_id)
);
CREATE TABLE IF NOT EXISTS audits (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT, action TEXT NOT NULL,
    subject TEXT NOT NULL, detail_json TEXT NOT NULL, at TEXT NOT NULL
);
"""


class Store:
    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.executescript(SCHEMA)

    @contextlib.contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=15000")
        try:
            yield conn
        finally:
            conn.close()

    @contextlib.contextmanager
    def transaction(self, *, write: bool = True) -> Iterator[sqlite3.Connection]:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            try:
                yield conn
            except BaseException:
                conn.rollback()
                raise
            else:
                conn.commit()

    @staticmethod
    def one(conn: sqlite3.Connection, sql: str, args: tuple = {}) -> dict | None:
        row = conn.execute(sql, args).fetchone()
        return dict(row) if row is not None else None

    @staticmethod
    def many(conn: sqlite3.Connection, sql: str, args: tuple = ()) -> list[dict]:
        return [dict(row) for row in conn.execute(sql, args)]

    @staticmethod
    def audit(conn: sqlite3.Connection, action: str, subject: str, detail: dict, at: str) -> None:
        conn.execute("INSERT INTO audits(action,subject,detail_json,at) VALUES(?,?,?,?)",
                     (action, subject, json.dumps(detail, sort_keys=True, ensure_ascii=False), at))
