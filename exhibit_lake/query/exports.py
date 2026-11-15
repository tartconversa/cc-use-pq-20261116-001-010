"""稳定导出已发布事件及汇总视图的 CSV。"""
from __future__ import annotations

import csv
import io
import json

from exhibit_lake.query.analytics import Analytics


class Exports:
    def __init__(self, analytics: Analytics):
        self.analytics = analytics

    def daily_csv(self, venue_id: str, day: str, snapshot_id: str | None = None) -> str:
        report = self.analytics.day(venue_id, day, snapshot_id)
        output = io.StringIO()
        fields = ["venue_id", "local_day", "snapshot_id", "generation", "exhibit_id", "name", "maker",
                  "visit_events", "visitors", "feedback_events", "unique_feedback_visitors",
                  "rating_average", "demo_events", "demo_seconds", "channels"]
        writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for exhibit in report["exhibits"]:
            row = {**{name: report[name] for name in fields[:4]}, **exhibit}
            row["channels"] = json.dumps(row["channels"], sort_keys=True)
            writer.writerow({name: row.get(name) for name in fields})
        return output.getvalue()

    def events_csv(self, venue_id: str, day: str, snapshot_id: str | None = None,
                   *, exhibit_id: str | None = None, kind: str | None = None) -> str:
        first = self.analytics.events(venue_id, day, snapshot_id, exhibit_id=exhibit_id, kind=kind, limit=500)
        output = io.StringIO()
        fields = ["event_id", "source_id", "exhibit_id", "event_at", "local_day", "kind", "value_num", "category", "visitor_hash"]
        writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for offset in range(0, first["total"], 500):
            page = first if offset == 0 else self.analytics.events(venue_id, day, first["snapshot_id"],
                    exhibit_id=exhibit_id, kind=kind, offset=offset, limit=500)
            writer.writerows({field: row.get(field) for field in fields} for row in page["events"])
        return output.getvalue()
