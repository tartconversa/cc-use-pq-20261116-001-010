"""计算已发布体验指标，明确分母和缺失信号。"""
from __future__ import annotations

import json
from collections import defaultdict
from statistics import median

from exhibit_lake.catalog.registry import iso_day
from exhibit_lake.catalog.store import Store
from exhibit_lake.errors import InvalidInput, NotFound
from exhibit_lake.query.analytics import Analytics


LEGACY_METRIC_KEY = "experience_conversion"


class Experience:
    def __init__(self, store: Store, analytics: Analytics):
        self.store = store
        self.analytics = analytics

    def _conversion(self, conn, snapshot_id: str, exhibit_id: str,
                    legacy_value: float | None) -> dict:
        """区分即时汇总值与已发布口径，历史报表可按版本解释。

        未绑定口径时只返回 unversioned_value，避免把临时算法当成对外定义。
        """
        publication = Store.one(conn, "SELECT * FROM metric_publications WHERE snapshot_id=? AND metric_key=? "
                                "AND active=1", (snapshot_id, LEGACY_METRIC_KEY))
        if not publication:
            return {"metric_key": LEGACY_METRIC_KEY, "published": False, "result_id": None,
                    "definition": None, "value": None, "status": "no_definition",
                    "reason": "no_metric_definition_published",
                    "unversioned_value": legacy_value,
                    "note": "no metric definition is published for this snapshot"}
        result = Store.one(conn, "SELECT * FROM metric_results WHERE result_id=?", (publication["result_id"],))
        payload = json.loads(result["result_json"])
        breakdown = next((item for item in payload["exhibits"] if item["exhibit_id"] == exhibit_id), None)
        return {"metric_key": LEGACY_METRIC_KEY, "published": True, "result_id": result["result_id"],
                "result_version": result["result_version"],
                "definition": json.loads(result["definition_json"]),
                "input_digest": result["input_digest"],
                "value": breakdown["value"] if breakdown else None,
                "status": breakdown["status"] if breakdown else "no_data",
                "reason": breakdown["reason"] if breakdown else "exhibit_not_observed",
                "unversioned_value": legacy_value}

    def exhibit(self, venue_id: str, day: str, exhibit_id: str,
                snapshot_id: str | None = None) -> dict:
        iso_day(day, "local_day")
        with self.store.transaction(write=False) as conn:
            exhibit = Store.one(conn, "SELECT * FROM exhibits WHERE exhibit_id=? AND venue_id=?",
                                (exhibit_id, venue_id))
            if not exhibit:
                raise NotFound("exhibit not found at venue")
            snapshot = self.analytics._resolve(conn, venue_id, day, snapshot_id)
            events = [x for x in self.analytics._records(conn, snapshot["snapshot_id"])
                      if x["exhibit_id"] == exhibit_id]
            visits = [x for x in events if x["kind"] == "visit"]
            feedback = [x for x in events if x["kind"] == "feedback"]
            demos = [x for x in events if x["kind"] == "demo"]
            visitor_total = sum(int(x["value_num"]) for x in visits)
            ratings = [int(x["value_num"]) for x in feedback]
            rating_buckets = {str(score): ratings.count(score) for score in range(1, 6)}
            legacy_value = (round(len(feedback) / visitor_total, 6)
                            if feedback and visitor_total else None)
            by_channel = defaultdict(lambda: {"visitors": 0, "ratings": [], "demos": 0})
            for event in events:
                channel = event["category"] or "unspecified"
                group = by_channel[channel]
                if event["kind"] == "visit":
                    group["visitors"] += int(event["value_num"])
                elif event["kind"] == "feedback":
                    group["ratings"].append(int(event["value_num"]))
                else:
                    group["demos"] += 1
            channels = []
            for name in sorted(by_channel):
                group = by_channel[name]
                channels.append({"channel": name, "visitors": group["visitors"],
                                 "feedback": len(group["ratings"]), "demos": group["demos"],
                                 "rating_average": round(sum(group["ratings"]) / len(group["ratings"]), 4)
                                 if group["ratings"] else None,
                                 "feedback_per_visitor": (round(len(group["ratings"]) / group["visitors"], 6)
                                                          if group["ratings"] and group["visitors"] else None)})
            conversion = self._conversion(conn, snapshot["snapshot_id"], exhibit_id, legacy_value)
            return {"venue_id": venue_id, "local_day": day, "snapshot_id": snapshot["snapshot_id"],
                    "exhibit_id": exhibit_id, "exhibit_name": exhibit["name"],
                    "visitors": visitor_total, "visit_events": len(visits),
                    "feedback": len(feedback), "unique_feedback_visitors": len({x["visitor_hash"] for x in feedback}),
                    "rating_histogram": rating_buckets,
                    "rating_average": round(sum(ratings) / len(ratings), 4) if ratings else None,
                    "rating_median": median(ratings) if ratings else None,
                    "feedback_per_visitor": round(len(feedback) / visitor_total, 6)
                    if feedback and visitor_total else None,
                    "conversion": conversion,
                    "demos": len(demos), "demo_seconds": round(sum(x["value_num"] for x in demos), 3),
                    "channels": channels}

    def compare_exhibits(self, venue_id: str, day: str, exhibit_ids: list[str],
                         snapshot_id: str | None = None) -> dict:
        if not isinstance(exhibit_ids, list) or not 1 <= len(exhibit_ids) <= 20:
            raise InvalidInput("compare between 1 and 20 exhibits")
        if len(exhibit_ids) != len(set(exhibit_ids)):
            raise InvalidInput("exhibit comparison contains duplicates")
        with self.store.transaction(write=False) as conn:
            snapshot = self.analytics._resolve(conn, venue_id, day, snapshot_id)
            chosen = snapshot["snapshot_id"]
        reports = [self.exhibit(venue_id, day, exhibit_id, chosen) for exhibit_id in exhibit_ids]
        ranked = sorted(reports, key=lambda x: (-x["visitors"], x["exhibit_id"]))
        return {"venue_id": venue_id, "local_day": day, "snapshot_id": chosen,
                "exhibits": reports, "traffic_order": [x["exhibit_id"] for x in ranked],
                "total_visitors": sum(x["visitors"] for x in reports)}
