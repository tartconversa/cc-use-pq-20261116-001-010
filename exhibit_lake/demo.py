"""使用合成事件的离线端到端演示。"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from exhibit_lake.service import ExhibitLake


def main() -> None:
    with tempfile.TemporaryDirectory() as folder:
        lake = ExhibitLake(Path(folder) / "demo.db")
        lake.register_venue("showroom", "Synthetic Innovation Showroom", "Asia/Shanghai")
        lake.register_exhibit("robot", "showroom", "Synthetic Robot", "Sample Maker", "2026-01-01")
        lake.register_source("counter", "showroom", "visit")
        lake.register_source("kiosk", "showroom", "feedback")
        lake.register_contract("count-v1", "counter", ["event_id", "exhibit_id", "event_at", "count"],
                               "2026-01-01T00:00:00+00:00")
        lake.register_contract("rating-v1", "kiosk", ["event_id", "exhibit_id", "event_at", "rating", "visitor_id"],
                               "2026-01-01T00:00:00+00:00")
        lake.ingest("counter", "count-v1", "batch-1", [{"event_id": "v1", "exhibit_id": "robot",
                   "event_at": "2026-03-01T10:00:00+08:00", "count": 12}])
        lake.ingest("kiosk", "rating-v1", "batch-2", [{"event_id": "f1", "exhibit_id": "robot",
                   "event_at": "2026-03-01T10:30:00+08:00", "rating": 5, "visitor_id": "visitor-1"}])
        lake.ingest("kiosk", "rating-v1", "batch-3", [{"event_id": "f2", "exhibit_id": "robot",
                   "event_at": "2026-03-01T14:30:00+08:00", "rating": 4, "visitor_id": "visitor-1"}])
        publication = lake.publish_day("showroom", "2026-03-01")
        result = lake.analytics.day("showroom", "2026-03-01")
        assert result["total_visitors"] == 12 and result["total_feedback"] == 2
        assert lake.snapshots.verify(publication["snapshot_id"])["valid"]

        # 指标口径与结果版本：旧口径绑定历史快照，新口径显式替换并保留旧绑定。
        old = lake.metrics.define(
            "experience_conversion", "体验转化率",
            {"numerator": {"kind": "feedback", "measure": "events"},
             "denominator": {"kind": "visit", "measure": "value_sum"}, "round": 6},
            "历史对外口径：反馈事件数除以访客人数")
        first = lake.metrics.recompute("experience_conversion", "showroom", "2026-03-01",
                                       publication["snapshot_id"], "demo-old", old["version"])
        bound = lake.metrics.publish(first["result"]["result_id"], reason="历史报表")
        new = lake.metrics.define(
            "experience_conversion", "体验转化率",
            {"numerator": {"kind": "feedback", "measure": "unique_visitors"},
             "denominator": {"kind": "visit", "measure": "value_sum"}, "round": 6},
            "新口径：按独立反馈访客计转化")
        second = lake.metrics.recompute("experience_conversion", "showroom", "2026-03-01",
                                        publication["snapshot_id"], "demo-new", new["version"])
        replaced = lake.metrics.publish(second["result"]["result_id"],
                                        expected_publication_id=bound["publication_id"],
                                        reason="切换为独立访客口径")
        assert first["result"]["value"] == round(2 / 12, 6)
        assert second["result"]["value"] == round(1 / 12, 6)
        assert replaced["definition_version"] == 2
        historical = lake.metrics.report("showroom", "2026-03-01", "experience_conversion",
                                         definition_version=1)
        assert historical["publication"]["publication_id"] == bound["publication_id"]
        assert not historical["publication"]["active"]
        assert lake.metrics.trace(second["result"]["result_id"])["snapshot"]["members"] == 3
        print(json.dumps({"publication": publication["snapshot_id"], "report": result,
                          "metric_result": {"result_id": second["result"]["result_id"],
                                            "definition_version": 2,
                                            "value": second["result"]["value"]},
                          "historical_definition_version": historical["definition"]["version"]},
                         ensure_ascii=False))


if __name__ == "__main__":
    main()
