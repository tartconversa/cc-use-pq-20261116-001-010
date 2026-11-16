"""按不可变口径计算分区级体验转化率。"""
from __future__ import annotations

from collections import defaultdict


def _measure(side: dict, events: list[dict]) -> dict:
    """返回单侧度量值；observations 明确区分“没有观测”与“观测为零”。"""
    selected = [event for event in events if event["kind"] == side["kind"]]
    measure = side["measure"]
    if measure == "events":
        return {"kind": side["kind"], "measure": measure,
                "value": len(selected), "observations": len(selected)}
    if measure == "unique_visitors":
        identities = {event["visitor_hash"] for event in selected if event["visitor_hash"] is not None}
        observed = sum(1 for event in selected if event["visitor_hash"] is not None)
        return {"kind": side["kind"], "measure": measure,
                "value": len(identities) if observed else None, "observations": observed}
    values = [event["value_num"] for event in selected if event["value_num"] is not None]
    if not values:
        return {"kind": side["kind"], "measure": measure, "value": None, "observations": 0}
    if measure == "value_sum":
        return {"kind": side["kind"], "measure": measure,
                "value": round(sum(values), 6), "observations": len(values)}
    return {"kind": side["kind"], "measure": measure,
            "value": round(sum(values) / len(values), 6), "observations": len(values)}


def _ratio(numerator: dict, denominator: dict, digits: int) -> dict:
    """计算比值；任一侧没有有效观测时返回 null，绝不把缺失信号折算成 0。"""
    if numerator["observations"] == 0 or numerator["value"] is None:
        return {"value": None, "status": "no_data", "reason": "no_numerator_observations"}
    if denominator["observations"] == 0 or denominator["value"] is None:
        return {"value": None, "status": "no_data", "reason": "no_denominator_observations"}
    if denominator["value"] == 0:
        return {"value": None, "status": "no_data", "reason": "zero_denominator"}
    return {"value": round(numerator["value"] / denominator["value"], digits),
            "status": "ok", "reason": None}


def calculate(venue_id: str, local_day: str, definition: dict, records: list[dict],
              *, scope: list[str] | None = None) -> dict:
    """对给定快照成员做确定性汇总，并按展品给出同样口径的分解。"""
    spec = definition["spec"]
    digits = spec["round"]

    def block(events: list[dict]) -> dict:
        numerator = _measure(spec["numerator"], events)
        denominator = _measure(spec["denominator"], events)
        return {"numerator": numerator, "denominator": denominator,
                **_ratio(numerator, denominator, digits)}

    groups = defaultdict(list)
    for record in records:
        groups[record["exhibit_id"]].append(record)
    exhibits = [{"exhibit_id": exhibit_id, **block(groups[exhibit_id])}
                for exhibit_id in sorted(groups)]
    return {"metric_key": definition["metric_key"], "venue_id": venue_id, "local_day": local_day,
            "definition": {"metric_key": definition["metric_key"], "version": definition["version"],
                           "digest": definition["digest"], "name": definition["name"],
                           "spec": spec},
            **block(records), "scope": scope or None,
            "observed_exhibits": [item["exhibit_id"] for item in exhibits
                                  if item["numerator"]["observations"] or item["denominator"]["observations"]],
            "exhibits": exhibits}


def exhibits_without_data(venue_exhibit_ids: list[str], result: dict) -> list[str]:
    """报告口径范围内未出现在观测中的展品，便于区分“零转化”和“没有数据”。"""
    observed = {item["exhibit_id"] for item in result["exhibits"]}
    return sorted(set(venue_exhibit_ids) - observed)
