"""指标定义、分区重算、结果版本与发布绑定的编排。"""
from __future__ import annotations

import json
import uuid

from exhibit_lake.catalog.registry import iso_day, now, require_id
from exhibit_lake.catalog.store import Store
from exhibit_lake.errors import Conflict, IntegrityFailure, InvalidInput, NotFound, StateError
from exhibit_lake.ingest.validation import canonical, digest
from exhibit_lake.metrics.compute import calculate, exhibits_without_data
from exhibit_lake.metrics.definitions import MetricDefinitions, definition_digest, validate_spec
from exhibit_lake.publish.snapshots import Snapshots


def _scope(exhibit_ids: list[str] | None) -> list[str]:
    if exhibit_ids is None:
        return []
    if not isinstance(exhibit_ids, list) or not 1 <= len(exhibit_ids) <= 20:
        raise InvalidInput("scope must list between 1 and 20 exhibits")
    if len(exhibit_ids) != len(set(exhibit_ids)):
        raise InvalidInput("scope contains duplicate exhibits")
    for exhibit_id in exhibit_ids:
        require_id(exhibit_id, "exhibit_id")
    return sorted(exhibit_ids)


class Metrics:
    def __init__(self, store: Store, definitions: MetricDefinitions, snapshots: Snapshots):
        self.store = store
        self.definitions = definitions
        self.snapshots = snapshots

    # -- 定义 ---------------------------------------------------------------

    def define(self, metric_key: str, name: str, spec: object, description: str | None = None) -> dict:
        return self.definitions.register(metric_key, name, spec, description)

    def definition(self, metric_key: str, version: int | None = None) -> dict:
        return self.definitions.resolve(metric_key, version)

    def definition_versions(self, metric_key: str) -> dict:
        return self.definitions.versions(metric_key)

    def catalog(self) -> dict:
        return self.definitions.catalog()

    # -- 输入指纹 -----------------------------------------------------------

    def _input(self, conn, snapshot: dict) -> dict:
        records = self.snapshots.records_in(conn, snapshot["snapshot_id"])
        members = self.snapshots.members_in(conn, snapshot["snapshot_id"])
        members_digest = digest(members)
        input_digest = digest([{key: record[key] for key in (
            "source_id", "event_id", "payload_hash", "exhibit_id", "event_at", "local_day",
            "kind", "value_num", "category", "visitor_hash")} for record in records])
        return {"snapshot_id": snapshot["snapshot_id"], "snapshot_digest": snapshot["digest"],
                "snapshot_generation": snapshot["generation"], "parent_id": snapshot["parent_id"],
                "members_digest": members_digest, "input_digest": input_digest,
                "input_event_count": len(records), "records": records}

    # -- 重算 ---------------------------------------------------------------

    def recompute(self, metric_key: str, venue_id: str, local_day: str, snapshot_id: str | None,
                  request_key: str, definition_version: int | None = None,
                  exhibit_ids: list[str] | None = None, *, defer: bool = False,
                  definition: dict | None = None) -> dict:
        """创建（并默认立即执行）重算任务。

        任务在创建时钉住口径版本与快照输入摘要；snapshot_id 省略时使用该分区
        当前已发布头，defer=True 时只登记任务，稍后通过 retry 执行。
        definition 允许直接携带新的口径内容，登记为不可变版本后参与本次重算。
        """
        require_id(metric_key, "metric_key")
        require_id(venue_id, "venue_id")
        require_id(request_key, "request_key")
        if snapshot_id is not None:
            require_id(snapshot_id, "snapshot_id")
        iso_day(local_day, "local_day")
        if definition is not None and definition_version is not None:
            raise InvalidInput("provide either definition or definition_version, not both")
        inline = None
        if definition is not None:
            if not isinstance(definition, dict) or set(definition) - {"name", "spec", "description"} \
                    or "name" not in definition or "spec" not in definition:
                raise InvalidInput("inline definition requires name and spec")
            resolved = validate_spec(definition["spec"])
            name = definition["name"]
            if not isinstance(name, str) or not 1 <= len(name.strip()) <= 120:
                raise InvalidInput("metric name must be a nonempty string of at most 120 characters")
            description = definition.get("description")
            if description is not None and (not isinstance(description, str) or len(description) > 500):
                raise InvalidInput("metric description must be text of at most 500 characters")
            inline = {"name": name.strip(), "spec": resolved, "description": description,
                      "digest": definition_digest(name.strip(), resolved, description)}
        existing_task = self._existing_task(metric_key, venue_id, local_day, request_key)
        if inline is not None and existing_task is None:
            # 先确认没有既有任务再登记，避免重放冲突留下无人使用的新口径版本。
            registered = self.definitions.register(metric_key, inline["name"], inline["spec"],
                                                   inline["description"])
            definition_version = registered["version"]
        failure = None
        with self.store.transaction() as conn:
            task = Store.one(conn, "SELECT * FROM metric_recompute_tasks WHERE metric_key=? AND venue_id=? "
                             "AND local_day=? AND request_key=?", (metric_key, venue_id, local_day, request_key))
            if task is None:
                definition_row = self.definitions._resolve(conn, metric_key, definition_version)
                snapshot, venue_exhibits = self._resolve_input(conn, venue_id, local_day, snapshot_id)
                scope = _scope(exhibit_ids)
                self._validate_scope(conn, venue_id, scope)
                input_state = self._input(conn, snapshot)
                task = self._new_task(conn, metric_key, definition_row, request_key, venue_id, local_day,
                                      snapshot, input_state, scope)
                if defer:
                    return {**self._describe_task(conn, task), "replay": False}
            else:
                if inline is not None and inline["digest"] != task["definition_digest"]:
                    raise Conflict("request key already used for a different recompute", details={
                        "task_id": task["task_id"],
                        "mismatches": {"definition": {"pinned": task["definition_digest"],
                                                      "requested": inline["digest"]}}})
                # 已有任务的重放沿用创建时钉住的目标：只有显式不同的参数才算冲突，
                # 否则定义变更会让原本幂等的重试失败。
                definition_row = self.definitions._get(
                    conn, metric_key,
                    definition_version if definition_version is not None else task["definition_version"])
                target_snapshot = snapshot_id if snapshot_id is not None else task["snapshot_id"]
                snapshot, venue_exhibits = self._resolve_input(conn, venue_id, local_day, target_snapshot)
                scope = _scope(exhibit_ids) if exhibit_ids is not None else json.loads(task["scope_json"])
                if exhibit_ids is not None:
                    self._validate_scope(conn, venue_id, scope)
                self._check_replay(task, definition_row, snapshot, scope)
                if task["status"] == "succeeded" or defer:
                    return {**self._describe_task(conn, task), "replay": True}
                input_state = self._input(conn, snapshot)
            failure = self._attempt(conn, task, definition_row, snapshot, input_state, scope,
                                    venue_exhibits, venue_id, local_day)
        if failure:
            raise failure["error"]
        with self.store.transaction(write=False) as conn:
            current = Store.one(conn, "SELECT * FROM metric_recompute_tasks WHERE task_id=?", (task["task_id"],))
            return {**self._describe_task(conn, current), "replay": False}

    def _existing_task(self, metric_key: str, venue_id: str, day: str, request_key: str) -> dict | None:
        with self.store.transaction(write=False) as conn:
            return Store.one(conn, "SELECT * FROM metric_recompute_tasks WHERE metric_key=? AND venue_id=? "
                              "AND local_day=? AND request_key=?", (metric_key, venue_id, day, request_key))

    def _resolve_input(self, conn, venue_id: str, day: str, snapshot_id: str | None) -> tuple[dict, list[str]]:
        if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
            raise NotFound("venue not found")
        if snapshot_id is None:
            part = Store.one(conn, "SELECT * FROM partitions WHERE venue_id=? AND local_day=?", (venue_id, day))
            if not part or not part["head_id"]:
                raise NotFound("published partition not found")
            snapshot_id = part["head_id"]
        snapshot = Store.one(conn, "SELECT * FROM snapshots WHERE snapshot_id=?", (snapshot_id,))
        if not snapshot or snapshot["venue_id"] != venue_id or snapshot["local_day"] != day:
            raise NotFound("snapshot not found in requested partition")
        if snapshot["status"] != "published":
            raise StateError("only published snapshots can be recomputed")
        exhibits = [row["exhibit_id"] for row in Store.many(
            conn, "SELECT exhibit_id FROM exhibits WHERE venue_id=? ORDER BY exhibit_id", (venue_id,))]
        return snapshot, exhibits

    def _validate_scope(self, conn, venue_id: str, scope: list[str]) -> None:
        if not scope:
            return
        known = {row["exhibit_id"] for row in Store.many(
            conn, "SELECT exhibit_id FROM exhibits WHERE venue_id=?", (venue_id,))}
        missing = sorted(set(scope) - known)
        if missing:
            raise NotFound("scope exhibit not found at venue", details={"missing": missing})

    def _new_task(self, conn, metric_key: str, definition: dict, request_key: str, venue_id: str,
                  day: str, snapshot: dict, input_state: dict, scope: list[str]) -> dict:
        stamp = now()
        task_id = uuid.uuid4().hex
        conn.execute("INSERT INTO metric_recompute_tasks(task_id,request_key,metric_key,definition_version,"
                     "definition_digest,venue_id,local_day,snapshot_id,snapshot_digest,members_digest,"
                     "input_generation,input_event_count,input_digest,scope_json,status,attempts,result_id,"
                     "error_json,created_at,updated_at,finished_at) "
                     "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                     (task_id, request_key, metric_key, definition["version"], definition["digest"],
                      venue_id, day, snapshot["snapshot_id"], snapshot["digest"], input_state["members_digest"],
                      snapshot["generation"], input_state["input_event_count"], input_state["input_digest"],
                      canonical(scope), "pending", 0, None, None, stamp, stamp, None))
        Store.audit(conn, "metric.recompute.requested", task_id,
                    {"metric_key": metric_key, "definition_version": definition["version"],
                     "snapshot_id": snapshot["snapshot_id"], "scope": scope}, stamp)
        return Store.one(conn, "SELECT * FROM metric_recompute_tasks WHERE task_id=?", (task_id,))

    def _check_replay(self, task: dict, definition: dict, snapshot: dict, scope: list[str]) -> None:
        mismatches = {}
        if task["definition_version"] != definition["version"]:
            mismatches["definition_version"] = {"pinned": task["definition_version"],
                                                "requested": definition["version"]}
        if task["snapshot_id"] != snapshot["snapshot_id"]:
            mismatches["snapshot_id"] = {"pinned": task["snapshot_id"], "requested": snapshot["snapshot_id"]}
        if json.loads(task["scope_json"]) != scope:
            mismatches["scope"] = {"pinned": json.loads(task["scope_json"]), "requested": scope}
        if mismatches:
            raise Conflict("request key already used for a different recompute", details={
                "task_id": task["task_id"], "mismatches": mismatches})

    def _attempt(self, conn, task: dict, definition: dict, snapshot: dict, input_state: dict,
                 scope: list[str], venue_exhibits: list[str], venue_id: str, day: str) -> dict | None:
        stamp = now()
        attempts = task["attempts"] + 1
        conn.execute("UPDATE metric_recompute_tasks SET status='running',attempts=?,updated_at=? WHERE task_id=?",
                     (attempts, stamp, task["task_id"]))
        try:
            verified = self.snapshots.verify_in(conn, snapshot["snapshot_id"])
            drifted = []
            if verified["digest"] != task["snapshot_digest"]:
                drifted.append("snapshot_digest")
            if input_state["members_digest"] != task["members_digest"]:
                drifted.append("members_digest")
            if input_state["input_digest"] != task["input_digest"]:
                drifted.append("input_digest")
            if input_state["input_event_count"] != task["input_event_count"]:
                drifted.append("input_event_count")
            if drifted:
                raise IntegrityFailure("snapshot input changed since recompute was requested",
                                       details={"task_id": task["task_id"], "changed": drifted})
            records = input_state["records"]
            if scope:
                records = [record for record in records if record["exhibit_id"] in set(scope)]
            result = calculate(venue_id, day, definition, records, scope=scope)
            result["unobserved_exhibits"] = exhibits_without_data(
                [x for x in venue_exhibits if not scope or x in set(scope)], result)
            calculation_key = digest({
                "definition_digest": definition["digest"],
                "snapshot_digest": snapshot["digest"],
                "members_digest": input_state["members_digest"],
                "input_digest": input_state["input_digest"],
                "scope": scope,
            })
            existing = Store.one(conn, "SELECT * FROM metric_results WHERE metric_key=? AND venue_id=? "
                                 "AND local_day=? AND calculation_key=?",
                                 (task["metric_key"], venue_id, day, calculation_key))
            deduplicated = existing is not None
            if existing:
                result_id = existing["result_id"]
            else:
                result_id = uuid.uuid4().hex
                result_version = Store.one(conn, "SELECT COALESCE(MAX(result_version),0) AS version FROM metric_results "
                                           "WHERE metric_key=? AND venue_id=? AND local_day=?",
                                           (task["metric_key"], venue_id, day))["version"] + 1
                definition_json = {"metric_key": definition["metric_key"], "version": definition["version"],
                                   "digest": definition["digest"], "name": definition["name"],
                                   "description": definition["description"], "spec": definition["spec"]}
                conn.execute("INSERT INTO metric_results(result_id,metric_key,definition_version,definition_digest,"
                             "venue_id,local_day,result_version,snapshot_id,snapshot_digest,members_digest,"
                             "input_generation,input_event_count,input_digest,task_id,definition_json,result_json,"
                             "scope_json,calculation_key,result_digest,created_at) "
                             "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                             (result_id, task["metric_key"], definition["version"], definition["digest"],
                              venue_id, day, result_version, snapshot["snapshot_id"], snapshot["digest"],
                              input_state["members_digest"], snapshot["generation"],
                              input_state["input_event_count"], input_state["input_digest"], task["task_id"],
                              canonical(definition_json), canonical(result), canonical(scope),
                              calculation_key, digest(result), stamp))
            conn.execute("UPDATE metric_recompute_tasks SET status='succeeded',result_id=?,error_json=NULL,"
                         "updated_at=?,finished_at=? WHERE task_id=?", (result_id, stamp, stamp, task["task_id"]))
            Store.audit(conn, "metric.recompute.succeeded", task["task_id"],
                        {"result_id": result_id, "deduplicated": deduplicated, "definition_version": definition["version"],
                         "snapshot_id": snapshot["snapshot_id"], "attempts": attempts}, stamp)
            return None
        except IntegrityFailure as exc:
            conn.execute("UPDATE metric_recompute_tasks SET status='failed',error_json=?,updated_at=?,"
                         "finished_at=? WHERE task_id=?",
                         (canonical({"code": exc.code, "message": str(exc), "details": exc.details}),
                          stamp, stamp, task["task_id"]))
            Store.audit(conn, "metric.recompute.failed", task["task_id"],
                        {"code": exc.code, "attempts": attempts, **exc.details}, stamp)
            return {"error": exc}
        except Exception as exc:  # 保留失败证据，避免任务永久停在 running。
            conn.execute("UPDATE metric_recompute_tasks SET status='failed',error_json=?,updated_at=?,"
                         "finished_at=? WHERE task_id=?",
                         (canonical({"code": "internal_error", "message": type(exc).__name__,
                                     "details": {"task_id": task["task_id"]}}), stamp, stamp, task["task_id"]))
            Store.audit(conn, "metric.recompute.failed", task["task_id"],
                        {"code": "internal_error", "attempts": attempts}, stamp)
            return {"error": exc}

    def _describe_task(self, conn, task: dict) -> dict:
        """任务视图同时暴露计算口径与输入版本，便于解释每次重试。"""
        definition = Store.one(conn, "SELECT * FROM metric_definitions WHERE metric_key=? AND version=?",
                               (task["metric_key"], task["definition_version"]))
        latest_definition = self.definitions._latest(conn, task["metric_key"])
        part = Store.one(conn, "SELECT * FROM partitions WHERE venue_id=? AND local_day=?",
                         (task["venue_id"], task["local_day"]))
        head_id = part["head_id"] if part else None
        payload = {"task_id": task["task_id"], "request_key": task["request_key"],
                   "metric_key": task["metric_key"], "status": task["status"], "attempts": task["attempts"],
                   "venue_id": task["venue_id"], "local_day": task["local_day"],
                   "definition": {"metric_key": task["metric_key"], "version": task["definition_version"],
                                  "digest": task["definition_digest"],
                                  "latest_version": latest_definition["version"],
                                  "superseded": latest_definition["version"] != task["definition_version"],
                                  "spec": json.loads(definition["spec_json"]) if definition else None},
                   "input": {"snapshot_id": task["snapshot_id"], "snapshot_digest": task["snapshot_digest"],
                             "members_digest": task["members_digest"], "generation": task["input_generation"],
                             "event_count": task["input_event_count"], "input_digest": task["input_digest"],
                             "current_head_id": head_id,
                             "current_generation": part["generation"] if part else 0,
                             "superseded": head_id != task["snapshot_id"],
                             "scope": json.loads(task["scope_json"])},
                   "created_at": task["created_at"], "updated_at": task["updated_at"],
                   "finished_at": task["finished_at"]}
        if task["error_json"]:
            payload["error"] = json.loads(task["error_json"])
        # 顶层便捷字段与嵌套结构保持一致，方便报表和调用方直接引用版本。
        payload["definition_version"] = task["definition_version"]
        payload["definition_digest"] = task["definition_digest"]
        payload["snapshot_id"] = task["snapshot_id"]
        payload["input_generation"] = task["input_generation"]
        payload["input_event_count"] = task["input_event_count"]
        if task["result_id"]:
            payload["result"] = self._result_view(conn, Store.one(
                conn, "SELECT * FROM metric_results WHERE result_id=?", (task["result_id"],)))
        return payload

    def task(self, task_id: str) -> dict:
        require_id(task_id, "task_id")
        with self.store.transaction(write=False) as conn:
            task = Store.one(conn, "SELECT * FROM metric_recompute_tasks WHERE task_id=?", (task_id,))
            if not task:
                raise NotFound("recompute task not found")
            return self._describe_task(conn, task)

    def tasks(self, *, venue_id: str | None = None, local_day: str | None = None,
              metric_key: str | None = None, status: str | None = None,
              offset: int = 0, limit: int = 100) -> dict:
        if offset < 0 or not 1 <= limit <= 500:
            raise InvalidInput("invalid task page")
        sql = "SELECT task_id FROM metric_recompute_tasks WHERE 1=1"
        args: list[object] = []
        for column, value in (("venue_id", venue_id), ("local_day", local_day),
                              ("metric_key", metric_key), ("status", status)):
            if value is not None:
                sql += f" AND {column}=?"
                args.append(value)
        with self.store.transaction(write=False) as conn:
            total = Store.one(conn, sql.replace("SELECT task_id", "SELECT COUNT(*) AS n"), tuple(args))["n"]
            rows = Store.many(conn, sql + " ORDER BY created_at,task_id LIMIT ? OFFSET ?",
                              tuple(args + [limit, offset]))
            items = [self._describe_task(conn, Store.one(
                conn, "SELECT * FROM metric_recompute_tasks WHERE task_id=?", (row["task_id"],))) for row in rows]
        return {"total": total, "offset": offset, "limit": limit, "tasks": items}

    def retry(self, task_id: str) -> dict:
        """重试失败任务；沿用任务创建时钉住的口径版本与快照输入。"""
        require_id(task_id, "task_id")
        with self.store.transaction() as conn:
            task = Store.one(conn, "SELECT * FROM metric_recompute_tasks WHERE task_id=?", (task_id,))
            if not task:
                raise NotFound("recompute task not found")
            if task["status"] == "succeeded":
                return {**self._describe_task(conn, task), "replay": True}
            definition = self.definitions._get(conn, task["metric_key"], task["definition_version"])
            snapshot, venue_exhibits = self._resolve_input(
                conn, task["venue_id"], task["local_day"], task["snapshot_id"])
            input_state = self._input(conn, snapshot)
            failure = self._attempt(conn, task, definition, snapshot, input_state,
                                    json.loads(task["scope_json"]), venue_exhibits,
                                    task["venue_id"], task["local_day"])
            result = {**self._describe_task(conn, Store.one(
                conn, "SELECT * FROM metric_recompute_tasks WHERE task_id=?", (task_id,))), "replay": False}
        if failure:
            raise failure["error"]
        return result

    # -- 结果 ---------------------------------------------------------------

    def _result_view(self, conn, result: dict) -> dict:
        definition = json.loads(result["definition_json"])
        payload = json.loads(result["result_json"])
        input_state = {"snapshot_id": result["snapshot_id"], "snapshot_digest": result["snapshot_digest"],
                       "members_digest": result["members_digest"], "generation": result["input_generation"],
                       "event_count": result["input_event_count"], "input_digest": result["input_digest"],
                       "scope": json.loads(result["scope_json"])}
        snapshot = Store.one(conn, "SELECT * FROM snapshots WHERE snapshot_id=?", (result["snapshot_id"],))
        part = Store.one(conn, "SELECT * FROM partitions WHERE venue_id=? AND local_day=?",
                         (result["venue_id"], result["local_day"]))
        active = Store.one(conn, "SELECT * FROM metric_publications WHERE snapshot_id=? AND metric_key=? "
                            "AND active=1", (result["snapshot_id"], result["metric_key"]))
        binding = Store.one(conn, "SELECT * FROM metric_publications WHERE snapshot_id=? AND metric_key=? "
                            "AND result_id=? ORDER BY published_at DESC LIMIT 1",
                            (result["snapshot_id"], result["metric_key"], result["result_id"]))
        latest_definition = self.definitions._latest(conn, result["metric_key"])["version"]
        payload.update({
            "result_id": result["result_id"], "result_version": result["result_version"],
            "definition": definition, "task_id": result["task_id"], "calculation_key": result["calculation_key"],
            "latest_definition_version": latest_definition,
            "definition_superseded": latest_definition != result["definition_version"],
            "input": input_state,
            "input_snapshot_status": snapshot["status"] if snapshot else None,
            "current_head_id": part["head_id"] if part else None,
            "current_generation": part["generation"] if part else 0,
            "superseded": bool(part and part["head_id"] != result["snapshot_id"]),
            "publication_id": binding["publication_id"] if binding else None,
            "active_publication_id": active["publication_id"] if active else None,
            "published": bool(active and active["result_id"] == result["result_id"]),
            "result_digest": result["result_digest"], "created_at": result["created_at"],
        })
        payload["definition_version"] = result["definition_version"]
        payload["definition_digest"] = result["definition_digest"]
        payload["snapshot_id"] = result["snapshot_id"]
        payload["input_generation"] = result["input_generation"]
        return payload

    def result(self, result_id: str) -> dict:
        require_id(result_id, "result_id")
        with self.store.transaction(write=False) as conn:
            result = Store.one(conn, "SELECT * FROM metric_results WHERE result_id=?", (result_id,))
            if not result:
                raise NotFound("metric result not found")
            return self._result_view(conn, result)

    def trace(self, result_id: str) -> dict:
        """把结果串回定义、重算任务、快照成员与原始批次，形成可追溯链条。"""
        require_id(result_id, "result_id")
        with self.store.transaction(write=False) as conn:
            result = Store.one(conn, "SELECT * FROM metric_results WHERE result_id=?", (result_id,))
            if not result:
                raise NotFound("metric result not found")
            task = Store.one(conn, "SELECT * FROM metric_recompute_tasks WHERE task_id=?", (result["task_id"],))
            snapshot = Store.one(conn, "SELECT * FROM snapshots WHERE snapshot_id=?", (result["snapshot_id"],))
            batches = Store.many(conn, "SELECT b.batch_id,b.source_id,b.contract_id,COUNT(*) AS accepted_rows "
                                 "FROM snapshot_members m JOIN accepted_events e "
                                 "ON e.source_id=m.source_id AND e.event_id=m.event_id "
                                 "JOIN batches b ON b.batch_id=e.batch_id WHERE m.snapshot_id=? "
                                 "GROUP BY b.batch_id,b.source_id,b.contract_id ORDER BY b.source_id,b.batch_id",
                                 (result["snapshot_id"],))
            publications = Store.many(conn, "SELECT publication_id,active,published_at,retired_at "
                                      "FROM metric_publications WHERE result_id=? ORDER BY published_at",
                                      (result_id,))
            return {"result_id": result_id, "metric_key": result["metric_key"],
                    "result_version": result["result_version"], "result_digest": result["result_digest"],
                    "definition": json.loads(result["definition_json"]),
                    "task": self._describe_task(conn, task) if task else None,
                    "snapshot": {"snapshot_id": snapshot["snapshot_id"], "status": snapshot["status"],
                                 "generation": snapshot["generation"], "parent_id": snapshot["parent_id"],
                                 "digest": snapshot["digest"], "published_at": snapshot["published_at"],
                                 "members": Store.one(conn, "SELECT COUNT(*) AS n FROM snapshot_members "
                                                      "WHERE snapshot_id=?", (result["snapshot_id"],))["n"],
                                 "batches": batches},
                    "event_count": result["input_event_count"], "input_digest": result["input_digest"],
                    "publications": publications,
                    "result": self._result_view(conn, result)}

    def results(self, metric_key: str, venue_id: str, local_day: str) -> dict:
        require_id(metric_key, "metric_key")
        require_id(venue_id, "venue_id")
        iso_day(local_day, "local_day")
        with self.store.transaction(write=False) as conn:
            if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
                raise NotFound("venue not found")
            rows = Store.many(conn, "SELECT * FROM metric_results WHERE metric_key=? AND venue_id=? "
                              "AND local_day=? ORDER BY result_version", (metric_key, venue_id, local_day))
            return {"metric_key": metric_key, "venue_id": venue_id, "local_day": local_day,
                    "results": [self._result_view(conn, row) for row in rows]}

    # -- 发布绑定 -----------------------------------------------------------

    def publish(self, result_id: str, expected_publication_id: str | None = None,
                reason: str | None = None) -> dict:
        """把结果显式绑定到快照；替换旧报表必须声明预期绑定，避免静默覆盖。"""
        require_id(result_id, "result_id")
        if expected_publication_id is not None:
            require_id(expected_publication_id, "expected_publication_id")
        if reason is not None and (not isinstance(reason, str) or len(reason) > 500):
            raise InvalidInput("reason must be text of at most 500 characters")
        with self.store.transaction() as conn:
            result = Store.one(conn, "SELECT * FROM metric_results WHERE result_id=?", (result_id,))
            if not result:
                raise NotFound("metric result not found")
            scope = json.loads(result["scope_json"])
            if scope:
                # 局部展品重算不能冒充整馆报表，否则会静默改变对外口径的分母。
                raise StateError("scoped results cannot be published as the partition metric",
                                 details={"result_id": result_id, "scope": scope})
            snapshot = Store.one(conn, "SELECT * FROM snapshots WHERE snapshot_id=?", (result["snapshot_id"],))
            if not snapshot or snapshot["status"] != "published":
                raise StateError("only results of published snapshots can be publicized")
            active = Store.one(conn, "SELECT * FROM metric_publications WHERE snapshot_id=? AND metric_key=? "
                               "AND active=1", (result["snapshot_id"], result["metric_key"]))
            if active:
                if active["result_id"] == result_id:
                    return self._publication_view(conn, active)
                if expected_publication_id != active["publication_id"]:
                    raise Conflict("metric already has a published result for this snapshot",
                                   details={"snapshot_id": result["snapshot_id"], "metric_key": result["metric_key"],
                                            "active_publication_id": active["publication_id"],
                                            "active_result_id": active["result_id"],
                                            "active_definition_version": active["definition_version"]})
            elif expected_publication_id is not None:
                raise Conflict("no active metric publication to replace",
                               details={"snapshot_id": result["snapshot_id"], "metric_key": result["metric_key"]})
            stamp = now()
            publication_id = uuid.uuid4().hex
            if active:
                conn.execute("UPDATE metric_publications SET active=0,retired_at=?,retire_reason=? "
                             "WHERE publication_id=?",
                             (stamp, reason or "superseded", active["publication_id"]))
                Store.audit(conn, "metric.publication.retired", active["publication_id"],
                            {"result_id": active["result_id"], "definition_version": active["definition_version"],
                             "replaced_by": result_id, "reason": reason or "superseded"}, stamp)
            conn.execute("INSERT INTO metric_publications(publication_id,snapshot_id,metric_key,result_id,"
                         "definition_version,definition_digest,active,published_at,retired_at,retire_reason) "
                         "VALUES(?,?,?,?,?,?,1,?,NULL,NULL)",
                         (publication_id, result["snapshot_id"], result["metric_key"], result_id,
                          result["definition_version"], result["definition_digest"], stamp))
            Store.audit(conn, "metric.publication.created", publication_id,
                        {"snapshot_id": result["snapshot_id"], "metric_key": result["metric_key"],
                         "result_id": result_id, "definition_version": result["definition_version"],
                         "replaces": active["publication_id"] if active else None}, stamp)
            row = Store.one(conn, "SELECT * FROM metric_publications WHERE publication_id=?", (publication_id,))
            return self._publication_view(conn, row)

    def _publication_view(self, conn, row: dict) -> dict:
        result = Store.one(conn, "SELECT * FROM metric_results WHERE result_id=?", (row["result_id"],))
        return {"publication_id": row["publication_id"], "snapshot_id": row["snapshot_id"],
                "metric_key": row["metric_key"], "result_id": row["result_id"],
                "definition_version": row["definition_version"],
                "definition_digest": row["definition_digest"], "active": bool(row["active"]),
                "published_at": row["published_at"], "retired_at": row["retired_at"],
                "retire_reason": row["retire_reason"],
                "result": self._result_view(conn, result) if result else None}

    def publications(self, snapshot_id: str, *, metric_key: str | None = None) -> dict:
        require_id(snapshot_id, "snapshot_id")
        with self.store.transaction(write=False) as conn:
            snapshot = Store.one(conn, "SELECT * FROM snapshots WHERE snapshot_id=?", (snapshot_id,))
            if not snapshot:
                raise NotFound("snapshot not found")
            sql = "SELECT * FROM metric_publications WHERE snapshot_id=?"
            args: list[object] = [snapshot_id]
            if metric_key:
                sql += " AND metric_key=?"
                args.append(metric_key)
            rows = Store.many(conn, sql + " ORDER BY published_at,publication_id", tuple(args))
            return {"snapshot_id": snapshot_id, "venue_id": snapshot["venue_id"],
                    "local_day": snapshot["local_day"], "generation": snapshot["generation"],
                    "publications": [self._publication_view(conn, row) for row in rows]}

    # -- 报表读取 -----------------------------------------------------------

    def _head_or_snapshot(self, conn, venue_id: str, day: str, snapshot_id: str | None) -> dict:
        if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
            raise NotFound("venue not found")
        if snapshot_id is None:
            part = Store.one(conn, "SELECT * FROM partitions WHERE venue_id=? AND local_day=?", (venue_id, day))
            if not part or not part["head_id"]:
                raise NotFound("published partition not found")
            snapshot_id = part["head_id"]
        snapshot = Store.one(conn, "SELECT * FROM snapshots WHERE snapshot_id=?", (snapshot_id,))
        if not snapshot or snapshot["venue_id"] != venue_id or snapshot["local_day"] != day:
            raise NotFound("snapshot not found in requested partition")
        if snapshot["status"] != "published":
            raise StateError("snapshot is not published")
        return snapshot

    def report(self, venue_id: str, day: str, metric_key: str,
               snapshot_id: str | None = None, definition_version: int | None = None) -> dict:
        """读取已发布口径；指定定义版本时只解释该版本，不自动采用最新定义。"""
        require_id(metric_key, "metric_key")
        iso_day(day, "local_day")
        with self.store.transaction(write=False) as conn:
            snapshot = self._head_or_snapshot(conn, venue_id, day, snapshot_id)
            if definition_version is None:
                publication = Store.one(conn, "SELECT * FROM metric_publications WHERE snapshot_id=? "
                                        "AND metric_key=? AND active=1", (snapshot["snapshot_id"], metric_key))
                if not publication:
                    raise NotFound("metric is not published for this snapshot", details={
                        "snapshot_id": snapshot["snapshot_id"], "metric_key": metric_key,
                        "definition_version": None})
                result = Store.one(conn, "SELECT * FROM metric_results WHERE result_id=?",
                                   (publication["result_id"],))
                return {**self._result_view(conn, result), "publication": self._publication_view(conn, publication)}
            definition = self.definitions._get(conn, metric_key, definition_version)
            # 解释过去对外报表时优先返回当时实际绑定的结果，其次才回退到该口径的最新计算。
            publication = Store.one(conn, "SELECT * FROM metric_publications WHERE snapshot_id=? "
                                    "AND metric_key=? AND definition_version=? "
                                    "ORDER BY active DESC,published_at DESC LIMIT 1",
                                    (snapshot["snapshot_id"], metric_key, definition["version"]))
            if publication:
                result = Store.one(conn, "SELECT * FROM metric_results WHERE result_id=?",
                                   (publication["result_id"],))
            else:
                result = Store.one(conn, "SELECT * FROM metric_results WHERE snapshot_id=? AND metric_key=? "
                                   "AND definition_version=? ORDER BY result_version DESC LIMIT 1",
                                   (snapshot["snapshot_id"], metric_key, definition["version"]))
            if not result:
                raise NotFound("no result for requested definition version", details={
                    "snapshot_id": snapshot["snapshot_id"], "metric_key": metric_key,
                    "definition_version": definition["version"]})
            return {**self._result_view(conn, result),
                    "publication": self._publication_view(conn, publication) if publication else None,
                    "definition_requested": definition["version"]}

    def day(self, venue_id: str, day: str, snapshot_id: str | None = None) -> dict:
        """列出当天各指标的有效发布绑定，并显式标记尚未发布的指标。"""
        iso_day(day, "local_day")
        with self.store.transaction(write=False) as conn:
            snapshot = self._head_or_snapshot(conn, venue_id, day, snapshot_id)
            metric_keys = [row["metric_key"] for row in Store.many(
                conn, "SELECT DISTINCT metric_key FROM metric_definitions ORDER BY metric_key")]
            items = []
            for metric_key in metric_keys:
                publication = Store.one(conn, "SELECT * FROM metric_publications WHERE snapshot_id=? "
                                        "AND metric_key=? AND active=1", (snapshot["snapshot_id"], metric_key))
                if publication:
                    result = Store.one(conn, "SELECT * FROM metric_results WHERE result_id=?",
                                       (publication["result_id"],))
                    items.append({"metric_key": metric_key, "published": True,
                                  "result": self._result_view(conn, result),
                                  "publication": self._publication_view(conn, publication)})
                else:
                    latest = self.definitions._latest(conn, metric_key)
                    items.append({"metric_key": metric_key, "published": False, "result": None,
                                  "latest_definition_version": latest["version"],
                                  "latest_definition_digest": latest["digest"]})
            return {"venue_id": venue_id, "local_day": day, "snapshot_id": snapshot["snapshot_id"],
                    "generation": snapshot["generation"], "metrics": items}
