import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from exhibit_lake.api import create_app
from exhibit_lake.errors import Conflict, IntegrityFailure, InvalidInput, NotFound, StateError
from exhibit_lake.service import ExhibitLake


OLD_SPEC = {"numerator": {"kind": "feedback", "measure": "events"},
            "denominator": {"kind": "visit", "measure": "value_sum"}, "round": 6}
NEW_SPEC = {"numerator": {"kind": "feedback", "measure": "unique_visitors"},
            "denominator": {"kind": "visit", "measure": "value_sum"}, "round": 6}


class MetricVersioningTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.lake = ExhibitLake(Path(self.folder.name) / "lake.db")
        self.lake.register_venue("venue", "Venue", "Asia/Shanghai")
        self.lake.register_exhibit("robot", "venue", "Robot", "Maker", "2026-01-01")
        self.lake.register_exhibit("sensor", "venue", "Sensor", "Maker", "2026-01-01")
        for kind, source, fields in [
            ("visit", "counter", ["event_id", "exhibit_id", "event_at", "count"]),
            ("feedback", "kiosk", ["event_id", "exhibit_id", "event_at", "rating", "visitor_id"]),
        ]:
            self.lake.register_source(source, "venue", kind)
            self.lake.register_contract(source + "-v1", source, fields, "2026-01-01T00:00:00+00:00")

    def row(self, event_id, exhibit_id="robot", **fields):
        return {"event_id": event_id, "exhibit_id": exhibit_id,
                "event_at": "2026-03-01T12:00:00+08:00", **fields}

    def add_visits(self, key="v1", count=10, exhibit_id="robot"):
        return self.lake.ingest("counter", "counter-v1", key,
                                [self.row(key, exhibit_id=exhibit_id, count=count)])

    def add_feedback(self, key="f1", visitor="person-1", exhibit_id="robot", rating=5):
        return self.lake.ingest("kiosk", "kiosk-v1", key,
                                [self.row(key, exhibit_id=exhibit_id, rating=rating, visitor_id=visitor)])

    def publish(self, parent=None):
        staged = self.lake.snapshots.stage("venue", "2026-03-01", parent)
        return self.lake.snapshots.publish(staged["snapshot_id"], parent)["snapshot_id"]

    def define(self, spec=OLD_SPEC, name="体验转化率", description=None):
        return self.lake.metrics.define("experience_conversion", name, spec, description)

    def test_definition_versions_are_immutable(self):
        first = self.define()
        second = self.define(NEW_SPEC, description="新口径")
        self.assertEqual((first["version"], second["version"]), (1, 2))
        self.assertTrue(second["created"])
        self.assertNotEqual(first["digest"], second["digest"])
        again = self.define(NEW_SPEC, description="新口径")
        self.assertFalse(again["created"])
        self.assertEqual(again["version"], 2)
        self.assertEqual(self.lake.metrics.definition("experience_conversion", 1)["spec"], first["spec"])
        self.assertEqual(len(self.lake.metrics.definition_versions("experience_conversion")["versions"]), 2)
        self.assertEqual(self.lake.metrics.catalog()["metrics"][0]["latest_version"], 2)

    def test_invalid_definition_is_rejected(self):
        with self.assertRaises(InvalidInput):
            self.define({"numerator": {"kind": "feedback", "measure": "events"}})
        with self.assertRaises(InvalidInput):
            self.define({**OLD_SPEC, "denominator": {"kind": "visit", "measure": "median"}})
        with self.assertRaises(InvalidInput):
            self.define({**OLD_SPEC, "extra": True})
        with self.assertRaises(NotFound):
            self.lake.metrics.definition("experience_conversion", 1)

    def test_recompute_reports_definition_and_input_versions(self):
        self.add_visits()
        self.add_feedback()
        snapshot = self.publish()
        definition = self.define()
        task = self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                           snapshot, "request-1", definition["version"])
        self.assertEqual(task["status"], "succeeded")
        self.assertEqual(task["definition"]["version"], 1)
        self.assertEqual(task["definition"]["digest"], definition["digest"])
        self.assertEqual(task["input"]["snapshot_id"], snapshot)
        self.assertEqual(task["input"]["generation"], 1)
        self.assertEqual(task["input"]["event_count"], 2)
        result = task["result"]
        self.assertEqual(result["value"], 0.1)
        self.assertEqual(result["definition"]["version"], 1)
        self.assertEqual(result["input"]["input_digest"], task["input"]["input_digest"])
        self.assertFalse(result["published"])
        self.assertFalse(result["superseded"])

    def test_new_definition_recomputes_historical_partition_without_overwriting(self):
        self.add_visits()
        self.add_feedback("f1", visitor="person-1")
        self.add_feedback("f2", visitor="person-1")
        snapshot = self.publish()
        old = self.define()
        first = self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                            snapshot, "r1", old["version"])
        publication = self.lake.metrics.publish(first["result"]["result_id"])
        self.assertTrue(publication["active"])
        new = self.define(NEW_SPEC, description="按独立访客计转化")
        second = self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                             snapshot, "r2", new["version"])
        self.assertEqual(second["definition"]["version"], 2)
        self.assertEqual(first["result"]["value"], 0.2)
        self.assertEqual(second["result"]["value"], 0.1)
        with self.assertRaises(Conflict):
            self.lake.metrics.publish(second["result"]["result_id"])
        with self.lake.store.transaction(write=False) as conn:
            still = conn.execute("SELECT result_id FROM metric_results WHERE metric_key=? AND venue_id=? "
                                 "AND local_day=? AND definition_version=1", ("experience_conversion", "venue",
                                                                             "2026-03-01")).fetchone()
        self.assertEqual(still["result_id"], first["result"]["result_id"])
        self.assertEqual(self.lake.metrics.report("venue", "2026-03-01",
                                                  "experience_conversion")["definition"]["version"], 1)
        replaced = self.lake.metrics.publish(second["result"]["result_id"],
                                             expected_publication_id=publication["publication_id"],
                                             reason="切换为独立访客口径")
        self.assertTrue(replaced["active"])
        self.assertEqual(replaced["definition_version"], 2)
        history = self.lake.metrics.publications(snapshot)["publications"]
        self.assertEqual(len(history), 2)
        self.assertEqual([item["active"] for item in history], [False, True])
        self.assertEqual(history[0]["retire_reason"], "切换为独立访客口径")
        self.assertEqual(history[0]["definition_version"], 1)
        current = self.lake.metrics.report("venue", "2026-03-01", "experience_conversion")
        self.assertEqual(current["value"], 0.1)
        self.assertEqual(current["definition"]["version"], 2)
        historical = self.lake.metrics.report("venue", "2026-03-01", "experience_conversion",
                                              snapshot_id=snapshot, definition_version=1)
        self.assertEqual(historical["value"], 0.2)
        self.assertEqual(historical["publication"]["publication_id"], publication["publication_id"])
        self.assertFalse(historical["publication"]["active"])
        self.assertEqual(historical["definition_requested"], 1)
        audit = self.lake.audit.trail(action="metric.publication.retired", limit=10)
        self.assertEqual(len(audit["audits"]), 1)

    def test_missing_observations_are_null_not_zero(self):
        self.add_visits()
        snapshot = self.publish()
        definition = self.define()
        task = self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                           snapshot, "r1", definition["version"])
        result = task["result"]
        self.assertIsNone(result["value"])
        self.assertEqual(result["status"], "no_data")
        self.assertEqual(result["reason"], "no_numerator_observations")
        self.assertEqual(result["numerator"]["observations"], 0)
        self.assertEqual(result["unobserved_exhibits"], ["sensor"])
        self.assertEqual([item["exhibit_id"] for item in result["exhibits"]], ["robot"])
        self.assertEqual(result["exhibits"][0]["value"], None)

    def test_zero_denominator_is_not_a_ratio(self):
        self.add_feedback()
        snapshot = self.publish()
        definition = self.define()
        result = self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                             snapshot, "r1", definition["version"])["result"]
        self.assertIsNone(result["value"])
        self.assertEqual(result["status"], "no_data")
        self.assertEqual(result["reason"], "no_denominator_observations")

    def test_recompute_requires_published_snapshot(self):
        self.add_visits()
        staged = self.lake.snapshots.stage("venue", "2026-03-01")
        definition = self.define()
        with self.assertRaises(StateError):
            self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                        staged["snapshot_id"], "r1", definition["version"])

    def test_recompute_rejects_cross_partition_snapshot(self):
        self.add_visits()
        snapshot = self.publish()
        definition = self.define()
        with self.assertRaises(NotFound):
            self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-02",
                                        snapshot, "r1", definition["version"])

    def test_request_key_pins_definition_and_input(self):
        self.add_visits()
        snapshot = self.publish()
        definition = self.define()
        first = self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                            snapshot, "same-key", definition["version"])
        replay = self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                             snapshot, "same-key", definition["version"])
        self.assertTrue(replay["replay"])
        self.assertEqual(first["task_id"], replay["task_id"])
        newer = self.define(NEW_SPEC)
        with self.assertRaises(Conflict) as caught:
            self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                        snapshot, "same-key", newer["version"])
        self.assertEqual(caught.exception.details["mismatches"]["definition_version"],
                         {"pinned": 1, "requested": 2})

    def test_retry_reuses_pinned_definition_and_reports_attempts(self):
        self.add_visits()
        self.add_feedback()
        snapshot = self.publish()
        definition = self.define()
        deferred = self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                               snapshot, "queued", definition["version"], defer=True)
        self.assertEqual(deferred["status"], "pending")
        self.define(NEW_SPEC)
        # 页面口径已是 v2，但任务仍按创建时钉住的 v1 执行。
        retried = self.lake.metrics.retry(deferred["task_id"])
        self.assertEqual(retried["status"], "succeeded")
        self.assertEqual(retried["attempts"], 1)
        self.assertEqual(retried["definition"]["version"], 1)
        self.assertEqual(retried["definition"]["latest_version"], 2)
        self.assertTrue(retried["definition"]["superseded"])
        self.assertEqual(retried["result"]["value"], 0.1)
        self.assertEqual(retried["result"]["definition"]["version"], 1)
        tasks = self.lake.metrics.tasks(venue_id="venue", metric_key="experience_conversion")
        self.assertEqual(tasks["total"], 1)

    def test_retry_failure_keeps_evidence_and_can_succeed_after_repair(self):
        self.add_visits()
        self.add_feedback()
        snapshot = self.publish()
        definition = self.define()
        deferred = self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                               snapshot, "queued", definition["version"], defer=True)
        with self.lake.store.transaction() as conn:
            conn.execute("UPDATE accepted_events SET value_num=2 WHERE kind='visit'")
        with self.assertRaises(IntegrityFailure):
            self.lake.metrics.retry(deferred["task_id"])
        failed = self.lake.metrics.task(deferred["task_id"])
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(failed["attempts"], 1)
        self.assertEqual(failed["error"]["code"], "integrity_failure")
        self.assertIn("snapshot statistics differ from members", failed["error"]["message"])
        self.assertEqual(failed["definition"]["version"], 1)
        with self.lake.store.transaction() as conn:
            conn.execute("UPDATE accepted_events SET value_num=10 WHERE kind='visit'")
        retried = self.lake.metrics.retry(deferred["task_id"])
        self.assertEqual(retried["status"], "succeeded")
        self.assertEqual(retried["attempts"], 2)
        self.assertEqual(retried["result"]["value"], 0.1)

    def test_identical_inputs_deduplicate_result(self):
        self.add_visits()
        self.add_feedback()
        snapshot = self.publish()
        definition = self.define()
        first = self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                            snapshot, "key-a", definition["version"])
        second = self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                             snapshot, "key-b", definition["version"])
        self.assertEqual(first["result"]["result_id"], second["result"]["result_id"])
        self.assertEqual(len(self.lake.metrics.results(
            "experience_conversion", "venue", "2026-03-01")["results"]), 1)

    def test_scoped_recompute_cannot_replace_partition_report(self):
        self.add_visits()
        self.add_feedback()
        self.add_visits("v2", 5, exhibit_id="sensor")
        snapshot = self.publish()
        definition = self.define()
        scoped = self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                             snapshot, "partial", definition["version"],
                                             exhibit_ids=["robot"])
        self.assertEqual(scoped["result"]["scope"], ["robot"])
        self.assertEqual(scoped["result"]["value"], 0.1)
        self.assertEqual(scoped["input"]["event_count"], 3)
        self.assertEqual([item["exhibit_id"] for item in scoped["result"]["exhibits"]], ["robot"])
        with self.assertRaises(StateError):
            self.lake.metrics.publish(scoped["result"]["result_id"])
        with self.assertRaises(NotFound):
            self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                        snapshot, "bad-scope", definition["version"],
                                        exhibit_ids=["missing"])

    def test_backfill_requires_new_snapshot_for_new_input_version(self):
        self.add_visits()
        self.add_feedback()
        first_snapshot = self.publish()
        definition = self.define()
        first = self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                            first_snapshot, "r1", definition["version"])
        self.assertEqual(first["input"]["event_count"], 2)
        self.add_visits("v2", 10)
        # 已发布快照不可变：补传数据不会改变历史结果的输入版本。
        unchanged = self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                                first_snapshot, "r2", definition["version"])
        self.assertEqual(unchanged["result"]["input"]["event_count"], 2)
        self.assertEqual(unchanged["result"]["value"], 0.1)
        second_snapshot = self.publish(first_snapshot)
        backfilled = self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                                 second_snapshot, "r3", definition["version"])
        self.assertEqual(backfilled["input"]["generation"], 2)
        self.assertEqual(backfilled["input"]["event_count"], 3)
        self.assertEqual(backfilled["result"]["value"], 0.05)
        old_publication = self.lake.metrics.publish(first["result"]["result_id"])
        # 新快照上的绑定独立于旧快照：同一快照内替换才需要声明预期绑定。
        replaced = self.lake.metrics.publish(backfilled["result"]["result_id"])
        self.assertTrue(replaced["active"])
        # 新快照上的绑定独立于旧快照，旧报表仍按旧快照与旧定义解释。
        historical = self.lake.metrics.report("venue", "2026-03-01", "experience_conversion",
                                              snapshot_id=first_snapshot)
        self.assertEqual(historical["value"], 0.1)
        self.assertTrue(historical["superseded"])
        self.assertEqual(historical["publication"]["publication_id"], old_publication["publication_id"])
        self.assertEqual(historical["current_head_id"], second_snapshot)

    def test_definition_change_backfill_and_retry_are_all_attributed(self):
        self.add_visits("v1", 10)
        self.add_feedback("f1", visitor="person-1")
        first_snapshot = self.publish()
        old = self.define()
        queued = self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                             first_snapshot, "queued", old["version"], defer=True)
        # 三件事同时发生：补传新数据、登记新定义、随后重试旧任务。
        self.add_visits("v2", 30)
        self.add_feedback("f2", visitor="person-2")
        new = self.define(NEW_SPEC, description="按独立访客计转化")
        retried = self.lake.metrics.retry(queued["task_id"])
        self.assertEqual(retried["status"], "succeeded")
        self.assertEqual(retried["definition"]["version"], 1)
        self.assertEqual(retried["definition"]["latest_version"], 2)
        self.assertTrue(retried["definition"]["superseded"])
        self.assertEqual(retried["input"]["snapshot_id"], first_snapshot)
        self.assertEqual(retried["input"]["generation"], 1)
        self.assertEqual(retried["input"]["event_count"], 2)
        self.assertEqual(retried["result"]["value"], 0.1)
        # 补传数据进入下一代快照后，用新定义重算，得到独立的结果版本。
        second_snapshot = self.publish(first_snapshot)
        fresh = self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                            second_snapshot, "fresh", new["version"])
        self.assertEqual(fresh["input"]["generation"], 2)
        self.assertEqual(fresh["input"]["event_count"], 4)
        self.assertEqual(fresh["result"]["value"], round(2 / 40, 6))
        old_bound = self.lake.metrics.publish(retried["result"]["result_id"])
        self.assertEqual(old_bound["snapshot_id"], first_snapshot)
        new_bound = self.lake.metrics.publish(fresh["result"]["result_id"])
        self.assertEqual(new_bound["snapshot_id"], second_snapshot)
        # 两个快照各自的绑定互不影响，历史报表仍可解释旧口径。
        self.assertEqual(self.lake.metrics.report(
            "venue", "2026-03-01", "experience_conversion",
            snapshot_id=first_snapshot)["definition"]["version"], 1)
        self.assertEqual(self.lake.metrics.report(
            "venue", "2026-03-01", "experience_conversion")["definition"]["version"], 2)
        self.assertEqual(self.lake.metrics.report(
            "venue", "2026-03-01", "experience_conversion")["input"]["event_count"], 4)

    def test_inline_definition_registers_version_for_recompute(self):
        self.add_visits()
        self.add_feedback()
        snapshot = self.publish()
        task = self.lake.metrics.recompute(
            "experience_conversion", "venue", "2026-03-01", snapshot, "inline",
            definition={"name": "体验转化率", "spec": NEW_SPEC, "description": "新算法"})
        self.assertEqual(task["definition"]["version"], 1)
        self.assertEqual(task["result"]["definition"]["spec"]["numerator"]["measure"], "unique_visitors")
        self.assertEqual(task["result"]["value"], 0.1)
        with self.assertRaises(InvalidInput):
            self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01", snapshot,
                                        "inline-bad", definition_version=1,
                                        definition={"name": "x", "spec": NEW_SPEC})
        replay = self.lake.metrics.recompute(
            "experience_conversion", "venue", "2026-03-01", snapshot, "inline",
            definition={"name": "体验转化率", "spec": NEW_SPEC, "description": "新算法"})
        self.assertTrue(replay["replay"])
        self.assertEqual(replay["task_id"], task["task_id"])
        self.assertEqual(len(self.lake.metrics.definition_versions(
            "experience_conversion")["versions"]), 1)
        with self.assertRaises(Conflict):
            self.lake.metrics.recompute(
                "experience_conversion", "venue", "2026-03-01", snapshot, "inline",
                definition={"name": "体验转化率", "spec": OLD_SPEC, "description": "旧算法"})
        self.assertEqual(len(self.lake.metrics.definition_versions(
            "experience_conversion")["versions"]), 1)

    def test_trace_links_definition_task_snapshot_and_batches(self):
        batch = self.add_visits()
        self.add_feedback()
        snapshot = self.publish()
        definition = self.define()
        task = self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                           snapshot, "r1", definition["version"])
        trace = self.lake.metrics.trace(task["result"]["result_id"])
        self.assertEqual(trace["definition"]["version"], 1)
        self.assertEqual(trace["task"]["task_id"], task["task_id"])
        self.assertEqual(trace["snapshot"]["snapshot_id"], snapshot)
        self.assertEqual(trace["snapshot"]["members"], 2)
        self.assertEqual(trace["snapshot"]["batches"][0]["batch_id"], batch["batch_id"])
        self.assertEqual(trace["event_count"], 2)
        self.assertEqual(trace["result"]["input"]["snapshot_digest"],
                         self.lake.snapshots.describe(snapshot)["digest"])

    def test_day_report_marks_unpublished_metric(self):
        self.add_visits()
        snapshot = self.publish()
        definition = self.define()
        listing = self.lake.metrics.day("venue", "2026-03-01")
        self.assertFalse(listing["metrics"][0]["published"])
        self.assertEqual(listing["metrics"][0]["latest_definition_version"], 1)
        with self.assertRaises(NotFound):
            self.lake.metrics.report("venue", "2026-03-01", "experience_conversion")
        task = self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                           snapshot, "r1", definition["version"])
        self.lake.metrics.publish(task["result"]["result_id"])
        listing = self.lake.metrics.day("venue", "2026-03-01")
        self.assertTrue(listing["metrics"][0]["published"])
        self.assertEqual(listing["metrics"][0]["result"]["definition"]["version"], 1)

    def test_empty_snapshot_reports_no_data(self):
        snapshot = self.publish()
        definition = self.define()
        task = self.lake.metrics.recompute("experience_conversion", "venue", "2026-03-01",
                                           snapshot, "r1", definition["version"])
        self.assertEqual(task["result"]["status"], "no_data")
        self.assertIsNone(task["result"]["value"])
        self.assertEqual(task["result"]["numerator"]["observations"], 0)


class MetricApiTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.client = TestClient(create_app(str(Path(self.folder.name) / "api.db")))
        self.assertEqual(self.client.post("/venues", json={"venue_id": "venue", "name": "Venue",
                                                    "timezone": "Asia/Shanghai"}).status_code, 201)
        self.assertEqual(self.client.post("/exhibits", json={"exhibit_id": "robot", "venue_id": "venue",
                                                      "name": "Robot", "maker": "Maker",
                                                      "launched_on": "2026-01-01"}).status_code, 201)
        for source, kind, fields in [
            ("counter", "visit", ["event_id", "exhibit_id", "event_at", "count"]),
            ("kiosk", "feedback", ["event_id", "exhibit_id", "event_at", "rating", "visitor_id"]),
        ]:
            self.client.post("/sources", json={"source_id": source, "venue_id": "venue", "kind": kind})
            self.client.post("/contracts", json={"contract_id": source + "-v1", "source_id": source,
                                                 "fields": fields,
                                                 "effective_from": "2026-01-01T00:00:00+00:00"})
        self.client.post("/batches", json={"source_id": "counter", "contract_id": "counter-v1",
                                           "request_key": "v",
                                           "events": [{"event_id": "v1", "exhibit_id": "robot",
                                                       "event_at": "2026-03-01T10:00:00+08:00", "count": 10}]})
        staged = self.client.post("/snapshots/stage", json={"venue_id": "venue",
                                                            "local_day": "2026-03-01"}).json()
        self.snapshot = staged["snapshot_id"]
        self.assertEqual(self.client.post(f"/snapshots/{self.snapshot}/publish",
                                          json={"expected_head": None}).status_code, 200)

    def define(self, spec=OLD_SPEC):
        return self.client.post("/metrics", json={"metric_key": "experience_conversion",
                                                  "name": "体验转化率",
                                                  "description": "历史口径", "spec": spec})

    def test_metric_flow_over_http(self):
        created = self.define()
        self.assertEqual(created.status_code, 201)
        self.assertEqual(created.json()["version"], 1)
        task = self.client.post("/metrics/recomputes", json={
            "metric_key": "experience_conversion", "venue_id": "venue",
            "local_day": "2026-03-01", "snapshot_id": self.snapshot,
            "request_key": "r1", "definition_version": 1})
        self.assertEqual(task.status_code, 201, task.text)
        payload = task.json()
        self.assertEqual(payload["status"], "succeeded")
        self.assertEqual(payload["result"]["value"], None)
        result_id = payload["result"]["result_id"]
        published = self.client.post(f"/metrics/results/{result_id}/publish", json={})
        self.assertEqual(published.status_code, 201, published.text)
        report = self.client.get("/venues/venue/days/2026-03-01/metrics/experience_conversion")
        self.assertEqual(report.status_code, 200, report.text)
        self.assertEqual(report.json()["definition"]["version"], 1)
        self.assertEqual(report.json()["status"], "no_data")
        trace = self.client.get(f"/metrics/results/{result_id}/trace")
        self.assertEqual(trace.status_code, 200)
        self.assertEqual(trace.json()["snapshot"]["snapshot_id"], self.snapshot)
        listed = self.client.get("/metrics/recomputes", params={"venue_id": "venue"})
        self.assertEqual(listed.json()["total"], 1)
        self.assertEqual(self.client.get("/metrics").json()["metrics"][0]["latest_version"], 1)

    def test_second_definition_requires_explicit_replacement(self):
        self.define()
        first = self.client.post("/metrics/recomputes", json={
            "metric_key": "experience_conversion", "venue_id": "venue",
            "local_day": "2026-03-01", "snapshot_id": self.snapshot,
            "request_key": "r1", "definition_version": 1}).json()
        published = self.client.post(f"/metrics/results/{first['result']['result_id']}/publish",
                                     json={}).json()
        second = self.define(NEW_SPEC).json()
        self.assertEqual(second["version"], 2)
        newer = self.client.post("/metrics/recomputes", json={
            "metric_key": "experience_conversion", "venue_id": "venue",
            "local_day": "2026-03-01", "snapshot_id": self.snapshot,
            "request_key": "r2", "definition_version": 2}).json()
        conflict = self.client.post(f"/metrics/results/{newer['result']['result_id']}/publish", json={})
        self.assertEqual(conflict.status_code, 409)
        self.assertEqual(conflict.json()["error"]["details"]["active_definition_version"], 1)
        replaced = self.client.post(f"/metrics/results/{newer['result']['result_id']}/publish",
                                    json={"expected_publication_id": published["publication_id"],
                                          "reason": "口径切换"})
        self.assertEqual(replaced.status_code, 201, replaced.text)
        old = self.client.get("/venues/venue/days/2026-03-01/metrics/experience_conversion",
                              params={"definition_version": 1})
        self.assertEqual(old.status_code, 200, old.text)
        self.assertEqual(old.json()["definition"]["version"], 1)

    def test_invalid_metric_request_has_error_envelope(self):
        result = self.client.post("/metrics", json={"metric_key": "experience_conversion",
                                                    "name": "体验转化率", "spec": {"numerator": {}}})
        self.assertEqual(result.status_code, 400)
        self.assertEqual(result.json()["error"]["code"], "invalid_input")
        missing = self.client.get("/venues/venue/days/2026-03-01/metrics/experience_conversion")
        self.assertEqual(missing.status_code, 404)
        self.assertEqual(missing.json()["error"]["code"], "not_found")


if __name__ == "__main__":
    unittest.main()
