import tempfile
import unittest
from pathlib import Path

from exhibit_lake.errors import Conflict, IntegrityFailure, NotFound, StateError
from exhibit_lake.service import ExhibitLake


class SnapshotAnalyticsTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.lake = ExhibitLake(Path(self.folder.name) / "lake.db")
        self.lake.register_venue("venue", "Test Venue", "Asia/Shanghai")
        self.lake.register_exhibit("robot", "venue", "Robot", "Maker", "2026-01-01")
        self.lake.register_exhibit("sensor", "venue", "Sensor", "Maker", "2026-01-01")
        for kind, source, fields in [
            ("visit", "counter", ["event_id", "exhibit_id", "event_at", "count"]),
            ("feedback", "kiosk", ["event_id", "exhibit_id", "event_at", "rating", "visitor_id"]),
            ("demo", "stage", ["event_id", "exhibit_id", "event_at", "duration_seconds"]),
        ]:
            self.lake.register_source(source, "venue", kind)
            self.lake.register_contract(source + "-v1", source, fields, "2026-01-01T00:00:00+00:00")

    def row(self, event_id, exhibit_id="robot", **fields):
        return {"event_id": event_id, "exhibit_id": exhibit_id,
                "event_at": "2026-03-01T12:00:00+08:00", **fields}

    def add_visits(self, key="v", number=5):
        return self.lake.ingest("counter", "counter-v1", key, [self.row(key, count=number)])

    def add_feedback(self, key="f", rating=5, visitor="person"):
        return self.lake.ingest("kiosk", "kiosk-v1", key,
                                [self.row(key, rating=rating, visitor_id=visitor)])

    def add_demo(self, key="d", seconds=35):
        return self.lake.ingest("stage", "stage-v1", key,
                                [self.row(key, duration_seconds=seconds)])

    def publish(self, parent=None):
        candidate = self.lake.snapshots.stage("venue", "2026-03-01", parent)
        published = self.lake.snapshots.publish(candidate["snapshot_id"], parent)
        return published["snapshot_id"]

    def test_empty_snapshot_is_valid(self):
        snapshot = self.publish()
        self.assertEqual(self.lake.snapshots.verify(snapshot)["events"], 0)
        self.assertEqual(self.lake.analytics.day("venue", "2026-03-01")["event_count"], 0)

    def test_publish_full_report(self):
        self.add_visits()
        self.add_feedback()
        self.add_demo()
        snapshot = self.publish()
        report = self.lake.analytics.day("venue", "2026-03-01")
        self.assertEqual(report["snapshot_id"], snapshot)
        self.assertEqual(report["total_visitors"], 5)
        self.assertEqual(report["total_feedback"], 1)
        self.assertEqual(report["total_demos"], 1)
        self.assertEqual(report["exhibits"][0]["rating_average"], 5)
        self.assertEqual(report["exhibits"][0]["demo_seconds"], 35)

    def test_publish_marks_ingested_events(self):
        self.add_visits()
        self.assertEqual(self.lake.quality.partition("venue", "2026-03-01")["pending_events"], 1)
        self.publish()
        self.assertEqual(self.lake.quality.partition("venue", "2026-03-01")["pending_events"], 0)

    def test_staged_candidate_not_visible_to_report(self):
        self.add_visits()
        candidate = self.lake.snapshots.stage("venue", "2026-03-01")
        with self.assertRaises(NotFound):
            self.lake.analytics.day("venue", "2026-03-01")
        with self.assertRaises(StateError):
            self.lake.analytics.day("venue", "2026-03-01", candidate["snapshot_id"])

    def test_staging_requires_expected_head(self):
        self.add_visits()
        parent = self.publish()
        with self.assertRaises(Conflict):
            self.lake.snapshots.stage("venue", "2026-03-01")
        self.assertEqual(self.lake.snapshots.head("venue", "2026-03-01")["head_id"], parent)

    def test_duplicate_candidate_conflicts(self):
        self.add_visits()
        self.lake.snapshots.stage("venue", "2026-03-01")
        with self.assertRaises(Conflict):
            self.lake.snapshots.stage("venue", "2026-03-01")

    def test_abandon_candidate_allows_restage(self):
        self.add_visits()
        first = self.lake.snapshots.stage("venue", "2026-03-01")
        self.lake.snapshots.abandon(first["snapshot_id"])
        second = self.lake.snapshots.stage("venue", "2026-03-01")
        self.assertNotEqual(first["snapshot_id"], second["snapshot_id"])
        with self.assertRaises(StateError):
            self.lake.snapshots.publish(first["snapshot_id"], None)

    def test_publish_is_idempotent_for_current_head(self):
        self.add_visits()
        snapshot = self.publish()
        again = self.lake.snapshots.publish(snapshot, None)
        self.assertEqual(again["snapshot_id"], snapshot)

    def test_new_batch_requires_new_generation(self):
        self.add_visits()
        first = self.publish()
        self.add_visits("v2", 7)
        second = self.publish(first)
        self.assertEqual(self.lake.analytics.day("venue", "2026-03-01", first)["total_visitors"], 5)
        self.assertEqual(self.lake.analytics.day("venue", "2026-03-01", second)["total_visitors"], 12)
        self.assertEqual(self.lake.snapshots.head("venue", "2026-03-01")["generation"], 2)

    def test_compare_snapshots(self):
        self.add_visits()
        first = self.publish()
        self.add_feedback()
        second = self.publish(first)
        comparison = self.lake.analytics.compare("venue", "2026-03-01", first, second)
        self.assertEqual(comparison["added"], [{"source_id": "kiosk", "event_id": "f"}])
        self.assertEqual(comparison["unchanged"], 1)

    def test_snapshot_manifest_matches_membership(self):
        self.add_visits()
        snapshot = self.publish()
        self.assertTrue(self.lake.snapshots.verify(snapshot)["valid"])
        with self.lake.store.transaction() as conn:
            conn.execute("UPDATE snapshot_members SET payload_hash='bad' WHERE snapshot_id=?", (snapshot,))
        with self.assertRaises(IntegrityFailure):
            self.lake.snapshots.verify(snapshot)

    def test_recovery_inspects_candidate(self):
        self.add_visits()
        candidate = self.lake.snapshots.stage("venue", "2026-03-01")
        diagnostics = self.lake.recovery.candidate_diagnostics(candidate["snapshot_id"])
        self.assertTrue(diagnostics["publishable"])
        self.assertEqual(self.lake.recovery.inspect("venue", "2026-03-01")["candidates"][0]["snapshot_id"], candidate["snapshot_id"])

    def test_recovery_does_not_abandon_valid_candidate(self):
        self.add_visits()
        candidate = self.lake.snapshots.stage("venue", "2026-03-01")
        result = self.lake.recovery.abandon_stale("venue", "2026-03-01")
        self.assertEqual(result["abandoned_ids"], [])
        self.assertEqual(self.lake.snapshots.describe(candidate["snapshot_id"])["status"], "staged")

    def test_events_are_paginated(self):
        self.lake.ingest("counter", "counter-v1", "many", [self.row("a", count=2), self.row("b", count=3)])
        self.publish()
        page = self.lake.analytics.events("venue", "2026-03-01", limit=1)
        self.assertEqual(page["total"], 2)
        self.assertEqual(len(page["events"]), 1)
        self.assertEqual(self.lake.analytics.events("venue", "2026-03-01", offset=1, limit=1)["events"][0]["event_id"], "b")

    def test_trend_tracks_published_head(self):
        self.add_visits()
        self.publish()
        trend = self.lake.analytics.trend("venue", "2026-03-01", "2026-03-02")
        self.assertEqual(trend["days"][0]["visitors"], 5)
        self.assertEqual(len(trend["days"]), 1)

    def test_experience_histogram_and_denominator(self):
        self.add_visits()
        self.add_feedback()
        self.publish()
        result = self.lake.experience.exhibit("venue", "2026-03-01", "robot")
        self.assertEqual(result["rating_histogram"]["5"], 1)
        self.assertEqual(result["feedback_per_visitor"], 0.2)

    def test_experience_missing_signal_is_null(self):
        self.add_visits()
        self.publish()
        result = self.lake.experience.exhibit("venue", "2026-03-01", "robot")
        self.assertIsNone(result["rating_average"])
        self.assertIsNone(result["rating_median"])

    def test_experience_compare_order(self):
        self.add_visits()
        self.publish()
        result = self.lake.experience.compare_exhibits("venue", "2026-03-01", ["sensor", "robot"])
        self.assertEqual(result["traffic_order"], ["robot", "sensor"])

    def test_export_csv_includes_snapshot(self):
        self.add_visits()
        snapshot = self.publish()
        csv_text = self.lake.exports.daily_csv("venue", "2026-03-01")
        self.assertIn(snapshot, csv_text)
        self.assertIn("robot", csv_text)

    def test_event_csv_uses_published_rows(self):
        self.add_visits()
        self.publish()
        csv_text = self.lake.exports.events_csv("venue", "2026-03-01")
        self.assertIn("counter", csv_text)
        self.assertIn("v,", csv_text)

    def test_lineage_traces_batch_contract(self):
        batch = self.add_visits()
        snapshot = self.publish()
        lineage = self.lake.analytics.lineage(snapshot)
        self.assertEqual(lineage["batches"][0]["batch_id"], batch["batch_id"])
        self.assertEqual(lineage["contracts"][0]["contract_id"], "counter-v1")

    def test_coverage_reports_no_pending_after_publish(self):
        self.add_visits()
        self.publish()
        coverage = self.lake.coverage.day("venue", "2026-03-01")
        self.assertEqual(coverage["pending_events"], 0)

    def test_venue_integrity_after_publication(self):
        self.add_visits()
        self.publish()
        self.assertTrue(self.lake.audit.venue_integrity("venue")["valid"])
