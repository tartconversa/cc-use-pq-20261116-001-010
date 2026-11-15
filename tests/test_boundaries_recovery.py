import tempfile
import unittest
from pathlib import Path

from exhibit_lake.errors import Conflict, InvalidInput, NotFound, StateError
from exhibit_lake.service import ExhibitLake


class BoundaryRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.lake = ExhibitLake(Path(self.folder.name) / "lake.db")
        self.lake.register_venue("venue", "Venue", "Asia/Shanghai")
        self.lake.register_exhibit("robot", "venue", "Robot", "Maker", "2026-01-01")
        self.lake.register_source("counter", "venue", "visit")
        self.lake.register_contract("v1", "counter", ["event_id", "exhibit_id", "event_at", "count"],
                                    "2026-01-01T00:00:00+00:00")

    def event(self, identity="v1", at="2026-03-01T00:30:00+08:00", count=1):
        return {"event_id": identity, "exhibit_id": "robot", "event_at": at, "count": count}

    def test_local_day_uses_venue_timezone(self):
        result = self.lake.ingest("counter", "v1", "b1", [self.event()])
        self.assertEqual(result["valid_rows"], 1)
        self.assertEqual(self.lake.snapshots.inventory("venue", "2026-03-01")["sources"][0]["events"], 1)
        self.assertEqual(self.lake.snapshots.inventory("venue", "2026-02-28")["sources"], [])

    def test_source_contract_is_event_time_bound(self):
        with self.lake.store.transaction() as conn:
            conn.execute("UPDATE contracts SET effective_to=? WHERE contract_id='v1'", ("2026-02-01T00:00:00+00:00",))
        result = self.lake.ingest("counter", "v1", "outside", [self.event()])
        self.assertEqual(result["rejections"][0]["code"], "outside_contract")

    def test_missing_field_is_rejected_as_row(self):
        row = self.event()
        row.pop("count")
        result = self.lake.ingest("counter", "v1", "missing", [row])
        self.assertEqual(result["rejections"][0]["code"], "field_mismatch")

    def test_extra_field_is_rejected_as_row(self):
        row = self.event()
        row["unexpected"] = 2
        result = self.lake.ingest("counter", "v1", "extra", [row])
        self.assertEqual(result["rejections"][0]["code"], "field_mismatch")

    def test_invalid_batch_size(self):
        with self.assertRaises(InvalidInput):
            self.lake.ingest("counter", "v1", "empty", [])

    def test_non_json_value_rejected_atomically(self):
        with self.assertRaises(InvalidInput):
            self.lake.ingest("counter", "v1", "nan", [self.event(count=float("nan"))])
        with self.lake.store.transaction(write=False) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0], 0)

    def test_raw_page_boundary(self):
        result = self.lake.ingest("counter", "v1", "b1", [self.event("a"), self.event("b")])
        page = self.lake.batches.raw(result["batch_id"], offset=1, limit=1)
        self.assertEqual(page["rows"][0]["payload"]["event_id"], "b")

    def test_invalid_raw_page(self):
        result = self.lake.ingest("counter", "v1", "b1", [self.event()])
        with self.assertRaises(InvalidInput):
            self.lake.batches.raw(result["batch_id"], offset=-1)

    def test_unknown_batch_report(self):
        with self.assertRaises(NotFound):
            self.lake.batches.report("missing")

    def test_candidate_sees_only_events_at_stage_time(self):
        self.lake.ingest("counter", "v1", "b1", [self.event("a")])
        candidate = self.lake.snapshots.stage("venue", "2026-03-01")
        self.lake.ingest("counter", "v1", "b2", [self.event("b")])
        published = self.lake.snapshots.publish(candidate["snapshot_id"], None)
        self.assertEqual(published["manifest"]["statistics"]["events"], 1)
        self.assertEqual(self.lake.snapshots.inventory("venue", "2026-03-01")["unpublished_events"], 1)

    def test_restage_after_abandon_picks_up_new_rows(self):
        self.lake.ingest("counter", "v1", "b1", [self.event("a")])
        old = self.lake.snapshots.stage("venue", "2026-03-01")
        self.lake.ingest("counter", "v1", "b2", [self.event("b")])
        self.lake.snapshots.abandon(old["snapshot_id"])
        new = self.lake.snapshots.stage("venue", "2026-03-01")
        self.assertEqual(new["manifest"]["statistics"]["events"], 2)

    def test_published_old_generation_is_stable(self):
        self.lake.ingest("counter", "v1", "b1", [self.event("a")])
        old = self.lake.publish_day("venue", "2026-03-01")
        self.lake.ingest("counter", "v1", "b2", [self.event("b")])
        new = self.lake.publish_day("venue", "2026-03-01", old["snapshot_id"])
        self.assertEqual(self.lake.analytics.day("venue", "2026-03-01", old["snapshot_id"])["event_count"], 1)
        self.assertEqual(self.lake.analytics.day("venue", "2026-03-01", new["snapshot_id"])["event_count"], 2)

    def test_coverage_range_reports_pending(self):
        self.lake.ingest("counter", "v1", "b1", [self.event()])
        result = self.lake.coverage.range("venue", "2026-03-01", "2026-03-02")
        self.assertEqual(result["unpublished_days"], ["2026-03-01"])

    def test_coverage_range_limit(self):
        with self.assertRaises(InvalidInput):
            self.lake.coverage.range("venue", "2026-01-01", "2026-03-01")

    def test_no_published_partition_is_not_a_report(self):
        with self.assertRaises(NotFound):
            self.lake.analytics.day("venue", "2026-03-01")

    def test_wrong_partition_snapshot_rejected(self):
        self.lake.ingest("counter", "v1", "b1", [self.event()])
        snapshot = self.lake.publish_day("venue", "2026-03-01")
        with self.assertRaises(NotFound):
            self.lake.analytics.day("venue", "2026-03-02", snapshot["snapshot_id"])

    def test_compare_exhibits_rejects_duplicates(self):
        self.lake.publish_day("venue", "2026-03-01")
        with self.assertRaises(InvalidInput):
            self.lake.experience.compare_exhibits("venue", "2026-03-01", ["robot", "robot"])

    def test_audit_trail_has_registration_and_publication(self):
        self.lake.publish_day("venue", "2026-03-01")
        result = self.lake.audit.trail(limit=100)
        self.assertIn("venue.created", [x["action"] for x in result["audits"]])
        self.assertIn("snapshot.published", [x["action"] for x in result["audits"]])

    def test_no_spurious_recovery_actions(self):
        self.lake.publish_day("venue", "2026-03-01")
        result = self.lake.recovery.abandon_stale("venue", "2026-03-01")
        self.assertEqual(result["abandoned_ids"], [])

    def test_snapshot_list_default_is_published_only(self):
        self.lake.ingest("counter", "v1", "b1", [self.event()])
        candidate = self.lake.snapshots.stage("venue", "2026-03-01")
        self.assertEqual(self.lake.snapshots.list("venue", "2026-03-01"), [])
        self.assertEqual(self.lake.snapshots.list("venue", "2026-03-01", include_staged=True)[0]["snapshot_id"], candidate["snapshot_id"])
