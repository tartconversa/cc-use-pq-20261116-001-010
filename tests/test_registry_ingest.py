import tempfile
import unittest
from pathlib import Path

from exhibit_lake.errors import Conflict, InvalidInput, NotFound
from exhibit_lake.service import ExhibitLake


STAMP = "2026-03-01T10:00:00+08:00"
START = "2026-01-01T00:00:00+00:00"


class RegistryIngestTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.path = Path(self.folder.name) / "lake.db"
        self.lake = ExhibitLake(self.path)
        self.lake.register_venue("venue", "Test Venue", "Asia/Shanghai")
        self.lake.register_exhibit("robot", "venue", "Robot", "Maker", "2026-01-01")
        self.lake.register_source("visits", "venue", "visit")
        self.lake.register_contract("visit-v1", "visits", ["event_id", "exhibit_id", "event_at", "count"], START)

    def visit(self, event_id="e1", count=3, stamp=STAMP, exhibit_id="robot"):
        return {"event_id": event_id, "exhibit_id": exhibit_id, "event_at": stamp, "count": count}

    def test_registration_description(self):
        result = self.lake.registry.describe("venue")
        self.assertEqual(result["venue"]["timezone"], "Asia/Shanghai")
        self.assertEqual(result["exhibits"][0]["name"], "Robot")
        self.assertEqual(result["sources"][0]["kind"], "visit")

    def test_duplicate_venue_conflicts(self):
        with self.assertRaises(Conflict):
            self.lake.register_venue("venue", "Again", "UTC")
        self.assertEqual(self.lake.registry.describe("venue")["venue"]["name"], "Test Venue")

    def test_invalid_timezone_rejected(self):
        with self.assertRaises(InvalidInput):
            self.lake.register_venue("bad", "Bad", "Nowhere/Nope")

    def test_invalid_identifier_rejected(self):
        with self.assertRaises(InvalidInput):
            self.lake.register_venue("bad id", "Bad", "UTC")

    def test_exhibit_requires_venue(self):
        with self.assertRaises(NotFound):
            self.lake.register_exhibit("other", "missing", "Other", "Maker", "2026-01-01")

    def test_exhibit_cannot_withdraw_before_launch(self):
        with self.assertRaises(InvalidInput):
            self.lake.registry.withdraw("robot", "2025-01-01")

    def test_withdrawal_changes_acceptance(self):
        self.lake.registry.withdraw("robot", "2026-02-28")
        result = self.lake.ingest("visits", "visit-v1", "withdrawn", [self.visit()])
        self.assertEqual(result["valid_rows"], 0)
        self.assertEqual(result["rejections"][0]["code"], "inactive_exhibit")

    def test_source_kind_rejected(self):
        with self.assertRaises(InvalidInput):
            self.lake.register_source("x", "venue", "unknown")

    def test_source_deactivation_blocks_ingest(self):
        self.lake.registry.deactivate_source("visits")
        with self.assertRaises(Conflict):
            self.lake.ingest("visits", "visit-v1", "blocked", [self.visit()])

    def test_contract_fields_required(self):
        with self.assertRaises(InvalidInput):
            self.lake.register_contract("bad", "visits", ["event_id"], START)

    def test_contract_unknown_fields_rejected(self):
        with self.assertRaises(InvalidInput):
            self.lake.register_contract("bad", "visits", ["event_id", "exhibit_id", "event_at", "count", "secret"], START)

    def test_contract_overlap_rejected(self):
        with self.assertRaises(Conflict):
            self.lake.register_contract("v2", "visits", ["event_id", "exhibit_id", "event_at", "count"], "2026-02-01T00:00:00+00:00")

    def test_contract_timeline_gap(self):
        with self.lake.store.transaction() as conn:
            conn.execute("UPDATE contracts SET effective_to=? WHERE contract_id=?", ("2026-02-01T00:00:00+00:00", "visit-v1"))
        self.lake.register_contract("v2", "visits", ["event_id", "exhibit_id", "event_at", "count", "channel"], "2026-03-01T00:00:00+00:00")
        quality = self.lake.quality.sources("venue")
        self.assertEqual(len(quality["sources"][0]["gaps"]), 1)
        self.assertEqual(self.lake.contracts.resolve("visits", STAMP)["contract_id"], "v2")

    def test_ingest_success_and_raw_landing(self):
        result = self.lake.ingest("visits", "visit-v1", "key", [self.visit()])
        self.assertEqual((result["total_rows"], result["valid_rows"], result["rejected_rows"]), (1, 1, 0))
        raw = self.lake.batches.raw(result["batch_id"])
        self.assertEqual(raw["rows"][0]["payload"], self.visit())
        self.assertTrue(self.lake.audit.batch_integrity(result["batch_id"])["valid"])

    def test_idempotent_request_key(self):
        first = self.lake.ingest("visits", "visit-v1", "key", [self.visit()])
        replay = self.lake.ingest("visits", "visit-v1", "key", [self.visit()])
        self.assertEqual(first["batch_id"], replay["batch_id"])
        self.assertTrue(replay["replay"])
        self.assertEqual(self.lake.snapshots.inventory("venue", "2026-03-01")["sources"][0]["events"], 1)

    def test_request_key_collision(self):
        self.lake.ingest("visits", "visit-v1", "key", [self.visit()])
        with self.assertRaises(Conflict):
            self.lake.ingest("visits", "visit-v1", "key", [self.visit(count=4)])

    def test_duplicate_event_in_next_batch_is_rejected(self):
        self.lake.ingest("visits", "visit-v1", "first", [self.visit()])
        second = self.lake.ingest("visits", "visit-v1", "second", [self.visit()])
        self.assertEqual(second["rejections"][0]["code"], "duplicate_event")

    def test_changed_event_in_next_batch_conflicts(self):
        self.lake.ingest("visits", "visit-v1", "first", [self.visit()])
        second = self.lake.ingest("visits", "visit-v1", "second", [self.visit(count=7)])
        self.assertEqual(second["rejections"][0]["code"], "event_conflict")

    def test_mixed_batch_isolated_rows(self):
        rows = [self.visit("ok", 2), self.visit("bad", -1), self.visit("ok2", 5)]
        result = self.lake.ingest("visits", "visit-v1", "mixed", rows)
        self.assertEqual(result["valid_rows"], 2)
        self.assertEqual(result["rejected_rows"], 1)
        self.assertEqual(result["rejections"][0]["row_no"], 2)
        self.assertEqual(len(self.lake.batches.raw(result["batch_id"])["rows"]), 3)

    def test_visitor_count_rejects_boolean(self):
        result = self.lake.ingest("visits", "visit-v1", "bool", [self.visit(count=True)])
        self.assertEqual(result["rejections"][0]["code"], "invalid_count")

    def test_naive_event_time_rejected(self):
        result = self.lake.ingest("visits", "visit-v1", "time", [self.visit(stamp="2026-03-01T10:00:00")])
        self.assertEqual(result["rejections"][0]["code"], "invalid_identity_or_time")

    def test_unknown_exhibit_rejected(self):
        result = self.lake.ingest("visits", "visit-v1", "missing", [self.visit(exhibit_id="missing")])
        self.assertEqual(result["rejections"][0]["code"], "unknown_exhibit")

    def test_preview_does_not_ingest(self):
        report = self.lake.preview.inspect("visits", "visit-v1", [self.visit()])
        self.assertEqual(report["would_accept"], 1)
        self.assertEqual(self.lake.snapshots.inventory("venue", "2026-03-01")["sources"], [])

    def test_preview_detects_duplicate(self):
        report = self.lake.preview.inspect("visits", "visit-v1", [self.visit(), self.visit()])
        self.assertEqual(report["would_reject"], 1)
        self.assertEqual(report["rows"][1]["code"], "duplicate_in_batch")

    def test_provenance_traces_source(self):
        batch = self.lake.ingest("visits", "visit-v1", "key", [self.visit()])
        result = self.lake.provenance.event("visits", "e1")
        self.assertEqual(result["batch"]["batch_id"], batch["batch_id"])
        self.assertEqual(result["raw"], self.visit())
        self.assertEqual(result["contract"]["revision"], 1)

    def test_contract_description_and_compatibility(self):
        description = self.lake.contracts.describe("visit-v1")
        self.assertIn("count", description["contract"]["fields"])
        compare = self.lake.contracts.compatibility("visit-v1", "visit-v1")
        self.assertTrue(compare["same_schema"])

    def test_quality_reports_pending(self):
        self.lake.ingest("visits", "visit-v1", "key", [self.visit()])
        quality = self.lake.quality.partition("venue", "2026-03-01")
        self.assertEqual(quality["pending_events"], 1)
        self.assertEqual(quality["exhibits_available"], 1)
