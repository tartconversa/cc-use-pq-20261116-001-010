"""连接登记、入湖、发布与分析流程的应用服务。"""
from __future__ import annotations

from pathlib import Path

from exhibit_lake.catalog.registry import Registry
from exhibit_lake.catalog.store import Store
from exhibit_lake.catalog.audit import Audit
from exhibit_lake.catalog.quality import Quality
from exhibit_lake.catalog.contracts import Contracts
from exhibit_lake.ingest.batches import Batches
from exhibit_lake.ingest.preview import Preview
from exhibit_lake.publish.snapshots import Snapshots
from exhibit_lake.publish.recovery import Recovery
from exhibit_lake.query.analytics import Analytics
from exhibit_lake.query.exports import Exports
from exhibit_lake.query.coverage import Coverage
from exhibit_lake.query.experience import Experience
from exhibit_lake.query.provenance import Provenance
from exhibit_lake.query.batch_quality import BatchQuality
from exhibit_lake.metrics.definitions import MetricDefinitions
from exhibit_lake.metrics.service import Metrics


class ExhibitLake:
    def __init__(self, database: str | Path):
        self.store = Store(database)
        self.registry = Registry(self.store)
        self.batches = Batches(self.store)
        self.preview = Preview(self.store)
        self.snapshots = Snapshots(self.store)
        self.recovery = Recovery(self.store, self.snapshots)
        self.analytics = Analytics(self.store, self.snapshots)
        self.audit = Audit(self.store)
        self.exports = Exports(self.analytics)
        self.coverage = Coverage(self.store)
        self.experience = Experience(self.store, self.analytics)
        self.quality = Quality(self.store)
        self.contracts = Contracts(self.store)
        self.provenance = Provenance(self.store)
        self.batch_quality = BatchQuality(self.store)
        self.metric_definitions = MetricDefinitions(self.store)
        self.metrics = Metrics(self.store, self.metric_definitions, self.snapshots)

    def register_venue(self, venue_id: str, name: str, timezone: str) -> dict:
        return self.registry.venue(venue_id, name, timezone)

    def register_exhibit(self, exhibit_id: str, venue_id: str, name: str,
                         maker: str, launched_on: str, withdrawn_on: str | None = None) -> dict:
        return self.registry.exhibit(exhibit_id, venue_id, name, maker, launched_on, withdrawn_on)

    def register_source(self, source_id: str, venue_id: str, kind: str) -> dict:
        return self.registry.source(source_id, venue_id, kind)

    def register_contract(self, contract_id: str, source_id: str, fields: list[str],
                          effective_from: str, effective_to: str | None = None) -> dict:
        return self.registry.contract(contract_id, source_id, fields, effective_from, effective_to)

    def ingest(self, source_id: str, contract_id: str, request_key: str, events: list[object]) -> dict:
        return self.batches.ingest(source_id, contract_id, request_key, events)

    def publish_day(self, venue_id: str, local_day: str, expected_head: str | None = None) -> dict:
        staged = self.snapshots.stage(venue_id, local_day, expected_head)
        return self.snapshots.publish(staged["snapshot_id"], expected_head)

    def overview(self, venue_id: str, local_day: str) -> dict:
        return {"head": self.snapshots.head(venue_id, local_day),
                "inventory": self.snapshots.inventory(venue_id, local_day)}