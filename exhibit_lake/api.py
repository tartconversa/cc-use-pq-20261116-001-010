"""FastAPI 传输层，提供稳定错误格式和明确的请求契约。"""
from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

from exhibit_lake.errors import LakeError
from exhibit_lake.service import ExhibitLake


class VenueIn(BaseModel):
    venue_id: str
    name: str
    timezone: str


class ExhibitIn(BaseModel):
    exhibit_id: str
    venue_id: str
    name: str
    maker: str
    launched_on: str
    withdrawn_on: str | None = None


class SourceIn(BaseModel):
    source_id: str
    venue_id: str
    kind: str


class ContractIn(BaseModel):
    contract_id: str
    source_id: str
    fields: list[str]
    effective_from: str
    effective_to: str | None = None


class BatchIn(BaseModel):
    source_id: str
    contract_id: str
    request_key: str
    events: list[Any]


class StageIn(BaseModel):
    venue_id: str
    local_day: str
    expected_head: str | None = None


class PublishIn(BaseModel):
    expected_head: str | None = None


class WithdrawIn(BaseModel):
    withdrawn_on: str


class MetricDefinitionIn(BaseModel):
    metric_key: str
    name: str
    description: str | None = None
    spec: dict


class RecomputeIn(BaseModel):
    metric_key: str
    venue_id: str
    local_day: str
    snapshot_id: str | None = None
    request_key: str
    definition_version: int | None = None
    definition: dict | None = None
    exhibit_ids: list[str] | None = None
    defer: bool = False


class MetricPublishIn(BaseModel):
    expected_publication_id: str | None = None
    reason: str | None = None


def create_app(database: str | None = None) -> FastAPI:
    lake = ExhibitLake(database or os.environ.get("EXHIBIT_LAKE_DB", "exhibit_lake.db"))
    app = FastAPI(title="Exhibit Lake", version="1.0.0")
    app.state.lake = lake

    @app.exception_handler(LakeError)
    async def domain_error(request: Request, exc: LakeError):
        return JSONResponse(status_code=exc.status,
                            content={"error": {"code": exc.code, "message": str(exc), "details": exc.details}})

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.post("/venues", status_code=201)
    def venue(data: VenueIn):
        return lake.register_venue(data.venue_id, data.name, data.timezone)

    @app.get("/venues/{venue_id}")
    def venue_description(venue_id: str):
        return lake.registry.describe(venue_id)

    @app.post("/exhibits", status_code=201)
    def exhibit(data: ExhibitIn):
        return lake.register_exhibit(data.exhibit_id, data.venue_id, data.name,
                                     data.maker, data.launched_on, data.withdrawn_on)

    @app.post("/exhibits/{exhibit_id}/withdraw")
    def withdraw(exhibit_id: str, data: WithdrawIn):
        return lake.registry.withdraw(exhibit_id, data.withdrawn_on)

    @app.post("/sources", status_code=201)
    def source(data: SourceIn):
        return lake.register_source(data.source_id, data.venue_id, data.kind)

    @app.post("/sources/{source_id}/deactivate")
    def deactivate(source_id: str):
        return lake.registry.deactivate_source(source_id)

    @app.post("/contracts", status_code=201)
    def contract(data: ContractIn):
        return lake.register_contract(data.contract_id, data.source_id, data.fields,
                                      data.effective_from, data.effective_to)

    @app.post("/batches", status_code=201)
    def batch(data: BatchIn):
        return lake.ingest(data.source_id, data.contract_id, data.request_key, data.events)

    @app.post("/batches/preview")
    def preview_batch(data: BatchIn):
        return lake.preview.inspect(data.source_id, data.contract_id, data.events)

    @app.get("/batches/{batch_id}")
    def batch_report(batch_id: str):
        return lake.batches.report(batch_id)

    @app.get("/batches/{batch_id}/raw")
    def batch_raw(batch_id: str, offset: int = 0, limit: int = 100):
        return lake.batches.raw(batch_id, offset=offset, limit=limit)

    @app.get("/partitions/{venue_id}/{local_day}")
    def partition(venue_id: str, local_day: str):
        return lake.overview(venue_id, local_day)

    @app.post("/snapshots/stage", status_code=201)
    def stage(data: StageIn):
        return lake.snapshots.stage(data.venue_id, data.local_day, data.expected_head)

    @app.get("/snapshots/{snapshot_id}")
    def snapshot(snapshot_id: str):
        return lake.snapshots.describe(snapshot_id)

    @app.get("/snapshots/{snapshot_id}/verify")
    def verify(snapshot_id: str):
        return lake.snapshots.verify(snapshot_id)

    @app.post("/snapshots/{snapshot_id}/publish")
    def publish(snapshot_id: str, data: PublishIn):
        return lake.snapshots.publish(snapshot_id, data.expected_head)

    @app.post("/snapshots/{snapshot_id}/abandon")
    def abandon(snapshot_id: str):
        return lake.snapshots.abandon(snapshot_id)

    @app.get("/snapshots/{snapshot_id}/lineage")
    def lineage(snapshot_id: str):
        return lake.analytics.lineage(snapshot_id)

    @app.get("/venues/{venue_id}/days/{local_day}/report")
    def report(venue_id: str, local_day: str, snapshot_id: str | None = None):
        return lake.analytics.day(venue_id, local_day, snapshot_id)

    @app.get("/venues/{venue_id}/days/{local_day}/events")
    def events(venue_id: str, local_day: str, snapshot_id: str | None = None,
               exhibit_id: str | None = None, kind: str | None = None,
               offset: int = 0, limit: int = 100):
        return lake.analytics.events(venue_id, local_day, snapshot_id,
                                     exhibit_id=exhibit_id, kind=kind, offset=offset, limit=limit)

    @app.get("/venues/{venue_id}/days/{local_day}/snapshots")
    def snapshots(venue_id: str, local_day: str, include_staged: bool = False):
        return lake.snapshots.list(venue_id, local_day, include_staged=include_staged)

    @app.get("/venues/{venue_id}/days/{local_day}/compare")
    def compare(venue_id: str, local_day: str, left_id: str, right_id: str):
        return lake.analytics.compare(venue_id, local_day, left_id, right_id)

    @app.get("/venues/{venue_id}/trend")
    def trend(venue_id: str, start_day: str, end_day: str):
        return lake.analytics.trend(venue_id, start_day, end_day)

    @app.get("/venues/{venue_id}/days/{local_day}/export.csv", response_class=PlainTextResponse)
    def export_day(venue_id: str, local_day: str, snapshot_id: str | None = None):
        return lake.exports.daily_csv(venue_id, local_day, snapshot_id)

    @app.get("/venues/{venue_id}/days/{local_day}/events.csv", response_class=PlainTextResponse)
    def export_events(venue_id: str, local_day: str, snapshot_id: str | None = None):
        return lake.exports.events_csv(venue_id, local_day, snapshot_id)

    @app.get("/audits")
    def audit_trail(action: str | None = None, subject: str | None = None, offset: int = 0, limit: int = 100):
        return lake.audit.trail(action=action, subject=subject, offset=offset, limit=limit)

    @app.get("/batches/{batch_id}/integrity")
    def batch_integrity(batch_id: str):
        return lake.audit.batch_integrity(batch_id)

    @app.get("/venues/{venue_id}/integrity")
    def venue_integrity(venue_id: str):
        return lake.audit.venue_integrity(venue_id)

    @app.get("/venues/{venue_id}/days/{local_day}/coverage")
    def day_coverage(venue_id: str, local_day: str):
        return lake.coverage.day(venue_id, local_day)

    @app.get("/venues/{venue_id}/coverage")
    def range_coverage(venue_id: str, start_day: str, end_day: str):
        return lake.coverage.range(venue_id, start_day, end_day)

    @app.get("/venues/{venue_id}/sources/quality")
    def source_quality(venue_id: str):
        return lake.quality.sources(venue_id)

    @app.get("/venues/{venue_id}/days/{local_day}/quality")
    def partition_quality(venue_id: str, local_day: str):
        return lake.quality.partition(venue_id, local_day)

    @app.get("/venues/{venue_id}/days/{local_day}/exhibits/{exhibit_id}/experience")
    def experience(venue_id: str, local_day: str, exhibit_id: str, snapshot_id: str | None = None):
        return lake.experience.exhibit(venue_id, local_day, exhibit_id, snapshot_id)

    @app.get("/venues/{venue_id}/days/{local_day}/experience")
    def compare_experience(venue_id: str, local_day: str, exhibit_ids: str, snapshot_id: str | None = None):
        return lake.experience.compare_exhibits(venue_id, local_day, exhibit_ids.split(","), snapshot_id)

    @app.get("/contracts/{contract_id}")
    def contract_description(contract_id: str):
        return lake.contracts.describe(contract_id)

    @app.get("/sources/{source_id}/contracts")
    def contract_timeline(source_id: str):
        return lake.contracts.timeline(source_id)

    @app.get("/sources/{source_id}/contract-at")
    def contract_at(source_id: str, event_at: str):
        return lake.contracts.resolve(source_id, event_at)

    @app.get("/contracts/{left_id}/compare/{right_id}")
    def compare_contracts(left_id: str, right_id: str):
        return lake.contracts.compatibility(left_id, right_id)

    @app.get("/sources/{source_id}/events/{event_id}/provenance")
    def event_provenance(source_id: str, event_id: str):
        return lake.provenance.event(source_id, event_id)

    @app.get("/batches/{batch_id}/provenance")
    def batch_provenance(batch_id: str):
        return lake.provenance.batch(batch_id)

    @app.get("/venues/{venue_id}/days/{local_day}/recovery")
    def recovery(venue_id: str, local_day: str):
        return lake.recovery.inspect(venue_id, local_day)

    @app.post("/venues/{venue_id}/days/{local_day}/abandon-stale")
    def abandon_stale(venue_id: str, local_day: str):
        return lake.recovery.abandon_stale(venue_id, local_day)

    @app.get("/snapshots/{snapshot_id}/diagnostics")
    def candidate_diagnostics(snapshot_id: str):
        return lake.recovery.candidate_diagnostics(snapshot_id)

    @app.get("/sources/{source_id}/batch-quality")
    def source_batch_quality(source_id: str):
        return lake.batch_quality.source(source_id)

    @app.get("/venues/{venue_id}/days/{local_day}/batch-quality")
    def day_batch_quality(venue_id: str, local_day: str):
        return lake.batch_quality.partition(venue_id, local_day)

    @app.post("/metrics", status_code=201)
    def register_metric(data: MetricDefinitionIn):
        result = lake.metric_definitions.register(data.metric_key, data.name, data.spec, data.description)
        return result

    @app.get("/metrics")
    def metric_catalog():
        return lake.metric_definitions.catalog()

    @app.get("/metrics/{metric_key}/definitions")
    def metric_definitions(metric_key: str):
        return lake.metric_definitions.versions(metric_key)

    @app.get("/metrics/{metric_key}/definitions/{version}")
    def metric_definition(metric_key: str, version: int):
        return lake.metric_definitions.get(metric_key, version)

    @app.post("/metrics/recomputes", status_code=201)
    def recompute_metric(data: RecomputeIn):
        return lake.metrics.recompute(data.metric_key, data.venue_id, data.local_day,
                                      data.snapshot_id, data.request_key,
                                      data.definition_version, data.exhibit_ids,
                                      defer=data.defer, definition=data.definition)

    @app.get("/metrics/recomputes")
    def recompute_tasks(venue_id: str | None = None, local_day: str | None = None,
                        metric_key: str | None = None, status: str | None = None,
                        offset: int = 0, limit: int = 100):
        return lake.metrics.tasks(venue_id=venue_id, local_day=local_day, metric_key=metric_key,
                                  status=status, offset=offset, limit=limit)

    @app.get("/metrics/recomputes/{task_id}")
    def recompute_task(task_id: str):
        return lake.metrics.task(task_id)

    @app.post("/metrics/recomputes/{task_id}/retry")
    def retry_recompute(task_id: str):
        return lake.metrics.retry(task_id)

    @app.get("/metrics/results/{result_id}")
    def metric_result(result_id: str):
        return lake.metrics.result(result_id)

    @app.get("/metrics/results/{result_id}/trace")
    def metric_trace(result_id: str):
        return lake.metrics.trace(result_id)

    @app.get("/metrics/{metric_key}/results")
    def metric_results(metric_key: str, venue_id: str, local_day: str):
        return lake.metrics.results(metric_key, venue_id, local_day)

    @app.post("/metrics/results/{result_id}/publish", status_code=201)
    def publish_metric(result_id: str, data: MetricPublishIn):
        return lake.metrics.publish(result_id, data.expected_publication_id, data.reason)

    @app.get("/snapshots/{snapshot_id}/metrics")
    def snapshot_metrics(snapshot_id: str, metric_key: str | None = None):
        return lake.metrics.publications(snapshot_id, metric_key=metric_key)

    @app.get("/venues/{venue_id}/days/{local_day}/metrics")
    def day_metrics(venue_id: str, local_day: str, snapshot_id: str | None = None):
        return lake.metrics.day(venue_id, local_day, snapshot_id)

    @app.get("/venues/{venue_id}/days/{local_day}/metrics/{metric_key}")
    def metric_report(venue_id: str, local_day: str, metric_key: str,
                      snapshot_id: str | None = None, definition_version: int | None = None):
        return lake.metrics.report(venue_id, local_day, metric_key, snapshot_id, definition_version)

    return app


app = create_app()
