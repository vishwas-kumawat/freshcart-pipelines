from pathlib import Path

import pytest

from elt import extract
from elt import run_pipeline as pipeline


class FakeConnection:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def _patch_pipeline(monkeypatch, extraction_action=None, load_action=None):
    connection = FakeConnection()
    sql_files = []
    logs = []
    monkeypatch.setattr(
        pipeline,
        "get_snowflake_connection",
        lambda: connection,
    )
    monkeypatch.setattr(
        pipeline,
        "_run_sql_file",
        lambda unused_connection, file_name: sql_files.append(file_name),
    )
    monkeypatch.setattr(
        pipeline,
        "_log_step",
        lambda unused_connection, run_id, step, status, detail:
            logs.append((step, status, detail)),
    )
    if extraction_action is not None:
        monkeypatch.setattr(pipeline, "_extract_sources", extraction_action)
    if load_action is not None:
        monkeypatch.setattr(pipeline, "_load_extracted_files", load_action)
    monkeypatch.setattr(pipeline, "load_missing_pincodes", lambda *args, **kwargs: 0)
    monkeypatch.setattr(pipeline, "quality_checks", lambda unused_connection: {})
    return connection, sql_files, logs


def test_failed_extraction_is_logged_and_prevents_load_and_transform(monkeypatch):
    def failed_extraction(*args, **kwargs):
        raise extract.InvalidAPIResponseError("missing required source key")

    loaded = []
    connection, sql_files, logs = _patch_pipeline(
        monkeypatch,
        extraction_action=failed_extraction,
        load_action=lambda *args: loaded.append(args),
    )

    with pytest.raises(
        extract.InvalidAPIResponseError,
        match="missing required source key",
    ):
        pipeline.run_pipeline(mode="mock", run_id="failed-extraction")

    assert connection.closed
    assert sql_files == ["00_setup.sql", "01_ingestion_files.sql"]
    assert loaded == []
    assert ("extract", "FAILED", "InvalidAPIResponseError: missing required source key") in logs
    assert not any(step == "clean" for step, _, _ in logs)


def test_failed_bronze_load_is_logged_and_prevents_transformations(monkeypatch):
    batch = pipeline.ExtractionBatch(
        files=(("web", Path("web__run.json.gz")),),
        record_counts=(("web", 1),),
    )
    connection, sql_files, logs = _patch_pipeline(
        monkeypatch,
        extraction_action=lambda *args, **kwargs: batch,
        load_action=lambda *args: (_ for _ in ()).throw(
            RuntimeError("simulated load failure")
        ),
    )

    with pytest.raises(RuntimeError, match="simulated load failure"):
        pipeline.run_pipeline(mode="mock", run_id="failed-load")

    assert connection.closed
    assert sql_files == ["00_setup.sql", "01_ingestion_files.sql"]
    assert ("load_bronze", "FAILED", "RuntimeError: simulated load failure") in logs
    assert not any(step == "clean" for step, _, _ in logs)


def test_valid_zero_record_sources_are_logged_and_pipeline_continues(monkeypatch):
    batch = pipeline.ExtractionBatch(
        files=(),
        record_counts=(
            ("web", 0),
            ("mobile", 0),
            ("crm", 0),
            ("fx", 1),
        ),
    )
    load_calls = []
    connection, sql_files, logs = _patch_pipeline(
        monkeypatch,
        extraction_action=lambda *args, **kwargs: batch,
        load_action=lambda unused_connection, loaded_batch:
            load_calls.append(loaded_batch) or 0,
    )

    pipeline.run_pipeline(mode="mock", run_id="empty-source")

    assert connection.closed
    assert load_calls == [batch]
    assert sql_files == [
        "00_setup.sql",
        "01_ingestion_files.sql",
        "10_orders_clean.sql",
        "20_orders_dedup.sql",
        "30_customer_tier_scd2.sql",
        "31_fx_rates.sql",
        "32_pincode_geo.sql",
        "40_gold.sql",
    ]
    extraction_log = next(entry for entry in logs if entry[0] == "extract")
    assert extraction_log[1] == "SUCCEEDED"
    assert "web=0" in extraction_log[2]
    assert "mobile=0" in extraction_log[2]
    assert ("load_bronze", "SUCCEEDED", "Files loaded or already loaded: 0") in logs


def test_transform_only_does_not_extract_or_load(monkeypatch):
    def unexpected_extraction(*args, **kwargs):
        raise AssertionError("transform-only must not extract")

    def unexpected_load(*args):
        raise AssertionError("transform-only must not load Bronze")

    connection, sql_files, logs = _patch_pipeline(
        monkeypatch,
        extraction_action=unexpected_extraction,
        load_action=unexpected_load,
    )

    pipeline.run_pipeline(mode="transform-only", run_id="transform-only")

    assert connection.closed
    assert sql_files == [
        "00_setup.sql",
        "01_ingestion_files.sql",
        "10_orders_clean.sql",
        "20_orders_dedup.sql",
        "30_customer_tier_scd2.sql",
        "31_fx_rates.sql",
        "32_pincode_geo.sql",
        "40_gold.sql",
    ]
    assert not any(step in {"extract", "load_bronze"} for step, _, _ in logs)