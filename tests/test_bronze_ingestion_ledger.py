import gzip
import json
from pathlib import Path
import re
import subprocess
import sys

import pytest

from elt.snowflake_io import (
    FileIdentityConflictError,
    IngestionError,
    IngestionInProgressError,
    _ingestion_writer_lock,
    load_file,
)


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []
        self.rowcount = -1
        self.sfqid = "fake-copy-query"
        self.closed = False

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split()).upper()
        params = params or ()
        self.rows = []
        self.rowcount = 0

        if normalized.startswith("SELECT STATUS, CONTENT_SHA256"):
            records = []
            if params[0] in self.connection.ledger:
                records.append(self.connection.ledger[params[0]])
            records.extend(
                self.connection.duplicate_ledger_rows.get(params[0], [])
            )
            self.rows = [
                (
                    record["STATUS"],
                    record["CONTENT_SHA256"],
                    record["STAGE_PATH"],
                    record["EXPECTED_ROWS"],
                    record["LOADED_ROWS"],
                )
                for record in records
            ]
        elif normalized.startswith("SELECT COUNT(*) FROM FRESHCART.BRONZE"):
            self.rows = [(
                self.connection.bronze_rows.get(params[0], 0),
            )]
        elif normalized.startswith("INSERT INTO FRESHCART.OPS.INGESTION_FILES"):
            file_id = params[0]
            record = {
                "SOURCE": params[1],
                "RUN_ID": params[2],
                "FILE_PATH": params[3],
                "STAGE_PATH": params[4],
                "CONTENT_SHA256": params[5],
                "EXPECTED_ROWS": params[6],
                "STATUS": params[7],
                "LOADED_ROWS": params[8],
                "COPY_QUERY_ID": None,
                "LAST_ERROR": params[9],
            }
            if file_id in self.connection.ledger:
                self.connection.duplicate_ledger_rows.setdefault(
                    file_id, []
                ).append(record)
            else:
                self.connection.ledger[file_id] = record
            self.rowcount = 1
        elif normalized.startswith("UPDATE FRESHCART.OPS.INGESTION_FILES"):
            if "SET STATUS = 'LOADING'" in normalized:
                source, run_id, file_path, expected, file_id, checksum, stage = params
                record = self.connection.ledger.get(file_id)
                if (
                    record
                    and record["STATUS"] == "FAILED"
                    and record["CONTENT_SHA256"] == checksum
                    and record["STAGE_PATH"] == stage
                ):
                    record.update({
                        "SOURCE": source,
                        "RUN_ID": run_id,
                        "FILE_PATH": file_path,
                        "EXPECTED_ROWS": expected,
                        "LOADED_ROWS": 0,
                        "STATUS": "LOADING",
                        "LAST_ERROR": None,
                    })
                    self.rowcount = 1
            elif (
                "SET STATUS = 'LOADED'" in normalized
                and "AND STATUS = 'FAILED'" in normalized
            ):
                loaded_rows, file_id = params
                record = self.connection.ledger.get(file_id)
                if record and record["STATUS"] == "FAILED":
                    record.update({
                        "STATUS": "LOADED",
                        "LOADED_ROWS": loaded_rows,
                        "LAST_ERROR": None,
                    })
                    self.rowcount = 1
            elif "SET STATUS = 'LOADED'" in normalized:
                loaded_rows, query_id, file_id = params
                if self.connection.fail_loaded_update:
                    self.connection.fail_loaded_update = False
                    raise RuntimeError(
                        "simulated ledger status update failure"
                    )
                record = self.connection.ledger.get(file_id)
                if record and record["STATUS"] == "LOADING":
                    record.update({
                        "STATUS": "LOADED",
                        "LOADED_ROWS": loaded_rows,
                        "COPY_QUERY_ID": query_id,
                        "LAST_ERROR": None,
                    })
                    self.rowcount = 1
            elif "SET STATUS = 'FAILED'" in normalized:
                query_id, error_message, file_id = params
                record = self.connection.ledger.get(file_id)
                if record and record["STATUS"] == "LOADING":
                    record.update({
                        "STATUS": "FAILED",
                        "COPY_QUERY_ID": query_id or record["COPY_QUERY_ID"],
                        "LAST_ERROR": error_message,
                    })
                    self.rowcount = 1
        elif normalized.startswith("PUT "):
            self.connection.put_count += 1
            self.rows = [("uploaded",)]
        elif normalized.startswith("COPY INTO"):
            self.connection.copy_count += 1
            if self.connection.fail_copy:
                self.connection.fail_copy = False
                raise RuntimeError("simulated COPY failure")
            match = re.search(r"FILES\s*=\s*\('([^']+)'\)", sql)
            assert match
            staged_file = match.group(1)
            row_count = (
                self.connection.copy_row_count
                if self.connection.copy_row_count is not None
                else self.connection.file_row_counts[staged_file]
            )
            self.connection.bronze_rows[staged_file] = row_count
            self.rows = [(
                staged_file,
                "LOADED",
                row_count,
                row_count,
                0,
                None,
            )]
        else:
            raise AssertionError(f"Unexpected SQL: {sql}")
        return self

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self):
        self.ledger = {}
        self.duplicate_ledger_rows = {}
        self.bronze_rows = {}
        self.file_row_counts = {}
        self.put_count = 0
        self.copy_count = 0
        self.fail_copy = False
        self.copy_row_count = None
        self.fail_loaded_update = False

    def cursor(self):
        return FakeCursor(self)


def _landing_file(tmp_path: Path, run_id: str, records: list[dict]) -> Path:
    folder = tmp_path / "web" / "2026-09-30"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"web__{run_id}.json.gz"
    with gzip.open(path, "wt", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps({
                "source": "web",
                "run_id": run_id,
                "extracted_at": "2026-09-30T00:00:00+00:00",
                "data": record,
            }) + "\n")
    return path


def _register_file_rows(connection, path: Path, count: int):
    connection.file_row_counts[
        f"web/{path.parent.name}/{path.name}"
    ] = count


def test_ledger_migration_uses_standard_table_and_expected_columns():
    migration = (
        Path(__file__).parents[1]
        / "elt"
        / "sql"
        / "01_ingestion_files.sql"
    ).read_text(encoding="utf-8")
    expected_columns = (
        "FILE_ID",
        "SOURCE",
        "RUN_ID",
        "FILE_PATH",
        "STAGE_PATH",
        "CONTENT_SHA256",
        "EXPECTED_ROWS",
        "STATUS",
        "LOADED_ROWS",
        "COPY_QUERY_ID",
        "LAST_ERROR",
        "CREATED_AT",
        "UPDATED_AT",
    )

    assert "CREATE TABLE IF NOT EXISTS FRESHCART.OPS.INGESTION_FILES" in migration
    assert "PRIMARY KEY" not in migration.upper()
    for column in expected_columns:
        assert column in migration


def test_same_logical_file_is_loaded_once_even_after_repeated_call(tmp_path):
    connection = FakeConnection()
    path = _landing_file(tmp_path, "run_one", [{"order_id": "ORD-1"}])
    _register_file_rows(connection, path, 1)

    first_result = load_file(connection, path, "web")
    second_result = load_file(connection, path, "web")

    assert first_result[0][1] == "LOADED"
    assert second_result == []
    assert connection.put_count == 1
    assert connection.copy_count == 1
    assert next(iter(connection.ledger.values()))["COPY_QUERY_ID"] == (
        "fake-copy-query"
    )
    expected_stage_relative_path = (
        f"web/{path.parent.name}/{path.name}"
    )
    assert connection.bronze_rows == {expected_stage_relative_path: 1}
    assert {row["STATUS"] for row in connection.ledger.values()} == {"LOADED"}


def test_different_run_files_are_distinct_ingestion_batches(tmp_path):
    connection = FakeConnection()
    first = _landing_file(tmp_path, "run_one", [{"order_id": "ORD-1"}])
    second = _landing_file(tmp_path, "run_two", [{"order_id": "ORD-1"}])
    _register_file_rows(connection, first, 1)
    _register_file_rows(connection, second, 1)

    load_file(connection, first, "web")
    load_file(connection, second, "web")

    assert len(connection.ledger) == 2
    assert connection.copy_count == 2


def test_failed_copy_is_recorded_and_safe_retry_succeeds(tmp_path):
    connection = FakeConnection()
    connection.fail_copy = True
    path = _landing_file(tmp_path, "retry_run", [{"order_id": "ORD-1"}])
    _register_file_rows(connection, path, 1)

    with pytest.raises(RuntimeError, match="simulated COPY failure"):
        load_file(connection, path, "web")

    assert next(iter(connection.ledger.values()))["STATUS"] == "FAILED"
    assert connection.bronze_rows == {}

    result = load_file(connection, path, "web")

    assert result[0][1] == "LOADED"
    assert next(iter(connection.ledger.values()))["STATUS"] == "LOADED"
    assert connection.put_count == 2
    assert connection.copy_count == 2


def test_ledger_update_failure_after_copy_is_reconciled_without_reload(
    tmp_path,
):
    connection = FakeConnection()
    connection.fail_loaded_update = True
    path = _landing_file(tmp_path, "ledger_retry", [{"order_id": "ORD-1"}])
    _register_file_rows(connection, path, 1)

    with pytest.raises(RuntimeError, match="ledger status update failure"):
        load_file(connection, path, "web")

    record = next(iter(connection.ledger.values()))
    assert record["STATUS"] == "FAILED"
    assert record["COPY_QUERY_ID"] == "fake-copy-query"
    assert connection.bronze_rows == {
        f"web/{path.parent.name}/{path.name}": 1
    }

    assert load_file(connection, path, "web") == []
    assert record["STATUS"] == "LOADED"
    assert connection.copy_count == 1


def test_failed_ledger_status_with_verified_bronze_rows_recovers_without_copy(
    tmp_path,
):
    connection = FakeConnection()
    path = _landing_file(tmp_path, "recover_run", [{"order_id": "ORD-1"}])
    _register_file_rows(connection, path, 1)

    load_file(connection, path, "web")
    record = next(iter(connection.ledger.values()))
    record["STATUS"] = "FAILED"
    record["LAST_ERROR"] = "simulated failure after COPY"
    original_copy_count = connection.copy_count

    result = load_file(connection, path, "web")

    assert result == []
    assert record["STATUS"] == "LOADED"
    assert connection.copy_count == original_copy_count


def test_partial_bronze_rows_block_retry_instead_of_copying_twice(tmp_path):
    connection = FakeConnection()
    path = _landing_file(
        tmp_path,
        "partial_run",
        [{"order_id": "ORD-1"}, {"order_id": "ORD-2"}],
    )
    _register_file_rows(connection, path, 2)
    connection.copy_row_count = 1

    with pytest.raises(IngestionError, match="expected 2"):
        load_file(connection, path, "web")

    with pytest.raises(IngestionError, match="expected 2"):
        load_file(connection, path, "web")

    assert connection.copy_count == 1
    assert connection.put_count == 1


def test_legacy_bronze_rows_without_ledger_are_not_assumed_to_match(
    tmp_path,
):
    connection = FakeConnection()
    path = _landing_file(tmp_path, "legacy_run", [{"order_id": "ORD-1"}])
    _register_file_rows(connection, path, 1)
    staged_file = f"web/{path.parent.name}/{path.name}"
    connection.bronze_rows[staged_file] = 1

    with pytest.raises(IngestionError, match="no ledger record exists"):
        load_file(connection, path, "web")

    assert connection.copy_count == 0
    assert connection.ledger == {}


def test_duplicate_ledger_rows_are_reported_as_ambiguous(tmp_path):
    connection = FakeConnection()
    path = _landing_file(tmp_path, "duplicate_run", [{"order_id": "ORD-1"}])
    _register_file_rows(connection, path, 1)
    load_file(connection, path, "web")
    file_id, record = next(iter(connection.ledger.items()))
    connection.duplicate_ledger_rows[file_id] = [record.copy()]
    copies_before_retry = connection.copy_count

    with pytest.raises(IngestionError, match="duplicate registrations"):
        load_file(connection, path, "web")

    assert connection.copy_count == copies_before_retry


def test_writer_lock_rejects_another_process_on_same_host(tmp_path):
    child_script = """
from elt.snowflake_io import IngestionInProgressError, _ingestion_writer_lock
try:
    with _ingestion_writer_lock():
        raise SystemExit(2)
except IngestionInProgressError:
    print('writer blocked')
"""
    with _ingestion_writer_lock():
        result = subprocess.run(
            [sys.executable, "-c", child_script],
            capture_output=True,
            text=True,
            timeout=30,
        )

    assert result.returncode == 0, result.stderr
    assert "writer blocked" in result.stdout


def test_changed_content_cannot_reuse_a_logical_file_identity(tmp_path):
    connection = FakeConnection()
    path = _landing_file(tmp_path, "same_run", [{"order_id": "ORD-1"}])
    _register_file_rows(connection, path, 1)
    load_file(connection, path, "web")
    _landing_file(tmp_path, "same_run", [{"order_id": "ORD-2"}])

    with pytest.raises(FileIdentityConflictError, match="different content"):
        load_file(connection, path, "web")

    assert connection.copy_count == 1


def test_active_ledger_claim_prevents_a_second_loader(tmp_path):
    connection = FakeConnection()
    path = _landing_file(tmp_path, "active_run", [{"order_id": "ORD-1"}])
    _register_file_rows(connection, path, 1)
    connection.ledger["placeholder"] = {"STATUS": "LOADING"}

    # The first call creates the identity claim, modeling a worker that stopped
    # after claiming but before COPY. The next invocation must not take it over.
    connection.fail_copy = True
    with pytest.raises(RuntimeError):
        load_file(connection, path, "web")
    record = next(
        entry for key, entry in connection.ledger.items()
        if key != "placeholder"
    )
    assert record["STATUS"] == "FAILED"
    record["STATUS"] = "LOADING"

    with pytest.raises(IngestionInProgressError):
        load_file(connection, path, "web")