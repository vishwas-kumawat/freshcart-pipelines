
import gzip
import hashlib
import json
import os
import re
import tempfile
from contextlib import contextmanager
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


BRONZE_TABLES = {
    "web": "FRESHCART.BRONZE.ORDERS_RAW",
    "mobile": "FRESHCART.BRONZE.ORDERS_RAW",
    "crm": "FRESHCART.BRONZE.CUSTOMERS_RAW",
    "fx": "FRESHCART.BRONZE.FX_RAW",
    "pincode": "FRESHCART.BRONZE.PINCODE_RAW",
}

BRONZE_STAGE = "@FRESHCART.BRONZE.LANDING"
FILE_FORMAT = "FRESHCART.BRONZE.FF_NDJSON"
INGESTION_LEDGER = "FRESHCART.OPS.INGESTION_FILES"


class IngestionError(RuntimeError):
    """A landing file could not be safely claimed or loaded."""


class IngestionInProgressError(IngestionError):
    """Another ingestion attempt currently owns the logical file."""


class FileIdentityConflictError(IngestionError):
    """A logical landing path was reused with different file contents."""


class AlreadyLoadedError(IngestionError):
    """Another writer completed the logical file load."""


@contextmanager
def _ingestion_writer_lock():
    """Lock ingestion writers on this host; deployment must remain single-host."""
    account_key = hashlib.sha256(
        os.getenv("SNOWFLAKE_ACCOUNT", "freshcart").casefold().encode("utf-8")
    ).hexdigest()[:16]
    lock_path = Path(tempfile.gettempdir()) / (
        f"freshcart-ingestion-{account_key}.lock"
    )
    lock_file = lock_path.open("a+b")
    try:
        lock_file.seek(0, os.SEEK_END)
        if lock_file.tell() == 0:
            lock_file.write(b"\0")
            lock_file.flush()
        lock_file.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(
                    lock_file.fileno(),
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
        except OSError as error:
            raise IngestionInProgressError(
                "Another FreshCart ingestion writer is active on this host"
            ) from error
        try:
            yield
        finally:
            lock_file.seek(0)
            if os.name == "nt":
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    finally:
        lock_file.close()


def get_snowflake_connection():
    """Create a Snowflake connection using local environment variables."""

    required_variables = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
    ]

    missing = [
        variable
        for variable in required_variables
        if not os.getenv(variable)
    ]

    if missing:
        raise RuntimeError(
            f"Missing required environment variables: "
            f"{', '.join(missing)}"
        )

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database="FRESHCART",
        schema="BRONZE",
    )


def test_connection():
    """Verify connectivity and print non-sensitive session details."""

    connection = get_snowflake_connection()

    try:
        cursor = connection.cursor()

        try:
            cursor.execute("""
                SELECT
                    CURRENT_ACCOUNT_NAME(),
                    CURRENT_USER(),
                    CURRENT_ROLE(),
                    CURRENT_WAREHOUSE(),
                    CURRENT_DATABASE()
            """)

            result = cursor.fetchone()

            print("Snowflake connection successful!")
            print("Account:", result[0])
            print("User:", result[1])
            print("Role:", result[2])
            print("Warehouse:", result[3])
            print("Database:", result[4])

        finally:
            cursor.close()

    finally:
        connection.close()


def load_file(connection, local_path, source):
    """Load one file while holding the host-wide single-writer lock."""
    with _ingestion_writer_lock():
        return _load_file_locked(connection, local_path, source)


def _load_file_locked(connection, local_path, source):
    """
    Upload one compressed NDJSON file and load its raw envelopes
    into the appropriate Bronze table.

    Returns the COPY INTO result.
    """

    if source not in BRONZE_TABLES:
        raise ValueError(f"Unsupported source: {source}")

    local_path = Path(local_path).resolve()

    if not local_path.is_file():
        raise FileNotFoundError(local_path)

    if local_path.suffixes[-2:] != [".json", ".gz"]:
        raise ValueError(
            "Expected a compressed .json.gz NDJSON file"
        )

    if not re.fullmatch(r"[A-Za-z0-9_.-]+", local_path.name):
        raise ValueError("Invalid source filename")

    if not re.fullmatch(r"[A-Za-z0-9_-]+", source):
        raise ValueError("Invalid source name")

    run_date = local_path.parent.name

    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", run_date):
        raise ValueError(
            f"Expected a YYYY-MM-DD parent directory, got {run_date}"
        )

    target_table = BRONZE_TABLES[source]

    # A source/date/filename identifies a logical landing file across retries.
    stage_prefix = f"{source}/{run_date}"
    stage_file = f"{stage_prefix}/{local_path.name}"
    staged_filename = stage_file
    file_id = hashlib.sha256(
        f"{source.lower()}\n{stage_file}".encode("utf-8")
    ).hexdigest()
    content_sha256 = _file_sha256(local_path)
    run_id, expected_rows = _inspect_landing_file(local_path, source)

    record = _get_ingestion_record(connection, file_id)
    if record is not None:
        _verify_file_identity(record, content_sha256, stage_file)
        if record["STATUS"] == "LOADED":
            _verify_loaded_rows(
                connection,
                source,
                staged_filename,
                expected_rows,
            )
            print(f"\nAlready loaded; skipping: {local_path.name}")
            return []
        if record["STATUS"] == "LOADING":
            raise IngestionInProgressError(
                f"Ingestion is already in progress for {stage_file}; "
                "verify the active attempt before recovery"
            )
        if record["STATUS"] != "FAILED":
            raise IngestionError(
                f"Unsupported ingestion status for {stage_file}: "
                f"{record['STATUS']}"
            )
        existing_rows = _bronze_file_row_count(
            connection, source, staged_filename
        )
        if existing_rows == expected_rows:
            _recover_failed_ingestion(connection, file_id, expected_rows)
            print(f"\nRecovered completed load; skipping: {local_path.name}")
            return []
        if existing_rows:
            raise IngestionError(
                f"Bronze contains {existing_rows} rows for {staged_filename}; "
                f"expected {expected_rows}. Manual recovery is required."
            )
        try:
            _claim_failed_ingestion(
                connection,
                file_id,
                source,
                run_id,
                local_path,
                stage_file,
                content_sha256,
                expected_rows,
            )
        except AlreadyLoadedError:
            print(f"\nAlready loaded; skipping: {local_path.name}")
            return []
    else:
        existing_rows = _bronze_file_row_count(
            connection, source, staged_filename
        )
        if existing_rows:
            raise IngestionError(
                f"Bronze already contains {existing_rows} rows for "
                f"{staged_filename}, but no ledger record exists. The "
                "existing data cannot be verified against this file's "
                "checksum; reconcile it manually before retrying."
            )
        try:
            _insert_ingestion_record(
                connection,
                file_id,
                source,
                run_id,
                local_path,
                stage_file,
                content_sha256,
                expected_rows,
                "LOADING",
                0,
                None,
            )
        except Exception:
            # A single writer is required because the standard table has no
            # enforced uniqueness constraint.
            record = _get_ingestion_record(connection, file_id)
            if record is None:
                raise
            _verify_file_identity(record, content_sha256, stage_file)
            if record["STATUS"] == "LOADED":
                _verify_loaded_rows(
                    connection,
                    source,
                    staged_filename,
                    expected_rows,
                )
                print(f"\nAlready loaded; skipping: {local_path.name}")
                return []
            if record["STATUS"] == "LOADING":
                raise IngestionInProgressError(
                    f"Ingestion is already in progress for {stage_file}"
                )
            if record["STATUS"] != "FAILED":
                raise IngestionError(
                    f"Unsupported ingestion status for {stage_file}: "
                    f"{record['STATUS']}"
                )
            try:
                _claim_failed_ingestion(
                    connection,
                    file_id,
                    source,
                    run_id,
                    local_path,
                    stage_file,
                    content_sha256,
                    expected_rows,
                )
            except AlreadyLoadedError:
                print(f"\nAlready loaded; skipping: {local_path.name}")
                return []

    # Windows paths must be converted to forward slashes for PUT.
    file_uri = local_path.as_posix()

    if not file_uri.startswith("/"):
        file_uri = "/" + file_uri

    put_sql = (
        f"PUT 'file://{file_uri}' "
        f"{BRONZE_STAGE}/{stage_prefix}/ "
        "AUTO_COMPRESS=FALSE "
        "OVERWRITE=TRUE"
    )

    copy_query_id = None
    try:
        print(f"\nUploading: {local_path.name}")
        put_results = _execute_fetchall(connection, put_sql)

        for result in put_results:
            print("PUT:", result)

        # Preserve the envelope metadata and raw data separately.
        copy_sql = f"""
            COPY INTO {target_table} (
                SOURCE,
                RAW,
                RUN_ID,
                EXTRACTED_AT,
                SRC_FILE,
                SRC_ROW
            )
            FROM (
                SELECT
                    $1:source::STRING,
                    $1:data::VARIANT,
                    $1:run_id::STRING,
                    TRY_TO_TIMESTAMP_TZ(
                        $1:extracted_at::STRING
                    ),
                    METADATA$FILENAME,
                    METADATA$FILE_ROW_NUMBER
                FROM {BRONZE_STAGE}
            )
            FILES = ('{stage_file}')
            FILE_FORMAT = (
                FORMAT_NAME = '{FILE_FORMAT}'
            )
            ON_ERROR = 'ABORT_STATEMENT'
        """

        copy_cursor = connection.cursor()
        try:
            copy_cursor.execute(copy_sql)
            copy_results = copy_cursor.fetchall()
            copy_query_id = getattr(copy_cursor, "sfqid", None)
        finally:
            copy_cursor.close()

        loaded_rows = sum(
            int(result[3] or 0)
            for result in copy_results
            if len(result) > 3
        )
        if loaded_rows != expected_rows:
            raise IngestionError(
                f"COPY reported {loaded_rows} loaded rows for "
                f"{staged_filename}; expected {expected_rows}"
            )
        actual_rows = _bronze_file_row_count(
            connection, source, staged_filename
        )
        if actual_rows != expected_rows:
            raise IngestionError(
                f"COPY completed with {actual_rows} Bronze rows for "
                f"{staged_filename}; expected {expected_rows}"
            )

        print(f"COPY INTO {target_table}:")

        for result in copy_results:
            print(result)

        _mark_ingestion_loaded(
            connection,
            file_id,
            actual_rows,
            copy_query_id,
        )
        return copy_results

    except Exception as error:
        _mark_ingestion_failed(
            connection,
            file_id,
            error,
            copy_query_id=copy_query_id,
        )
        raise


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _inspect_landing_file(path: Path, source: str) -> tuple[str, int]:
    run_id = None
    row_count = 0
    try:
        with gzip.open(path, "rt", encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                if not line.strip():
                    raise ValueError(f"blank NDJSON row at line {line_number}")
                envelope = json.loads(line)
                if not isinstance(envelope, dict):
                    raise ValueError(f"invalid envelope at line {line_number}")
                if envelope.get("source") != source:
                    raise ValueError(
                        f"source mismatch at line {line_number}"
                    )
                row_run_id = envelope.get("run_id")
                if not isinstance(row_run_id, str) or not row_run_id:
                    raise ValueError(
                        f"missing run_id at line {line_number}"
                    )
                if run_id is not None and run_id != row_run_id:
                    raise ValueError("file contains multiple run IDs")
                if "data" not in envelope:
                    raise ValueError(f"missing data at line {line_number}")
                run_id = row_run_id
                row_count += 1
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise ValueError(f"Invalid landing NDJSON file {path.name}: {error}") from error

    if run_id is None:
        raise ValueError(f"Landing NDJSON file is empty: {path.name}")
    return run_id, row_count


def _execute_fetchone(connection, sql: str, params=None):
    cursor = connection.cursor()
    try:
        cursor.execute(sql, params) if params is not None else cursor.execute(sql)
        return cursor.fetchone()
    finally:
        cursor.close()


def _execute_fetchall(connection, sql: str, params=None):
    cursor = connection.cursor()
    try:
        cursor.execute(sql, params) if params is not None else cursor.execute(sql)
        return cursor.fetchall()
    finally:
        cursor.close()


def _execute(connection, sql: str, params=None) -> int:
    cursor = connection.cursor()
    try:
        cursor.execute(sql, params) if params is not None else cursor.execute(sql)
        return cursor.rowcount
    finally:
        cursor.close()


def _get_ingestion_record(connection, file_id: str):
    rows = _execute_fetchall(
        connection,
        f"""
        SELECT STATUS, CONTENT_SHA256, STAGE_PATH, EXPECTED_ROWS, LOADED_ROWS
        FROM {INGESTION_LEDGER}
        WHERE FILE_ID = %s
        """,
        (file_id,),
    )
    if not rows:
        return None
    if len(rows) != 1:
        raise IngestionError(
            f"Ledger contains {len(rows)} rows for FILE_ID {file_id}; "
            "duplicate registrations require manual reconciliation."
        )
    row = rows[0]
    return dict(zip(
        ("STATUS", "CONTENT_SHA256", "STAGE_PATH", "EXPECTED_ROWS", "LOADED_ROWS"),
        row,
    ))


def _verify_file_identity(record, checksum: str, stage_file: str) -> None:
    if (
        record["CONTENT_SHA256"] != checksum
        or record["STAGE_PATH"] != stage_file
    ):
        raise FileIdentityConflictError(
            f"Logical landing path {stage_file} was reused with different "
            "content; use a new run ID and filename"
        )


def _bronze_file_row_count(connection, source: str, staged_filename: str) -> int:
    table = BRONZE_TABLES[source]
    row = _execute_fetchone(
        connection,
        f"SELECT COUNT(*) FROM {table} WHERE SRC_FILE = %s",
        (staged_filename,),
    )
    return int(row[0]) if row else 0


def _insert_ingestion_record(
    connection,
    file_id: str,
    source: str,
    run_id: str,
    local_path: Path,
    stage_file: str,
    checksum: str,
    expected_rows: int,
    status: str,
    loaded_rows: int,
    error_message: str | None,
) -> None:
    _execute(
        connection,
        f"""
        INSERT INTO {INGESTION_LEDGER} (
          FILE_ID, SOURCE, RUN_ID, FILE_PATH, STAGE_PATH, CONTENT_SHA256,
          EXPECTED_ROWS, STATUS, LOADED_ROWS, LAST_ERROR
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            file_id,
            source,
            run_id,
            str(local_path),
            stage_file,
            checksum,
            expected_rows,
            status,
            loaded_rows,
            error_message,
        ),
    )


def _claim_failed_ingestion(
    connection,
    file_id: str,
    source: str,
    run_id: str,
    local_path: Path,
    stage_file: str,
    checksum: str,
    expected_rows: int,
) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute(
            f"""
            UPDATE {INGESTION_LEDGER}
            SET STATUS = 'LOADING',
                SOURCE = %s,
                RUN_ID = %s,
                FILE_PATH = %s,
                EXPECTED_ROWS = %s,
                LOADED_ROWS = 0,
                LAST_ERROR = NULL,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE FILE_ID = %s
              AND STATUS = 'FAILED'
              AND CONTENT_SHA256 = %s
              AND STAGE_PATH = %s
            """,
            (
                source,
                run_id,
                str(local_path),
                expected_rows,
                file_id,
                checksum,
                stage_file,
            ),
        )
        if cursor.rowcount != 1:
            record = _get_ingestion_record(connection, file_id)
            if record is not None and record["STATUS"] == "LOADED":
                _verify_loaded_rows(
                    connection,
                    source,
                    stage_file,
                    expected_rows,
                )
                raise AlreadyLoadedError(
                    f"Another process completed {stage_file}"
                )
            raise IngestionInProgressError(
                f"Could not claim failed ingestion for {stage_file}; "
                "another process may own it"
            )
    finally:
        cursor.close()


def _verify_loaded_rows(
    connection,
    source: str,
    staged_filename: str,
    expected_rows: int,
) -> None:
    actual_rows = _bronze_file_row_count(
        connection, source, staged_filename
    )
    if actual_rows != expected_rows:
        raise IngestionError(
            f"Ledger says {staged_filename} is loaded, but Bronze contains "
            f"{actual_rows} rows; expected {expected_rows}. Manual recovery "
            "is required to avoid duplicating data."
        )


def _mark_ingestion_loaded(
    connection,
    file_id: str,
    loaded_rows: int,
    copy_query_id: str | None,
) -> None:
    rowcount = _execute(
        connection,
        f"""
        UPDATE {INGESTION_LEDGER}
        SET STATUS = 'LOADED',
            LOADED_ROWS = %s,
            COPY_QUERY_ID = %s,
            LAST_ERROR = NULL,
            UPDATED_AT = CURRENT_TIMESTAMP()
        WHERE FILE_ID = %s
          AND STATUS = 'LOADING'
        """,
        (loaded_rows, copy_query_id, file_id),
    )
    if rowcount != 1:
        raise IngestionError(
            f"Could not mark ingestion {file_id} as loaded; "
            "inspect the ingestion ledger"
        )


def _recover_failed_ingestion(
    connection, file_id: str, loaded_rows: int
) -> None:
    rowcount = _execute(
        connection,
        f"""
        UPDATE {INGESTION_LEDGER}
        SET STATUS = 'LOADED',
            LOADED_ROWS = %s,
            LAST_ERROR = NULL,
            UPDATED_AT = CURRENT_TIMESTAMP()
        WHERE FILE_ID = %s
          AND STATUS = 'FAILED'
        """,
        (loaded_rows, file_id),
    )
    if rowcount != 1:
        raise IngestionError(
            f"Could not reconcile failed ingestion {file_id}; "
            "inspect the ingestion ledger and Bronze rows"
        )


def _mark_ingestion_failed(
    connection,
    file_id: str,
    error: Exception,
    copy_query_id: str | None = None,
) -> None:
    try:
        rowcount = _execute(
            connection,
            f"""
            UPDATE {INGESTION_LEDGER}
            SET STATUS = 'FAILED',
                COPY_QUERY_ID = COALESCE(%s, COPY_QUERY_ID),
                LAST_ERROR = %s,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE FILE_ID = %s
              AND STATUS = 'LOADING'
            """,
            (
                copy_query_id,
                f"{type(error).__name__}: {str(error)[:1500]}",
                file_id,
            ),
        )
        if rowcount != 1:
            raise IngestionError(
                f"Could not mark ingestion {file_id} as failed"
            )
    except Exception as ledger_error:
        raise IngestionError(
            "File ingestion failed and the ledger could not be updated; "
            "inspect the ledger and Bronze file rows before retrying"
        ) from ledger_error


if __name__ == "__main__":
    test_connection()