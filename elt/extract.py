import gzip
import json
import logging
import os
import time

from datetime import datetime, timedelta, timezone
from math import isfinite
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import mock_data


log = logging.getLogger("extract")
PROJECT_ROOT = Path(__file__).resolve().parents[1]


class ExtractionError(RuntimeError):
    """Base class for API extraction failures."""


class APIRequestError(ExtractionError):
    """An API request failed or returned an unsuccessful status."""


class InvalidAPIJSONError(ExtractionError):
    """The API response body could not be decoded as JSON."""


class InvalidAPIResponseError(ExtractionError):
    """The API returned JSON that did not match its contract."""


class PaginationError(ExtractionError):
    """Pagination metadata or a pagination URL was invalid."""


def get_landing_dir() -> Path:
    """Resolve LANDING_DIR consistently relative to the project root."""
    landing_dir = Path(os.getenv("LANDING_DIR", "elt/landing")).expanduser()
    if not landing_dir.is_absolute():
        landing_dir = PROJECT_ROOT / landing_dir
    return landing_dir.resolve()


def _validate_records(records, source: str) -> list[dict]:
    if not isinstance(records, list):
        raise InvalidAPIResponseError(
            f"{source} response collection must be a JSON array"
        )
    if any(not isinstance(record, dict) for record in records):
        raise InvalidAPIResponseError(
            f"{source} response collection must contain JSON objects"
        )
    return records


def _validate_secure_url(candidate: str, origin: tuple[str, str, int] | None = None):
    if not isinstance(candidate, str) or not candidate.strip():
        raise PaginationError("Pagination URL must be a non-empty string")
    try:
        parsed = urlsplit(candidate)
        if parsed.scheme.lower() != "https":
            raise PaginationError("Pagination URLs must use HTTPS")
        if not parsed.hostname or parsed.username or parsed.password:
            raise PaginationError("Pagination URL must have a valid host")
        port = parsed.port if parsed.port is not None else 443
    except ValueError as error:
        raise PaginationError("Pagination URL is malformed") from error

    url_origin = (parsed.scheme.lower(), parsed.hostname.lower(), port)
    if origin is not None and url_origin != origin:
        raise PaginationError(
            "Cross-origin pagination URL rejected; pagination must remain "
            "on the original HTTPS scheme, host, and port"
        )
    return parsed, url_origin


def http_session():
    """Create an HTTP session with retry support."""

    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True
    )

    session = requests.Session()

    session.mount(
        "https://",
        HTTPAdapter(max_retries=retry)
    )

    session.mount(
        "http://",
        HTTPAdapter(max_retries=retry)
    )

    session.headers["User-Agent"] = "freshcart-elt/1.0"

    return session


def _auth(token_env):
    token = os.getenv(token_env)

    return (
        {"Authorization": f"Bearer {token}"}
        if token else {}
    )


def paginate(
    session,
    url,
    params=None,
    headers=None,
    items_key="data",
    next_key="next",
    timeout=30,
    max_pages=10000
):
    """Retrieve validated, same-origin HTTPS paginated API responses."""

    if not isinstance(url, str):
        raise PaginationError("Initial API URL must be a string")
    try:
        page_url = urljoin(url, url)
    except (TypeError, ValueError) as error:
        raise PaginationError("Initial API URL is malformed") from error
    _, origin = _validate_secure_url(page_url)
    page_params = dict(params or {})

    for _ in range(max_pages):
        _validate_secure_url(page_url, origin)

        try:
            response = session.get(
                page_url,
                params=page_params,
                headers=headers,
                timeout=timeout
            )
            response.raise_for_status()
        except requests.RequestException as error:
            raise APIRequestError(
                f"API request failed ({type(error).__name__})"
            ) from error

        try:
            body = response.json()
        except (ValueError, json.JSONDecodeError) as error:
            raise InvalidAPIJSONError(
                "API response body is not valid JSON"
            ) from error

        if not isinstance(body, dict):
            raise InvalidAPIResponseError(
                "Paginated API response must be a JSON object"
            )
        if items_key not in body:
            raise InvalidAPIResponseError(
                f"API response is missing required collection key "
                f"{items_key!r}"
            )
        items = _validate_records(body[items_key], items_key)

        yield from items

        next_page = body.get(next_key)
        if next_page is None or next_page == "":
            return

        if not isinstance(next_page, str):
            raise PaginationError("Pagination URL must be a string")
        try:
            page_url = urljoin(page_url, next_page)
        except (TypeError, ValueError) as error:
            raise PaginationError("Pagination URL is malformed") from error
        _validate_secure_url(page_url, origin)
        page_params = None

    raise PaginationError(
        f"{url}: more than {max_pages} pages"
    )


def write_landing(source, records, run_id, run_date):
    """Write records as compressed newline-delimited JSON."""

    records = list(records)

    if not records:
        log.info("%s: 0 records, nothing written", source)
        return None

    folder = get_landing_dir() / source / run_date.isoformat()

    folder.mkdir(parents=True, exist_ok=True)

    path = folder / f"{source}__{run_id}.json.gz"

    now = datetime.now(timezone.utc).isoformat()

    with gzip.open(path, "wt", encoding="utf-8") as file:

        for record in records:

            envelope = {
                "source": source,
                "run_id": run_id,
                "extracted_at": now,
                "data": record
            }

            file.write(
                json.dumps(
                    envelope,
                    ensure_ascii=False
                ) + "\n"
            )

    log.info(
        "%s: %d records -> %s",
        source,
        len(records),
        path
    )

    return path


def web_orders(session, run_date, mock=False):

    if mock:
        return _validate_records(mock_data.WEB_ORDERS, "web")

    since = (
        run_date - timedelta(days=1)
    ).isoformat()

    return paginate(
        session,
        os.environ["WEB_ORDERS_URL"],
        params={"updated_since": since},
        headers=_auth("WEB_ORDERS_TOKEN")
    )


def mobile_orders(session, run_date, mock=False):

    if mock:
        return _validate_records(mock_data.MOBILE_ORDERS, "mobile")

    since = (
        run_date - timedelta(days=1)
    ).isoformat()

    return paginate(
        session,
        os.environ["MOBILE_ORDERS_URL"],
        params={"updatedSince": since},
        headers=_auth("MOBILE_ORDERS_TOKEN"),
        items_key="orders",
        next_key="nextPage"
    )


def crm_tier_events(session, run_date, mock=False):

    if mock:
        return _validate_records(mock_data.CRM_TIER_EVENTS, "crm")

    return paginate(
        session,
        os.environ["CRM_TIER_EVENTS_URL"],
        headers=_auth("CRM_TOKEN")
    )


def fx_rates(session, start, end, mock=False):
    """Retrieve daily USD-to-INR exchange rates."""

    if mock:
        return _validate_fx_responses(mock_data.FX_RESPONSES)

    base_url = os.getenv(
        "FX_URL",
        "https://api.frankfurter.app"
    )

    output = []

    currencies = [
        currency.strip()
        for currency in os.getenv(
            "FX_BASES", "USD"
        ).split(",")
        if currency.strip()
    ]
    if not currencies:
        raise InvalidAPIResponseError(
            "FX_BASES must contain at least one currency"
        )

    for base in currencies:

        try:
            response = session.get(
                f"{base_url}/{start.isoformat()}..{end.isoformat()}",
                params={
                    "from": base,
                    "to": "INR"
                },
                timeout=30
            )
            response.raise_for_status()
        except requests.RequestException as error:
            raise APIRequestError(
                f"FX API request failed ({type(error).__name__})"
            ) from error

        try:
            result = response.json()
        except (ValueError, json.JSONDecodeError) as error:
            raise InvalidAPIJSONError(
                "FX API response body is not valid JSON"
            ) from error
        output.append(result)

    return _validate_fx_responses(output, expected_bases=currencies)


def _validate_fx_responses(
    responses, expected_bases: list[str] | None = None
) -> list[dict]:
    if not isinstance(responses, list) or not responses:
        raise InvalidAPIResponseError(
            "FX response must contain at least one response object"
        )
    seen_bases = set()
    for response in responses:
        if not isinstance(response, dict):
            raise InvalidAPIResponseError(
                "Each FX response must be a JSON object"
            )
        base = response.get("base")
        rates = response.get("rates")
        if not isinstance(base, str) or not base.strip():
            raise InvalidAPIResponseError(
                "FX response is missing a valid base currency"
            )
        base = base.strip().upper()
        if base in seen_bases:
            raise InvalidAPIResponseError(
                f"FX response contains duplicate base currency {base}"
            )
        seen_bases.add(base)
        if not isinstance(rates, dict):
            raise InvalidAPIResponseError(
                "FX response is missing a rates object"
            )
        for rate_date, currencies in rates.items():
            try:
                datetime.strptime(rate_date, "%Y-%m-%d")
            except (TypeError, ValueError) as error:
                raise InvalidAPIResponseError(
                    f"FX response contains an invalid date key {rate_date!r}"
                ) from error
            if not isinstance(currencies, dict):
                raise InvalidAPIResponseError(
                    f"FX rates for {rate_date} must be a JSON object"
                )
            if "INR" not in currencies:
                raise InvalidAPIResponseError(
                    f"FX response for {rate_date} is missing the INR rate"
                )
            try:
                rate = float(currencies["INR"])
            except (TypeError, ValueError) as error:
                raise InvalidAPIResponseError(
                    f"FX rate for {rate_date} is not numeric"
                ) from error
            if not isfinite(rate) or rate <= 0:
                raise InvalidAPIResponseError(
                    f"FX rate for {rate_date} must be finite and positive"
                )
    if expected_bases is not None and seen_bases != {
        base.strip().upper() for base in expected_bases
    }:
        raise InvalidAPIResponseError(
            "FX API responses did not match the requested base currencies"
        )
    return responses


def pincodes(session, pins, mock=False):
    """Retrieve pincode details from India Post."""

    base_url = os.getenv(
        "PINCODE_URL",
        "https://api.postalpincode.in/pincode"
    )

    output = []

    for pin in pins:

        if mock:
            response = mock_data.pincode_response(pin)

        else:
            try:
                result = session.get(
                    f"{base_url}/{pin}",
                    timeout=30
                )
                result.raise_for_status()
            except requests.RequestException as error:
                raise APIRequestError(
                    f"Pincode API request failed ({type(error).__name__})"
                ) from error

            try:
                response = result.json()
            except (ValueError, json.JSONDecodeError) as error:
                raise InvalidAPIJSONError(
                    "Pincode API response body is not valid JSON"
                ) from error

            time.sleep(0.2)

        record = {
            "pincode": pin,
            "response": response
        }
        _validate_pincode_record(record)
        output.append(record)

    return output


def _validate_pincode_record(record: dict) -> None:
    response = record.get("response")
    if not isinstance(response, list) or not response:
        raise InvalidAPIResponseError(
            f"Pincode response for {record.get('pincode')} must be a "
            "non-empty JSON array"
        )
    for result in response:
        if not isinstance(result, dict):
            raise InvalidAPIResponseError(
                "Each India Post response entry must be a JSON object"
            )
        status = result.get("Status")
        if status not in {"Success", "Error"}:
            raise InvalidAPIResponseError(
                "India Post response has an unsupported Status value"
            )
        offices = result.get("PostOffice")
        if status == "Success":
            if not isinstance(offices, list) or not offices:
                raise InvalidAPIResponseError(
                    "Successful India Post response must contain "
                    "PostOffice records"
                )
            if any(not isinstance(office, dict) for office in offices):
                raise InvalidAPIResponseError(
                    "India Post PostOffice records must be JSON objects"
                )
        elif offices is not None and not isinstance(offices, list):
            raise InvalidAPIResponseError(
                "Error response PostOffice must be null or a JSON array"
            )