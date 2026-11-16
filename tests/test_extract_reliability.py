from datetime import date
import json
from pathlib import Path

import pytest
import requests

from elt import extract
from elt.load_bronze import source_files


class FakeResponse:
    def __init__(self, body=None, error=None, status_error=None):
        self.body = body
        self.error = error
        self.status_error = status_error

    def raise_for_status(self):
        if self.status_error:
            raise self.status_error

    def json(self):
        if self.error:
            raise self.error
        return self.body


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if not self.responses:
            raise AssertionError(f"Unexpected request to {url}")
        return self.responses.pop(0)


def test_same_origin_absolute_pagination_preserves_request_options():
    session = FakeSession([
        FakeResponse({
            "data": [{"id": 1}],
            "next": "https://api.example.test/v1/page2",
        }),
        FakeResponse({"data": [{"id": 2}], "next": None}),
    ])
    headers = {"Authorization": "Bearer test-token"}

    records = list(extract.paginate(
        session,
        "https://api.example.test/v1/orders",
        headers=headers,
        timeout=13,
    ))

    assert records == [{"id": 1}, {"id": 2}]
    assert [call[0] for call in session.calls] == [
        "https://api.example.test/v1/orders",
        "https://api.example.test/v1/page2",
    ]
    assert all(call[1]["headers"] == headers for call in session.calls)
    assert all(call[1]["timeout"] == 13 for call in session.calls)


def test_relative_pagination_url_is_resolved_against_current_page():
    session = FakeSession([
        FakeResponse({"data": [], "next": "../page2?cursor=abc"}),
        FakeResponse({"data": [{"id": 2}]}),
    ])

    assert list(extract.paginate(
        session,
        "https://api.example.test/v1/orders",
    )) == [{"id": 2}]
    assert session.calls[1][0] == (
        "https://api.example.test/page2?cursor=abc"
    )


def test_cross_origin_pagination_is_rejected_before_credentials_are_forwarded():
    session = FakeSession([
        FakeResponse({
            "data": [],
            "next": "https://attacker.example.test/steal",
        }),
    ])

    with pytest.raises(extract.PaginationError, match="Cross-origin"):
        list(extract.paginate(
            session,
            "https://api.example.test/orders",
            headers={"Authorization": "Bearer test-token"},
        ))

    assert len(session.calls) == 1
    assert session.calls[0][0] == "https://api.example.test/orders"
    assert not any(
        "attacker.example.test" in call[0]
        for call in session.calls
    )


def test_malformed_pagination_url_fails_without_followup_request():
    session = FakeSession([
        FakeResponse({"data": [], "next": "https://[malformed"}),
    ])

    with pytest.raises(extract.PaginationError, match="malformed"):
        list(extract.paginate(session, "https://api.example.test/orders"))

    assert len(session.calls) == 1


def test_pagination_requires_https_and_rejects_port_changes():
    initial_session = FakeSession([])
    with pytest.raises(extract.PaginationError, match="HTTPS"):
        list(extract.paginate(initial_session, "http://api.example.test/"))
    assert initial_session.calls == []

    session = FakeSession([
        FakeResponse({
            "data": [],
            "next": "https://api.example.test:8443/page2",
        }),
    ])
    with pytest.raises(extract.PaginationError, match="Cross-origin"):
        list(extract.paginate(session, "https://api.example.test/orders"))
    assert len(session.calls) == 1

    zero_port_session = FakeSession([
        FakeResponse({
            "data": [],
            "next": "https://api.example.test:0/page2",
        }),
    ])
    with pytest.raises(extract.PaginationError, match="Cross-origin"):
        list(extract.paginate(
            zero_port_session,
            "https://api.example.test/orders",
        ))
    assert len(zero_port_session.calls) == 1


def test_invalid_pagination_metadata_does_not_end_pagination_silently():
    session = FakeSession([
        FakeResponse({"data": [], "next": []}),
    ])

    with pytest.raises(extract.PaginationError, match="must be a string"):
        list(extract.paginate(session, "https://api.example.test/orders"))

    assert len(session.calls) == 1


def test_pagination_distinguishes_empty_missing_invalid_json_and_api_failure():
    empty = FakeSession([FakeResponse({"data": []})])
    assert list(extract.paginate(empty, "https://api.example.test/")) == []

    missing_key = FakeSession([FakeResponse({"items": []})])
    with pytest.raises(
        extract.InvalidAPIResponseError,
        match="missing required collection key",
    ):
        list(extract.paginate(missing_key, "https://api.example.test/"))

    wrong_type = FakeSession([FakeResponse([{"id": 1}])])
    with pytest.raises(
        extract.InvalidAPIResponseError,
        match="JSON object",
    ):
        list(extract.paginate(wrong_type, "https://api.example.test/"))

    invalid_json = FakeSession([
        FakeResponse(error=requests.exceptions.JSONDecodeError(
            "bad json", "{", 0
        ))
    ])
    with pytest.raises(extract.InvalidAPIJSONError):
        list(extract.paginate(invalid_json, "https://api.example.test/"))

    api_failure = FakeSession([
        FakeResponse(status_error=requests.HTTPError("server error"))
    ])
    with pytest.raises(extract.APIRequestError):
        list(extract.paginate(api_failure, "https://api.example.test/"))


def test_invalid_collection_members_and_fx_response_fail_clearly():
    session = FakeSession([FakeResponse({"data": [None]})])
    with pytest.raises(
        extract.InvalidAPIResponseError,
        match="JSON objects",
    ):
        list(extract.paginate(session, "https://api.example.test/"))

    with pytest.raises(
        extract.InvalidAPIResponseError,
        match="rates object",
    ):
        extract._validate_fx_responses([{"base": "USD", "rates": []}])


def test_landing_directory_default_relative_and_absolute_paths(monkeypatch, tmp_path):
    project_root = tmp_path / "project"
    monkeypatch.setattr(extract, "PROJECT_ROOT", project_root)

    monkeypatch.delenv("LANDING_DIR", raising=False)
    default_dir = project_root / "elt" / "landing"
    assert extract.get_landing_dir() == default_dir.resolve()
    assert source_files("web") == []

    monkeypatch.setenv("LANDING_DIR", "custom-landing")
    relative_dir = (project_root / "custom-landing").resolve()
    assert extract.get_landing_dir() == relative_dir

    (relative_dir / "web" / "2026-09-30").mkdir(parents=True)
    (relative_dir / "web" / "2026-09-30" / "web__test.json.gz").touch()
    assert source_files("web") == [
        relative_dir / "web" / "2026-09-30" / "web__test.json.gz"
    ]

    absolute_dir = tmp_path / "absolute-landing"
    monkeypatch.setenv("LANDING_DIR", str(absolute_dir))
    assert extract.get_landing_dir() == absolute_dir.resolve()


def test_mock_sources_and_pincode_responses_obey_expected_shapes():
    assert extract.web_orders(None, date(2026, 9, 30), mock=True)
    assert extract.mobile_orders(None, date(2026, 9, 30), mock=True)
    assert extract.crm_tier_events(None, date(2026, 9, 30), mock=True)
    assert extract.fx_rates(
        None,
        date(2026, 9, 1),
        date(2026, 9, 3),
        mock=True,
    )
    assert extract.pincodes(None, ["000000"], mock=True)[0]["response"][0][
        "Status"
    ] == "Error"

    with pytest.raises(extract.InvalidAPIResponseError):
        extract._validate_pincode_record({
            "pincode": "600041",
            "response": [{"Status": "Success", "PostOffice": None}],
        })