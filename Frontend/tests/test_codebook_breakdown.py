"""Tests for the per-theme demographic breakdown wiring in the codebook
controller: the themes page (dimension picker / disabled state) and the
JSON proxy route used by theme_breakdown.js.
"""
from __future__ import annotations

import json

THEMES_PATH = "/codebooks/test-corpus-id/cb-1/themes"
BREAKDOWN_JSON_PATH = (
    "/codebooks/test-corpus-id/cb-1/themes/theme-1/demographic-breakdown.json"
)


def _seed_themes_page(fake_backend):
    fake_backend.codebooks = [
        {"id": "cb-1", "name": "My Codebook", "version": 1, "corpus_id": "test-corpus-id"}
    ]
    fake_backend.theme_frequencies = [
        {
            "theme_id": "theme-1",
            "theme_name": "Theme One",
            "occurrence_count": 3,
            "interview_coverage_percentage": 60.0,
        }
    ]
    fake_backend.theme_tree = []


# ---------------------------------------------------------------------------
# Themes page: dimension picker vs disabled state
# ---------------------------------------------------------------------------


def test_themes_page_shows_dimension_picker_when_data_present(client, fake_backend):
    _seed_themes_page(fake_backend)
    fake_backend.demographic_dimensions = [
        {"name": "gender", "is_numeric": False},
        {"name": "age_group", "is_numeric": True},
    ]

    resp = client.get(THEMES_PATH)
    assert resp.status_code == 200
    assert b"Demographic Breakdown" in resp.data
    assert b"data-breakdown-dimension" in resp.data
    assert b"gender" in resp.data
    assert b"age_group" in resp.data
    assert b"No demographic data available" not in resp.data
    # The dimensions are also embedded for the JS to read.
    assert b"data-demographic-dimensions" in resp.data
    # Only the numeric dimension gets a bin-count input.
    assert b'data-breakdown-bin-count="age_group"' in resp.data
    assert b'data-breakdown-bin-count="gender"' not in resp.data


def test_themes_page_shows_disabled_state_when_no_data(client, fake_backend):
    _seed_themes_page(fake_backend)
    fake_backend.demographic_dimensions = []

    resp = client.get(THEMES_PATH)
    assert resp.status_code == 200
    assert b"Demographic Breakdown" in resp.data
    assert b"No demographic data available" in resp.data
    assert b"data-breakdown-dimension" not in resp.data


def test_themes_page_survives_dimensions_backend_error(client, fake_backend):
    # A failure fetching dimensions must not break the themes page; it degrades
    # to the disabled state.
    _seed_themes_page(fake_backend)
    fake_backend.raise_on = "get_demographic_dimensions"

    resp = client.get(THEMES_PATH)
    assert resp.status_code == 200
    assert b"No demographic data available" in resp.data


# ---------------------------------------------------------------------------
# JSON proxy route
# ---------------------------------------------------------------------------


def test_breakdown_json_returns_payload(client, fake_backend):
    fake_backend.theme_demographic_breakdown = {
        "theme_id": "theme-1",
        "application_run_id": "run-1",
        "dimensions": [
            {
                "dimension": "gender",
                "groups": [
                    {
                        "group_value": "male",
                        "present_count": 1,
                        "group_total": 2,
                        "percentage": 50.0,
                        "small_sample": True,
                    }
                ],
            }
        ],
    }

    resp = client.get(BREAKDOWN_JSON_PATH + "?dimensions=gender&application_run_id=run-1")
    assert resp.status_code == 200
    payload = json.loads(resp.data)
    assert payload["dimensions"][0]["dimension"] == "gender"
    assert payload["dimensions"][0]["groups"][0]["percentage"] == 50.0
    # The controller parses and forwards the comma-separated dimensions.
    assert fake_backend.last_breakdown_request["dimensions"] == ["gender"]
    assert fake_backend.last_breakdown_request["application_run_id"] == "run-1"


def test_breakdown_json_parses_multiple_dimensions(client, fake_backend):
    fake_backend.theme_demographic_breakdown = {"theme_id": "theme-1", "dimensions": []}
    resp = client.get(BREAKDOWN_JSON_PATH + "?dimensions=gender,age_group,party")
    assert resp.status_code == 200
    assert fake_backend.last_breakdown_request["dimensions"] == ["gender", "age_group", "party"]


def test_breakdown_json_parses_bins_param(client, fake_backend):
    fake_backend.theme_demographic_breakdown = {"theme_id": "theme-1", "dimensions": []}
    resp = client.get(BREAKDOWN_JSON_PATH + "?dimensions=age_group&bins=age_group:5")
    assert resp.status_code == 200
    assert fake_backend.last_breakdown_request["bins"] == {"age_group": 5}


def test_breakdown_json_ignores_malformed_bins_entries(client, fake_backend):
    fake_backend.theme_demographic_breakdown = {"theme_id": "theme-1", "dimensions": []}
    resp = client.get(BREAKDOWN_JSON_PATH + "?dimensions=age_group&bins=age_group:five,,gender:")
    assert resp.status_code == 200
    assert fake_backend.last_breakdown_request["bins"] is None


def test_breakdown_json_handles_empty_dimensions(client, fake_backend):
    fake_backend.theme_demographic_breakdown = {"theme_id": "theme-1", "dimensions": []}
    resp = client.get(BREAKDOWN_JSON_PATH)
    assert resp.status_code == 200
    assert fake_backend.last_breakdown_request["dimensions"] == []


def test_breakdown_json_surfaces_backend_error(client, fake_backend):
    fake_backend.raise_on = "get_theme_demographic_breakdown"
    resp = client.get(BREAKDOWN_JSON_PATH + "?dimensions=gender")
    assert resp.status_code == 502
    assert "error" in json.loads(resp.data)
