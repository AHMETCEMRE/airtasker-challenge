"""Tests for the Open-Meteo client.

These cover the ingestion behaviour that cannot be asserted in SQL: the request
parameters that decide whether the data is correct at all, the refusal to send
implausible coordinates, the error handling, and the flattening of Open-Meteo's
columnar response into rows.

weather_risk/open_meteo.py deliberately has no Dagster or DuckDB imports, which
is what lets all of this run without the stack up.
"""

from datetime import datetime
from unittest.mock import MagicMock

import pytest
import requests

from weather_risk.open_meteo import (
    FORECAST_DAYS,
    HOURLY_VARIABLES,
    REQUEST_TIMEZONE,
    RETRY_STATUS_FORCELIST,
    ForecastUnavailable,
    Location,
    build_session,
    fetch_forecast,
    forecast_rows,
)

FETCHED_AT = datetime(2026, 11, 17, 4, 49, 0)

SYDNEY = Location(
    location_id="LOC_AU_SYD",
    latitude=-33.8688,
    longitude=151.2093,
    timezone_name="Australia/Sydney",
)

# The actual row from dbt_project/seeds/raw_locations.csv: latitude and
# longitude are transposed, giving a latitude of 115 degrees.
PERTH_TRANSPOSED = Location(
    location_id="LOC_AU_PER",
    latitude=115.8605,
    longitude=-31.9505,
    timezone_name="Australia/Perth",
)


def _payload(hourly=None, latitude=-33.85, longitude=151.2):
    """A minimal Open-Meteo response shaped the way the real API returns one."""
    if hourly is None:
        hourly = {
            "time": ["2026-11-17T00:00", "2026-11-17T01:00", "2026-11-17T02:00"],
            "temperature_2m": [19.3, 19.5, 20.1],
            "precipitation": [0.1, 0.1, 0.0],
            "precipitation_probability": [88.0, 94.0, 91.0],
            "wind_speed_10m": [8.7, 9.3, 7.5],
            "wind_gusts_10m": [25.6, 27.7, 27.0],
            "weather_code": [51, 51, 3],
        }
    return {"latitude": latitude, "longitude": longitude, "hourly": hourly}


def _response(status_code=200, payload=None, text="", json_raises=False):
    response = MagicMock()
    response.status_code = status_code
    response.text = text
    if json_raises:
        response.json.side_effect = ValueError("not json")
    else:
        response.json.return_value = payload if payload is not None else {}
    return response


# ---------------------------------------------------------------------------
# Coordinate validation
# ---------------------------------------------------------------------------


def test_plausible_coordinates_are_accepted():
    assert SYDNEY.has_valid_coordinates is True


def test_perth_transposed_coordinates_are_rejected():
    """The seed's Perth row has latitude 115.86, which does not exist.

    This is the check that keeps a request that can only fail from reaching the
    API, and that routes Perth's tasks to 'unknown' instead of giving them a
    forecast for a point in the Indian Ocean.
    """
    assert PERTH_TRANSPOSED.has_valid_coordinates is False


@pytest.mark.parametrize(
    "latitude,longitude",
    [
        (None, 151.2093),
        (-33.8688, None),
        (None, None),
        (91.0, 0.0),
        (-91.0, 0.0),
        (0.0, 181.0),
        (0.0, -181.0),
    ],
)
def test_impossible_or_missing_coordinates_are_rejected(latitude, longitude):
    location = Location("LOC_X", latitude, longitude, "UTC")
    assert location.has_valid_coordinates is False


# ---------------------------------------------------------------------------
# Request parameters - these decide whether the data is correct, not just
# which fields come back
# ---------------------------------------------------------------------------


def test_request_pins_horizon_timezone_and_units():
    """The three parameters that affect correctness.

    forecast_days must cover the 14-day scheduling horizon; the API default of
    7 would leave roughly half the eligible tasks with no forecast. The
    timezone must be UTC so local conversion happens per timestamp in SQL. The
    units must be explicit because the risk thresholds are written in celsius,
    mm and km/h.
    """
    session = MagicMock()
    session.get.return_value = _response(payload=_payload())

    fetch_forecast(session, SYDNEY)

    params = session.get.call_args.kwargs["params"]
    assert params["forecast_days"] == FORECAST_DAYS == 16
    assert params["timezone"] == REQUEST_TIMEZONE == "UTC"
    assert params["temperature_unit"] == "celsius"
    assert params["precipitation_unit"] == "mm"
    assert params["wind_speed_unit"] == "kmh"
    assert params["latitude"] == SYDNEY.latitude
    assert params["longitude"] == SYDNEY.longitude


def test_request_asks_for_hourly_not_daily_variables():
    """A task is scheduled at an hour, and a daily precipitation_sum cannot say
    whether the rain falls at 3am or at 9am."""
    session = MagicMock()
    session.get.return_value = _response(payload=_payload())

    fetch_forecast(session, SYDNEY)

    params = session.get.call_args.kwargs["params"]
    assert "daily" not in params
    assert params["hourly"] == ",".join(HOURLY_VARIABLES)
    assert "wind_gusts_10m" in params["hourly"]


def test_a_timeout_is_always_set():
    """Without a timeout a single hung connection stalls the whole run."""
    session = MagicMock()
    session.get.return_value = _response(payload=_payload())

    fetch_forecast(session, SYDNEY)

    connect_timeout, read_timeout = session.get.call_args.kwargs["timeout"]
    assert connect_timeout > 0
    assert read_timeout > 0


# ---------------------------------------------------------------------------
# Retry configuration and error handling
# ---------------------------------------------------------------------------


def test_session_retries_transient_failures_with_backoff():
    """429 is Open-Meteo's rate limit and the 5xx family is transient, so both
    are retried. Retry-After is honoured because the API sends it."""
    adapter = build_session().get_adapter("https://api.open-meteo.com/v1/forecast")
    retry = adapter.max_retries

    assert retry.total >= 3
    assert retry.backoff_factor > 0
    assert retry.respect_retry_after_header is True
    for status in (429, 500, 502, 503, 504):
        assert status in retry.status_forcelist


def test_a_client_error_is_not_retried():
    """400 means our request is malformed. Retrying it just wastes the run."""
    adapter = build_session().get_adapter("https://api.open-meteo.com/v1/forecast")
    assert 400 not in adapter.max_retries.status_forcelist
    assert 400 not in RETRY_STATUS_FORCELIST


def test_invalid_coordinates_never_reach_the_api():
    session = MagicMock()

    with pytest.raises(ForecastUnavailable, match="implausible coordinates"):
        fetch_forecast(session, PERTH_TRANSPOSED)

    session.get.assert_not_called()


def test_network_failure_after_retries_raises_forecast_unavailable():
    """The session has already exhausted its retries by this point, so the
    caller's job is to turn the failure into something it can skip over rather
    than letting it kill the run."""
    session = MagicMock()
    session.get.side_effect = requests.ConnectTimeout("connect timed out")

    with pytest.raises(ForecastUnavailable) as exc:
        fetch_forecast(session, SYDNEY)

    assert "LOC_AU_SYD" in str(exc.value)


def test_http_error_includes_the_status_and_body():
    """Open-Meteo puts a human-readable reason in the body on 400, and losing it
    turns a five-second diagnosis into an afternoon."""
    session = MagicMock()
    session.get.return_value = _response(
        status_code=400, text='{"reason":"Latitude must be in range of -90 to 90"}'
    )

    with pytest.raises(ForecastUnavailable) as exc:
        fetch_forecast(session, SYDNEY)

    assert "400" in str(exc.value)
    assert "Latitude must be in range" in str(exc.value)


def test_non_json_response_raises_forecast_unavailable():
    session = MagicMock()
    session.get.return_value = _response(json_raises=True)

    with pytest.raises(ForecastUnavailable, match="not JSON"):
        fetch_forecast(session, SYDNEY)


@pytest.mark.parametrize(
    "payload",
    [
        {"latitude": 1.0, "longitude": 2.0},
        {"latitude": 1.0, "longitude": 2.0, "hourly": {}},
        {"latitude": 1.0, "longitude": 2.0, "hourly": {"time": []}},
    ],
)
def test_a_response_with_no_hourly_series_raises(payload):
    """A 200 with an empty body is still a failure. Landing zero rows silently
    would look like success and leave the location unforecast."""
    session = MagicMock()
    session.get.return_value = _response(payload=payload)

    with pytest.raises(ForecastUnavailable):
        fetch_forecast(session, SYDNEY)


# ---------------------------------------------------------------------------
# Flattening the columnar response
# ---------------------------------------------------------------------------


def test_forecast_rows_flattens_parallel_arrays_into_rows():
    rows = forecast_rows(SYDNEY, _payload(), FETCHED_AT, "run-abc")

    assert len(rows) == 3

    first = rows[0]
    assert first[0] == "LOC_AU_SYD"
    assert first[3] == "UTC"                              # timezone_requested
    assert first[4] == datetime(2026, 11, 17, 0, 0)       # forecast_time_utc
    assert first[5] == 19.3                               # temperature_2m_c
    assert first[6] == 0.1                                # precipitation_mm
    assert first[7] == 88.0                               # precipitation_probability
    assert first[8] == 8.7                                # wind_speed_kmh
    assert first[9] == 25.6                               # wind_gusts_kmh
    assert first[10] == 51                                # weather_code
    assert first[11] == FETCHED_AT
    assert first[12] == "run-abc"


def test_forecast_rows_keeps_the_grid_cell_the_api_actually_served():
    """Open-Meteo snaps to its nearest model grid cell rather than using the
    coordinate requested. Storing what it served is what explains why two
    nearby cities can share a forecast."""
    rows = forecast_rows(
        SYDNEY, _payload(latitude=-33.848858, longitude=151.19551), FETCHED_AT
    )

    assert rows[0][1] == -33.848858
    assert rows[0][2] == 151.19551
    assert rows[0][1] != SYDNEY.latitude


def test_a_short_variable_series_is_padded_rather_than_misaligning_rows():
    """If one variable's array is shorter than `time`, zipping them naively
    shifts every later value onto the wrong timestamp. A few nulls are vastly
    preferable to silently wrong weather."""
    hourly = {
        "time": ["2026-11-17T00:00", "2026-11-17T01:00", "2026-11-17T02:00"],
        "temperature_2m": [19.3],
        "precipitation": [0.1, 0.1, 0.0],
        "precipitation_probability": [88.0, 94.0, 91.0],
        "wind_speed_10m": [8.7, 9.3, 7.5],
        "wind_gusts_10m": [25.6, 27.7, 27.0],
        "weather_code": [51, 51, 3],
    }
    rows = forecast_rows(SYDNEY, _payload(hourly=hourly), FETCHED_AT)

    assert len(rows) == 3
    assert rows[0][5] == 19.3
    assert rows[1][5] is None
    assert rows[2][5] is None
    # The variables that were complete must not have shifted.
    assert rows[1][6] == 0.1
    assert rows[2][7] == 91.0


def test_a_missing_variable_becomes_nulls_not_an_error():
    hourly = {
        "time": ["2026-11-17T00:00", "2026-11-17T01:00"],
        "temperature_2m": [19.3, 19.5],
    }
    rows = forecast_rows(SYDNEY, _payload(hourly=hourly), FETCHED_AT)

    assert len(rows) == 2
    assert all(row[9] is None for row in rows)  # wind_gusts_kmh


def test_every_row_carries_the_same_fetch_stamp():
    """fetched_at_utc is part of the natural key of the append-only table, and
    is what stg_weather_forecast orders by to find the current forecast. Rows
    from one fetch must share one value."""
    rows = forecast_rows(SYDNEY, _payload(), FETCHED_AT, "run-abc")

    assert {row[11] for row in rows} == {FETCHED_AT}
    assert {row[12] for row in rows} == {"run-abc"}
