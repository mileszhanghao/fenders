import json
from datetime import date
from urllib.parse import parse_qs, urlparse

import pyarrow.parquet as pq
import pytest
import responses

from fenders import extract
from fenders.sources import Counter

C = Counter("test", "abcd-1234", "date", "Test Counter")
META = f"https://data.seattle.gov/api/views/{C.dataset}.json"
ROWS = f"https://data.seattle.gov/resource/{C.dataset}.json"


def row(ts, total):
    return {"date": ts, "total": str(total), ":id": "row-x"}


def rows_callback(data):
    """Serve `data` like Socrata: honour $where (>= on the time field), $offset and $limit."""

    def cb(request):
        q = {k: v[0] for k, v in parse_qs(urlparse(request.url).query).items()}
        out = data
        if "$where" in q:
            since = q["$where"].split("'")[1]
            out = [r for r in out if r["date"] >= since]
        off, lim = int(q.get("$offset", 0)), int(q["$limit"])
        return 200, {}, json.dumps(out[off : off + lim])

    return cb


def month_rows(path):
    return pq.read_table(path).to_pylist()


def test_shift_month_crosses_years():
    assert extract.shift_month(date(2026, 1, 1), -1) == date(2025, 12, 1)
    assert extract.shift_month(date(2025, 12, 1), 1) == date(2026, 1, 1)


@responses.activate
def test_first_run_loads_everything_into_monthly_files(tmp_path):
    data = [row("2026-07-31T23:00:00.000", 5), row("2026-08-01T00:00:00.000", 7), row("2026-08-01T01:00:00.000", 0)]
    responses.get(META, json={"rowsUpdatedAt": 100})
    responses.add_callback(responses.GET, ROWS, callback=rows_callback(data))

    state = {}
    assert extract.extract_counter(C, tmp_path, state) == 3

    folder = tmp_path / "counters" / "test"
    assert extract.stored_months(folder) == ["2026-07", "2026-08"]
    aug = month_rows(folder / "2026-08.parquet")
    assert [r["total"] for r in aug] == ["7", "0"]
    assert ":id" not in aug[0]  # Socrata system fields are dropped
    assert state["test"]["rows_updated_at"] == 100
    assert "$where" not in responses.calls[1].request.url


@responses.activate
def test_unchanged_dataset_is_skipped(tmp_path):
    responses.get(META, json={"rowsUpdatedAt": 100})
    responses.add_callback(responses.GET, ROWS, callback=rows_callback([row("2026-08-01T00:00:00.000", 1)]))
    state = {}
    extract.extract_counter(C, tmp_path, state)
    calls = len(responses.calls)

    assert extract.extract_counter(C, tmp_path, state) == 0
    assert len(responses.calls) == calls + 1  # only the metadata request


@responses.activate
def test_incremental_run_refetches_lookback_window_and_replaces_it(tmp_path, monkeypatch):
    monkeypatch.setattr(extract, "LOOKBACK_MONTHS", 2)
    old = [row("2026-06-15T10:00:00.000", 1), row("2026-07-15T10:00:00.000", 2), row("2026-08-15T10:00:00.000", 3)]
    responses.get(META, json={"rowsUpdatedAt": 100})
    responses.add_callback(responses.GET, ROWS, callback=rows_callback(old))
    state = {}
    extract.extract_counter(C, tmp_path, state)

    # The publisher corrects July and adds September.
    new = [old[0], row("2026-07-15T10:00:00.000", 20), old[2], row("2026-09-01T00:00:00.000", 4)]
    responses.replace(responses.GET, META, json={"rowsUpdatedAt": 200})
    responses.remove(responses.GET, ROWS)
    responses.add_callback(responses.GET, ROWS, callback=rows_callback(new))

    # Latest stored month is August, so the window starts at July.
    assert extract.extract_counter(C, tmp_path, state) == 3
    assert "2026-07-01T00:00:00" in parse_qs(urlparse(responses.calls[-1].request.url).query)["$where"][0]

    folder = tmp_path / "counters" / "test"
    assert [r["total"] for r in month_rows(folder / "2026-07.parquet")] == ["20"]
    assert [r["total"] for r in month_rows(folder / "2026-06.parquet")] == ["1"]  # outside window, untouched
    assert extract.stored_months(folder) == ["2026-06", "2026-07", "2026-08", "2026-09"]


@responses.activate
def test_pages_through_large_results(tmp_path, monkeypatch):
    monkeypatch.setattr(extract, "PAGE", 2)
    data = [row(f"2026-08-01T0{h}:00:00.000", h) for h in range(5)]
    responses.get(META, json={"rowsUpdatedAt": 1})
    responses.add_callback(responses.GET, ROWS, callback=rows_callback(data))
    assert extract.extract_counter(C, tmp_path, {}) == 5
    assert len(month_rows(tmp_path / "counters/test/2026-08.parquet")) == 5


@responses.activate
def test_weather_is_split_by_month_and_stops_at_archive_lag(tmp_path, monkeypatch):
    monkeypatch.setattr(extract, "WEATHER_START", date(2026, 7, 31))
    times = ["2026-07-31T23:00", "2026-08-01T00:00"]
    responses.get(
        extract.OPEN_METEO,
        json={"hourly": {"time": times} | {v: [0.5, None] for v in extract.WEATHER_VARS}},
    )
    state = {}
    extract.extract_weather(tmp_path, state, today=date(2026, 8, 8))

    q = parse_qs(urlparse(responses.calls[0].request.url).query)
    assert q["end_date"] == ["2026-08-02"]  # 6 days before "today"
    assert q["timezone"] == ["GMT"]
    assert extract.stored_months(tmp_path / "weather") == ["2026-07", "2026-08"]
    aug = month_rows(tmp_path / "weather/2026-08.parquet")
    assert aug[0]["time_utc"] == "2026-08-01T00:00" and aug[0]["precipitation"] is None


def test_failed_source_does_not_stop_the_others(tmp_path, monkeypatch):
    seen = []

    def fake_counter(c, lake, state, full):
        seen.append(c.id)
        if c.id == "fremont":
            raise extract.requests.ConnectionError("down")

    monkeypatch.setattr(extract, "extract_counter", fake_counter)
    monkeypatch.setattr(extract, "extract_weather", lambda *a, **k: 0)
    with pytest.raises(SystemExit, match="fremont"):
        extract.run(tmp_path, today=date(2026, 8, 8))
    assert len(seen) == len(extract.COUNTERS)
