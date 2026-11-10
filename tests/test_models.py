"""Run the dbt project against the fixture lake and check the answers it was built to give."""

from pathlib import Path

import pytest


def one(con, sql):
    return con.execute(sql).fetchone()


def test_rain_halves_riding_in_the_fixture(db):
    change, hours = one(
        db, "select change, wet_hours from rain_effect where counter_id='fremont' and sky='rain' and daypart='all'"
    )
    assert hours == 3
    assert change == pytest.approx(-0.5)


def test_outage_day_is_excluded_not_counted_as_zero(db):
    row = one(
        db, "select is_suspect_zero, bikes from daily_counts where counter_id='thomas_st' and date_local='2025-11-04'"
    )
    assert row == (True, None)
    assert one(db, "select count(*) from fct_hourly where counter_id='thomas_st' and date_local='2025-11-04'")[0] == 0


def test_fall_back_hour_combines_two_utc_hours(db):
    utc_hours, flagged = one(
        db,
        """select w.utc_hours, h.is_fall_back_hour
           from stg_weather_hourly w join int_counter_hourly h using (hour_local)
           where h.counter_id = 'fremont' and h.hour_local = '2025-11-02 01:00:00'""",
    )
    assert (utc_hours, flagged) == (2, True)
    assert one(db, "select count(*) from int_counter_hourly where is_fall_back_hour")[0] == 2  # one per counter


def test_every_day_has_24_clock_hours(db):
    assert one(
        db, "select min(hours_reported), max(hours_reported) from int_counter_days where counter_id='fremont'"
    ) == (24, 24)


def test_pedestrians_are_kept_apart_from_bikes(db):
    bikes, peds = one(
        db,
        "select bikes, pedestrians from int_counter_hourly"
        " where counter_id='thomas_st' and hour_local='2025-10-30 12:00:00'",
    )
    assert (bikes, peds) == (20, 6)


def test_export_writes_dashboard_files(db, tmp_path):
    from fenders.export import export

    path = db.execute("select path from duckdb_databases() where database_name = current_database()").fetchone()[0]
    s = export(Path(path), tmp_path / "no-lake", tmp_path / "site")
    assert {c["counter_id"] for c in s["counters"]} == {"fremont", "thomas_st"}
    daily = __import__("json").loads((tmp_path / "site/daily.json").read_text())
    i = daily["thomas_st"]["d"].index("2025-11-04")
    assert daily["thomas_st"]["b"][i] is None  # outage shows as a gap


def test_tableau_export_has_readable_flat_tables(db, tmp_path):
    from fenders.tableau import export

    path = db.execute("select path from duckdb_databases() where database_name = current_database()").fetchone()[0]
    counts = export(Path(path), tmp_path)
    assert set(counts) == {"hourly", "hourly_profile", "daily", "rain_effect"}
    header = (tmp_path / "hourly.csv").read_text().splitlines()[0]
    assert "counter" in header and "is_dry_baseline" in header
    assert "Thomas St Overpass" in (tmp_path / "daily.csv").read_text()
