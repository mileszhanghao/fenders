"""Export flat CSV files for building the dashboard in Tableau (or Power BI, Excel).

    python -m fenders.tableau --db transform/fenders.duckdb --out site/data/tableau

Writes three tables with readable column names and labels, one row per grain:
    hourly.csv       one row per counter and local hour (working days only)
    daily.csv        one row per counter and day, excluded days kept with a flag
    rain_effect.csv  the matched wet-vs-dry comparison from the rain_effect mart
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

DAYPART = """
    case
        when is_weekend then 'Weekend'
        when hour_of_day in (7, 8, 9, 16, 17, 18) then 'Weekday commute'
        else 'Weekday other'
    end"""

QUERIES = {
    "hourly": f"""
        select
            c.name as counter,
            h.hour_local as datetime_local,
            h.date_local as date,
            h.year, h.month, h.hour_of_day as hour,
            case when h.is_weekend then 'Weekend' else 'Weekday' end as day_type,
            {DAYPART} as daypart,
            h.bikes,
            round(h.precipitation_mm, 2) as precipitation_mm,
            round(h.temperature_c, 1) as temperature_c,
            upper(left(h.sky, 1)) || substr(h.sky, 2) as sky,
            -- The comparison baseline: dry now and for the three hours before.
            (h.sky = 'dry' and not h.rained_in_prev_3h) as is_dry_baseline
        from fct_hourly h join counters c using (counter_id)
        where h.bikes is not null
        order by counter, datetime_local
    """,
    "daily": """
        select
            c.name as counter,
            d.date_local as date,
            year(d.date_local) as year, month(d.date_local) as month,
            dayname(d.date_local) as weekday,
            d.bikes,
            d.is_excluded as excluded,
            d.precipitation_mm,
            d.high_c,
            case when d.precipitation_mm >= 5 then 'Wet day (5 mm+)'
                 when d.precipitation_mm >= 1 then 'Damp day (1-5 mm)'
                 else 'Dry day' end as day_weather
        from daily_counts d join counters c using (counter_id)
        order by counter, date
    """,
    "rain_effect": """
        select
            coalesce(c.name, 'All counters') as counter,
            upper(left(r.sky, 1)) || substr(r.sky, 2) as sky,
            case r.daypart when 'all' then 'All daytime hours'
                 else upper(left(r.daypart, 1)) || substr(r.daypart, 2) end as daypart,
            r.wet_hours, r.observed as observed_riders, r.expected as expected_riders,
            round(100 * r.change, 1) as change_pct
        from rain_effect r left join counters c using (counter_id)
        order by counter, sky, daypart
    """,
}


def export(db: Path, out: Path) -> dict[str, int]:
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db), read_only=True)
    counts = {}
    for name, sql in QUERIES.items():
        path = out / f"{name}.csv"
        con.execute(f"copy ({sql}) to '{path}' (header, delimiter ',')")
        counts[name] = con.execute(f"select count(*) from read_csv_auto('{path}')").fetchone()[0]
    con.close()
    return counts


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", type=Path, default=Path("transform/fenders.duckdb"))
    p.add_argument("--out", type=Path, default=Path("site/data/tableau"))
    a = p.parse_args()
    for name, n in export(a.db, a.out).items():
        print(f"{name}.csv: {n:,} rows")


if __name__ == "__main__":
    main()
