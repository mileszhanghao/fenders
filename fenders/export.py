"""Export the marts the dashboard needs as small JSON files.

python -m fenders.export --db transform/fenders.duckdb --lake lake --out site/data
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb


def rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict]:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def columns(con: duckdb.DuckDBPyConnection, sql: str) -> dict[str, list]:
    """Column-oriented JSON: a third the size of a list of objects for long series."""
    cur = con.execute(sql)
    names = [d[0] for d in cur.description]
    data = cur.fetchall()
    return {n: [r[i] for r in data] for i, n in enumerate(names)}


def export(db: Path, lake: Path, out: Path) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db), read_only=True)

    counters = rows(
        con,
        """
        select c.counter_id, c.name, c.counts,
               min(d.date_local)::varchar as first_day,
               max(d.date_local)::varchar as last_day,
               count(*) filter (where d.is_excluded) as excluded_days,
               count(*) as days
        from counters c join int_counter_days d using (counter_id)
        group by all order by first_day
        """,
    )
    effect = rows(con, "select * from rain_effect order by counter_id, sky, daypart")
    daily = {
        c["counter_id"]: columns(
            con,
            f"""select date_local::varchar as d, bikes as b, precipitation_mm as p
                from daily_counts where counter_id = '{c["counter_id"]}' order by date_local""",
        )
        for c in counters
    }
    by_hour = rows(
        con,
        """
        select counter_id, hour_of_day, is_weekend,
               round(avg(bikes) filter (where sky = 'dry' and not rained_in_prev_3h), 1) as dry,
               round(avg(bikes) filter (where sky = 'rain'), 1) as rain,
               count(*) filter (where sky = 'rain') as rain_hours
        from fct_hourly where bikes is not null and year >= year(current_date) - 3
        group by all order by all
        """,
    )

    state_path = lake / "_state.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else {}
    summary = {
        "generated_at": datetime.now(UTC).isoformat(timespec="minutes"),
        "counters": counters,
        "rain_effect": effect,
        "by_hour_last_3_years": by_hour,
        "weather_through": state.get("weather", {}).get("through"),
        "loads": {k: v.get("loaded_at") for k, v in state.items()},
    }
    (out / "summary.json").write_text(json.dumps(summary, default=str))
    (out / "daily.json").write_text(json.dumps(daily, default=str, separators=(",", ":")))
    con.close()
    return summary


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", type=Path, default=Path("transform/fenders.duckdb"))
    p.add_argument("--lake", type=Path, default=Path("lake"))
    p.add_argument("--out", type=Path, default=Path("site/data"))
    a = p.parse_args()
    s = export(a.db, a.lake, a.out)
    print(f"exported {len(s['counters'])} counters, {len(s['rain_effect'])} rain-effect rows")


if __name__ == "__main__":
    main()
