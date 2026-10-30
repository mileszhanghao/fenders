"""Extract: pull new counter and weather data into the lake as monthly Parquet files.

Layout:
    lake/counters/<counter>/<YYYY-MM>.parquet   raw rows as published (all columns kept as text)
    lake/weather/<YYYY-MM>.parquet              hourly weather, UTC
    lake/_state.json                            what was loaded, and when

Each run re-fetches the last LOOKBACK_MONTHS months and overwrites those files, so
late-arriving rows and corrections replace what was there before. Running twice
gives the same lake (the job is idempotent), and a counter whose dataset hasn't
changed since the last run is skipped without downloading anything.

    python -m fenders.extract              # incremental
    python -m fenders.extract --full       # rebuild everything from the sources
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from fenders.sources import (
    COUNTERS,
    OPEN_METEO,
    SOCRATA,
    WEATHER_POINT,
    WEATHER_START,
    WEATHER_VARS,
    Counter,
)

log = logging.getLogger("fenders.extract")

LOOKBACK_MONTHS = 2
PAGE = 50_000
# Open-Meteo's archive is a few days behind real time.
WEATHER_LAG = timedelta(days=6)

session = requests.Session()
session.headers["User-Agent"] = "fenders/0.1 (portfolio data pipeline; https://github.com/mileszhanghao/fenders)"


def get_json(url: str, params: dict | None = None):
    """GET with retries for rate limits and server errors."""
    for attempt in range(5):
        r = session.get(url, params=params, timeout=120)
        if r.status_code in (429, 500, 502, 503, 504) and attempt < 4:
            time.sleep(2**attempt)
            continue
        r.raise_for_status()
        return r.json()
    raise AssertionError("unreachable")


def month_key(ts: str) -> str:
    """'2026-08-31T23:00:00.000' -> '2026-08'"""
    return ts[:7]


def month_start(key: str) -> date:
    return date(int(key[:4]), int(key[5:7]), 1)


def shift_month(d: date, months: int) -> date:
    m = d.year * 12 + d.month - 1 + months
    return date(m // 12, m % 12 + 1, 1)


def stored_months(folder: Path) -> list[str]:
    return sorted(p.stem for p in folder.glob("*.parquet"))


def window_start(folder: Path, full: bool) -> date | None:
    """First month to (re)fetch, or None for 'everything'."""
    months = stored_months(folder)
    if full or not months:
        return None
    return shift_month(month_start(months[-1]), -(LOOKBACK_MONTHS - 1))


def write_month(path: Path, rows: list[dict], loaded_at: str) -> None:
    columns = sorted({k for r in rows for k in r if not k.startswith(":")})
    table = pa.table(
        {c: pa.array([None if r.get(c) is None else str(r[c]) for r in rows], pa.string()) for c in columns}
        | {"_loaded_at": pa.array([loaded_at] * len(rows), pa.string())}
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    pq.write_table(table, tmp, compression="zstd")
    tmp.replace(path)


def extract_counter(c: Counter, lake: Path, state: dict, full: bool = False) -> int:
    """Load new rows for one counter. Returns the number of rows written."""
    meta = get_json(f"{SOCRATA}/api/views/{c.dataset}.json")
    updated = meta.get("rowsUpdatedAt")
    prev = state.get(c.id, {})
    folder = lake / "counters" / c.id
    if not full and prev.get("rows_updated_at") == updated and stored_months(folder):
        log.info("%s: unchanged since %s, skipping", c.id, prev.get("loaded_at"))
        return 0

    start = window_start(folder, full)
    params = {"$order": f"{c.time_field},:id", "$limit": PAGE}
    if start:
        params["$where"] = f"{c.time_field} >= '{start.isoformat()}T00:00:00'"

    by_month: dict[str, list[dict]] = defaultdict(list)
    offset = 0
    while True:
        rows = get_json(f"{SOCRATA}/resource/{c.dataset}.json", params | {"$offset": offset})
        for r in rows:
            if r.get(c.time_field):
                by_month[month_key(r[c.time_field])].append(r)
        offset += len(rows)
        if len(rows) < PAGE:
            break

    loaded_at = datetime.now(UTC).isoformat(timespec="seconds")
    for key, rows in by_month.items():
        write_month(folder / f"{key}.parquet", rows, loaded_at)

    state[c.id] = {
        "dataset": c.dataset,
        "rows_updated_at": updated,
        "loaded_at": loaded_at,
        "rows_last_run": offset,
        "months_written": sorted(by_month),
    }
    log.info("%s: %d rows into %d month files (from %s)", c.id, offset, len(by_month), start or "the beginning")
    return offset


def extract_weather(lake: Path, state: dict, today: date, full: bool = False) -> int:
    """Hourly weather in UTC from Open-Meteo's historical archive, one file per month."""
    folder = lake / "weather"
    end = today - WEATHER_LAG
    start = window_start(folder, full) or WEATHER_START
    if start > end:
        return 0

    by_month: dict[str, dict[str, list]] = {}
    chunk = start
    while chunk <= end:
        chunk_end = min(date(chunk.year, 12, 31), end)
        data = get_json(
            OPEN_METEO,
            {
                "latitude": WEATHER_POINT[0],
                "longitude": WEATHER_POINT[1],
                "start_date": chunk.isoformat(),
                "end_date": chunk_end.isoformat(),
                "hourly": ",".join(WEATHER_VARS),
                "timezone": "GMT",
            },
        )
        hourly = data["hourly"]
        for i, t in enumerate(hourly["time"]):
            month = by_month.setdefault(month_key(t), {k: [] for k in ["time", *WEATHER_VARS]})
            month["time"].append(t)
            for v in WEATHER_VARS:
                month[v].append(hourly[v][i])
        chunk = chunk_end + timedelta(days=1)

    loaded_at = datetime.now(UTC).isoformat(timespec="seconds")
    rows = 0
    folder.mkdir(parents=True, exist_ok=True)
    for key, cols in by_month.items():
        table = pa.table(
            {"time_utc": pa.array(cols["time"], pa.string())}
            | {v: pa.array(cols[v], pa.float64()) for v in WEATHER_VARS}
            | {"_loaded_at": pa.array([loaded_at] * len(cols["time"]), pa.string())}
        )
        pq.write_table(table, folder / f"{key}.parquet", compression="zstd")
        rows += len(cols["time"])
    state["weather"] = {"loaded_at": loaded_at, "through": end.isoformat(), "months_written": sorted(by_month)}
    log.info("weather: %d hours into %d month files", rows, len(by_month))
    return rows


def run(lake: Path, full: bool = False, today: date | None = None) -> dict:
    lake.mkdir(parents=True, exist_ok=True)
    state_path = lake / "_state.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else {}
    failures = []
    for c in COUNTERS:
        try:
            extract_counter(c, lake, state, full)
        except requests.RequestException as e:
            # One broken source shouldn't block the rest; the run still fails at the end.
            failures.append(c.id)
            log.error("%s: %s", c.id, e)
    extract_weather(lake, state, today or datetime.now(UTC).date(), full)
    state_path.write_text(json.dumps(state, indent=1, sort_keys=True) + "\n")
    if failures:
        raise SystemExit(f"failed: {', '.join(failures)}")
    return state


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--lake", type=Path, default=Path("lake"))
    p.add_argument("--full", action="store_true", help="ignore what's stored and reload everything")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run(args.lake, args.full)


if __name__ == "__main__":
    main()
