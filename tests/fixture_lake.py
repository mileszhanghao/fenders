"""A tiny lake with known answers, for testing the dbt models without the network.

Two counters over 2025-10-01 .. 2025-11-05 (clocks fall back on 2025-11-02):
* fremont publishes `date`, two directions and a total. 10 riders an hour north and
  south in daytime, 1 at night. It rains (2 mm/h) 14:00-16:59 local on 2025-10-31,
  and riding halves in those hours.
* thomas_st publishes `time`, bikes and pedestrians. On 2025-11-04 the sensor is
  stuck at zero all day (an outage the models must exclude).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

TZ = ZoneInfo("America/Los_Angeles")
START, END = datetime(2025, 10, 1), datetime(2025, 11, 6)
RAIN_DAY, RAIN_HOURS = 31, range(14, 17)
OUTAGE_DAY = 4


def local_hours():
    """Wall-clock hours as the counters publish them: 24 rows a day, including on DST days."""
    t = START
    while t < END:
        yield t
        t += timedelta(hours=1)


def riders(t: datetime) -> int:
    base = 10 if 7 <= t.hour <= 19 else 1
    if t.month == 10 and t.day == RAIN_DAY and t.hour in RAIN_HOURS:
        base //= 2
    return base


def write(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = sorted({k for r in rows for k in r})
    pq.write_table(pa.table({c: pa.array([r.get(c) for r in rows], pa.string()) for c in cols}), path)


def make(lake: Path) -> None:
    fremont, thomas = {}, {}
    for t in local_hours():
        n = riders(t)
        if t.month == 11 and t.day == 2 and t.hour == 1:
            n *= 2  # the fall-back 01:00 row holds two real hours
        ts = t.strftime("%Y-%m-%dT%H:00:00.000")
        fremont.setdefault(ts[:7], []).append(
            {
                "date": ts,
                "fremont_bridge_nb": str(n),
                "fremont_bridge_sb": str(n),
                "fremont_bridge": str(2 * n),
                "_loaded_at": "2025-11-10T00:00:00+00:00",
            }
        )
        dead = t.month == 11 and t.day == OUTAGE_DAY
        b, p = ("0", "0") if dead else (str(n), "3")
        thomas.setdefault(ts[:7], []).append(
            {
                "time": ts,
                "bicyclist_northbound": b,
                "bicyclist_southbound": b,
                "pedestrian_northbound": p,
                "pedestrian_southbound": p,
                "thomas_st_overpass": str(2 * int(b) + 2 * int(p)),
                "_loaded_at": "2025-11-10T00:00:00+00:00",
            }
        )
    for month, rows in fremont.items():
        write(lake / "counters" / "fremont" / f"{month}.parquet", rows)
    for month, rows in thomas.items():
        write(lake / "counters" / "thomas_st" / f"{month}.parquet", rows)

    # Weather is published in UTC: every real hour once, so 25 hours on the fall-back day.
    weather: dict[str, dict[str, list]] = {}
    t = START.replace(tzinfo=TZ).astimezone(ZoneInfo("UTC"))
    end = END.replace(tzinfo=TZ).astimezone(ZoneInfo("UTC"))
    while t < end:
        local = t.astimezone(TZ)
        rain = 2.0 if (local.month == 10 and local.day == RAIN_DAY and local.hour in RAIN_HOURS) else 0.0
        m = weather.setdefault(
            t.strftime("%Y-%m"),
            {k: [] for k in ("time_utc", "precipitation", "temperature_2m", "wind_speed_10m", "cloud_cover")},
        )
        m["time_utc"].append(t.strftime("%Y-%m-%dT%H:%M"))
        m["precipitation"].append(rain)
        m["temperature_2m"].append(10.0)
        m["wind_speed_10m"].append(5.0)
        m["cloud_cover"].append(50.0)
        t += timedelta(hours=1)
    for month, cols in weather.items():
        path = lake / "weather" / f"{month}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(
                {"time_utc": pa.array(cols["time_utc"], pa.string())}
                | {k: pa.array(v, pa.float64()) for k, v in cols.items() if k != "time_utc"}
            ),
            path,
        )


if __name__ == "__main__":
    import sys

    make(Path(sys.argv[1]))
