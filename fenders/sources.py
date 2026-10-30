"""What the pipeline pulls. Adding a counter is one entry here plus its column mapping
in transform/seeds/counter_columns.csv."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class Counter:
    id: str  # our short name, used in file paths and models
    dataset: str  # Socrata dataset id on data.seattle.gov
    time_field: str  # column holding the (local, timezone-less) hour
    name: str


COUNTERS: list[Counter] = [
    Counter("fremont", "65db-xm6k", "date", "Fremont Bridge"),
    Counter("spokane", "upms-nr8w", "date", "Spokane St. Bridge"),
    Counter("second_ave", "avwm-i8ym", "date", "2nd Ave Cycle Track"),
    Counter("burke_gilman", "2z5v-ecg8", "date", "Burke-Gilman Trail at NE 70th St"),
    Counter("twelfth_ave_s", "m2yw-err8", "time", "12th Ave S"),
    Counter("thomas_st", "t8i6-tipf", "time", "Thomas St Overpass"),
]

SOCRATA = "https://data.seattle.gov"

# One weather point for the whole city: the counters are all within about 10 km of it.
WEATHER_POINT = (47.62, -122.33)
WEATHER_START = date(2012, 10, 1)  # the Fremont counter's first month
WEATHER_VARS = ["precipitation", "temperature_2m", "wind_speed_10m", "cloud_cover"]
OPEN_METEO = "https://archive-api.open-meteo.com/v1/archive"
