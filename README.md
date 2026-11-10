# Fenders

**Does rain keep Seattle off its bikes?** Most Seattle cyclists ride with fenders, and this pipeline measures how much the rain still costs. Every morning it pulls the city's bike counters and hourly weather, cleans and models them with dbt, tests the result, and publishes a dashboard.

**Dashboard:** https://mileszhanghao.github.io/fenders/ · **Model docs and lineage:** https://mileszhanghao.github.io/fenders/dbt/ · **Tableau Public version:** [Fenders on Tableau Public](https://public.tableau.com/app/profile/hao.zhang4334/viz/FenderswhoridesintheSeattlerain/FenderswhoridesintheSeattlerain)

**Stack:** Python (requests, PyArrow) · Parquet · DuckDB · dbt (dbt-duckdb) · GitHub Actions (schedule) · Observable Plot · Tableau Public · pytest · ruff

## The answer so far

Six counters, October 2012 to September 2026:

| | Drizzle (under 1 mm/h) | Rain (1 mm/h or more) |
|---|---|---|
| All daytime hours | −33% | −48% |
| Weekday rush hours (7–9, 16–18) | −25% | −38% |
| Other weekday hours | −34% | −50% |
| Weekends | −47% | −69% |

Rain halves riding, but not for everyone. Most commuters ride anyway; weekend riders are the ones who stay home. It shows by route too: the Burke-Gilman Trail, a recreation trail, loses 67% of riders in the rain, while the 2nd Ave Cycle Track downtown loses 37%. The dashboard has the breakdown per counter and hour, and the numbers update as new data arrives.

**How "less" is measured.** Each wet hour is compared with dry hours at the same counter, in the same month of the same year, on the same kind of day (weekday or weekend), at the same hour. That holds the season, the long-term trend (including 2020), the weekly rhythm and the rush hours constant. "Dry" hours must also have had no rain in the three hours before, since wet pavement keeps people off bikes too. Then change = riders in wet hours ÷ riders expected from the matching dry hours − 1.

## Tableau version

The same results are also on [Tableau Public](https://public.tableau.com/app/profile/hao.zhang4334/viz/FenderswhoridesintheSeattlerain/FenderswhoridesintheSeattlerain), built from the flat CSV files that `fenders/tableau.py` exports (`site/data/tableau/`). It has three views:

- **Who stays home in the rain:** the change in riders for drizzle and rain, by weekday commute, other weekday hours and weekend (`rain_effect.csv`).
- **Rain effect by counter:** the change in each counter during rain, all daytime hours (`rain_effect.csv`).
- **A weekday, dry vs rain:** average riders per counter-hour by hour of day, weekdays since 2023 (`hourly_profile.csv`, as `SUM(riders) / SUM(hours)`).

The numbers match the dbt marts: -48.1% in rain across all counters, -38.4% for the weekday commute and -68.8% on weekends. The Tableau workbook is a snapshot uploaded by hand; the GitHub Pages dashboard is the one that updates daily.

## Pipeline

```
data.seattle.gov (6 counter datasets)   Open-Meteo archive (hourly, UTC)
            │                                       │
            └──────── extract (Python) ─────────────┘
                              │   monthly Parquet files, `lake` branch
                              ▼
                     dbt on DuckDB
   staging      stg_counter_readings · stg_weather_hourly
   intermediate int_counter_readings · int_counter_hourly · int_counter_days
   marts        fct_hourly · rain_effect · daily_counts
                              │   21 data tests; any error stops the run
                              ▼
                 export → JSON → GitHub Pages dashboard
```

Every run is a GitHub Actions job (`.github/workflows/pipeline.yml`) on a daily schedule.

1. **Extract** (`fenders/extract.py`). For each counter, it first checks the dataset's `rowsUpdatedAt`. If nothing changed since the last run, nothing is downloaded. Otherwise it re-fetches the last two months (`$where date >= …`, paged 50,000 rows at a time) and overwrites those monthly files. Weather is fetched the same way from Open-Meteo, in UTC. Re-running gives the same lake, so a failed or repeated run is harmless. `--full` rebuilds everything from scratch.
2. **Land** the files on the repo's `lake` branch: about 7 MB of Parquet for 14 years of hourly data, versioned by git.
3. **Transform and test** with dbt. If any test fails with an error, the job stops before publishing, and the lake is saved but the dashboard keeps the last good data.
4. **Export** the marts to JSON and deploy the dashboard and the dbt docs.
5. **Publish to S3** (optional, `fenders/publish_s3.py`): the dashboard files, a dated Parquet snapshot of every mart (`marts/<table>/dt=YYYY-MM-DD/`), and a `_manifest.json` with row counts and SHA-256 checksums. Runs only when the repository variables from `infra/` are set.

## AWS: storage, monitoring and infrastructure as code

```
GitHub Actions ──OIDC (no stored keys)──► IAM role ──► S3 bucket (private, versioned, encrypted)
                                                          │  _manifest.json
EventBridge (daily) ──► Lambda `freshness` ──reads────────┘
                              │
                              ├──► CloudWatch metrics: Healthy, HoursSincePublish, MartRows
                              └──► SNS email when something is wrong
```

- **Terraform** (`infra/`) creates everything: the bucket with public access blocked, versioning, encryption and a lifecycle rule that moves snapshots to cheaper storage after 30 days and deletes them after 180; a least-privilege role that only the `main` branch of this repo can assume through GitHub OIDC; the Lambda, its daily schedule and log group; an SNS topic; and a CloudWatch alarm that also fires if the Lambda stops reporting at all.
- **The freshness Lambda** (`aws/freshness/handler.py`) catches the failures a green CI badge doesn't: a pipeline that silently stopped running, a weather source that stopped advancing, or a mart that came out empty.
- **Tested without an AWS account:** `tests/test_aws.py` runs the publisher and the Lambda against moto's in-memory S3, SNS, SQS and CloudWatch, including the stale, missing and empty cases.

```bash
cd infra
terraform init
terraform apply -var 'alert_email=you@example.com'
# then set repository variables AWS_ROLE_ARN and FENDERS_BUCKET from the outputs
```

## Databricks

`databricks/fenders_rain_effect.py` is a Databricks notebook that reads the `fct_hourly` Parquet (from the S3 bucket or an uploaded volume), recomputes the rain effect in Spark SQL (`databricks/rain_effect.sql`), adds a year-by-year view, and saves both as Delta tables. `tests/test_databricks.py` runs the same SQL on a local Spark session and checks it gives exactly the same numbers as the dbt model on DuckDB (`pip install pyspark` to run it; skipped otherwise).

## Problems in the data, and what the pipeline does about them

These came up on the first full load. The tests caught the first three; the first run failed, and nothing was published until each was handled.

| Problem | What it looked like | Handling |
|---|---|---|
| Duplicate months | Burke-Gilman's September 2020 was published twice, identical rows | Exact repeats are kept once (`int_counter_readings`) |
| Conflicting duplicates | Spokane St, May 2022 to July 2025: some hours have two rows with different counts, one often much lower or empty | An empty row loses to one with values. When two rows disagree, there is no way to know which is right, so the hour is left out and flagged (903 hours), instead of adding them up and double counting |
| Sensor error values | Burke-Gilman reports 8191 (2¹³−1) and 4095 (2¹²−1) riders in an hour, against a 99.9th percentile of about 250 | A reading over 3× its column's 99.9th percentile is treated as an error (19 hours in all) |
| Totals that don't add up | In 2024, Spokane St's published total differs from east + west by a few riders in 1.8% of hours | A test fails the build above 5% (that would mean the column mapping is wrong) and warns above 1% (the publisher's own noise), which is where this one sits |
| Dead sensors | Days of exact zeros (Thomas St: 197 days) | A day with 8+ daytime hours of exactly zero is excluded, and so is a day missing more than one hour. Excluded days show as gaps on the charts, not as days nobody rode |
| Daylight saving | Counters publish local clock time with no zone: on the fall-back night one 01:00 row holds two real hours, and on the spring-forward night the 02:00 row exists but is empty | Weather (UTC) is moved onto the same local clock, and the two UTC hours of the fall-back night are combined just as the counter combines them. Both nights are covered by tests |
| Six different schemas | Columns like `fremont_bridge_nb`, `east`, `bike_north`, `_12th_ave_s_south_of_s_lane_1` | One seed (`counter_columns.csv`) maps each column to bike/pedestrian and direction. A test fails if a dataset adds a column the seed doesn't know |
| Late data | The city publishes in batches; data is usually one to two months behind | The daily job is cheap when nothing changed, and the two-month window picks up late rows and corrections |

**Is excluding zero days biasing the answer?** On Burke-Gilman and 12th Ave S, the excluded days are rainier than average (about 7 mm vs 3 mm). That could mean heavy rain breaks sensors, or that nobody rode. To check, I re-ran everything with `--vars '{exclude_zero_days: false}'`. No rain effect moved by more than 0.3 percentage points, so the result doesn't depend on that choice.

## Choices and trade-offs

- **dbt + DuckDB + GitHub Actions instead of Airflow and a warehouse.** The whole job is one linear daily run over about half a million counter-hours. Airflow would need a server running all year to schedule one job, while Actions is free, logs every run, and the schedule is a line of YAML. DuckDB reads the Parquet lake directly and builds every model in seconds. The cost: no backfill UI, no task-level retries, and Actions schedules can start late. At larger scale, or with many dependent jobs, I'd move this to Airflow or Dagster and a cloud warehouse. The dbt models would move as they are.
- **Raw data kept as published.** Extract stores every column as text with a load timestamp and does no cleaning. All interpretation happens in dbt, where it is tested and can be changed and re-run without downloading again.
- **Parquet on a git branch, not a database file.** Monthly files are small, diffable in history, and readable by anything. A DuckDB file in git would be one opaque blob rewritten every day.
- **One weather point for the city.** All six counters are within about 10 km of central Seattle, but a shower can hit Fremont and miss SoDo. Per-counter grid cells would be a modest refinement. The weather is ERA5-based reanalysis, not a rain gauge, so light rain is approximate. That is one reason the drizzle threshold is 0.1 mm rather than zero.
- **A matched comparison, not a regression.** It's easy to explain and check. It doesn't control for temperature within a month (rainy hours tend to be a bit colder), holidays, or events. A count regression (Poisson or negative binomial) with temperature, wind and holidays would be the next step.
- **Bikes and scooters.** Three counters (Fremont, Spokane St, 2nd Ave) count bikes and scooters together, as the city publishes them.

## Running it

```bash
pip install -e '.[dev]'
python -m fenders.extract --lake lake           # incremental; --full to reload
cd transform && FENDERS_LAKE=../lake dbt build --profiles-dir .
cd .. && python -m fenders.export --db transform/fenders.duckdb --lake lake --out site/data
python -m http.server -d site                   # dashboard on :8000
```

## Tests

```bash
pytest          # extract with mocked HTTP, the dbt project on a fixture lake, AWS on moto, Spark parity
```

- **Extract:** first load, skipping unchanged datasets, the re-fetch window replacing corrected months, paging, the weather lag, and one failing source not stopping the others.
- **Models:** a fixture lake (`tests/fixture_lake.py`) with known answers: rain that halves riding, a sensor stuck at zero, and the fall-back night. dbt builds it and the tests check the numbers.
- **AWS:** S3 publishing and the freshness Lambda against moto (healthy, stale, missing manifest, empty mart).
- **Databricks:** the Spark SQL port matches the dbt result row for row.
- **21 data tests** run on the real data in every pipeline run: uniqueness at every grain, accepted values, relationships to the seeds, non-negative counts, unmapped columns, and published totals.

CI runs lint and all tests on every push.

## Data and credits

- Bike counters: City of Seattle, Seattle Department of Transportation, via [data.seattle.gov](https://data.seattle.gov) (public domain). Datasets: Fremont Bridge `65db-xm6k`, Spokane St. Bridge `upms-nr8w`, 2nd Ave Cycle Track `avwm-i8ym`, Burke-Gilman Trail `2z5v-ecg8`, 12th Ave S `m2yw-err8`, Thomas St Overpass `t8i6-tipf`.
- Weather: [Open-Meteo](https://open-meteo.com) historical weather API (ERA5 reanalysis), CC BY 4.0.

Built by Miles Zhang. Developed with AI pair-programming assistance (Claude).
