"""Airflow DAG: the Fenders pipeline as four tasks with retries and a quality gate.

    extract  ->  dbt_build  ->  export  ->  publish_s3 (only when FENDERS_BUCKET is set)

- extract:   pull new counter and weather rows into the Parquet lake (idempotent; safe to retry)
- dbt_build: build the models and run the 21 data tests; a failing test fails this task,
             so nothing downstream runs and the dashboard keeps the last good data
- export:    write the dashboard JSON and the Tableau CSVs
- publish_s3: upload the dashboard files, dated Parquet snapshots of the marts and a manifest

Paths come from environment variables so the same DAG runs locally, in Docker or on a
managed Airflow (MWAA, Astronomer):
    FENDERS_HOME    repository checkout (default: the folder above this file)
    FENDERS_LAKE    Parquet lake        (default: $FENDERS_HOME/lake)
    FENDERS_BUCKET  S3 bucket; empty = skip publishing
    FENDERS_RETRIES retries per task (default 2, with exponential backoff from 5 minutes)

Trigger with {"full": true} to reload everything, or {"extract": false} to rebuild from
the lake that's already there.
"""

from __future__ import annotations

import os
import subprocess
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

HOME = Path(os.environ.get("FENDERS_HOME", Path(__file__).resolve().parents[1]))
LAKE = Path(os.environ.get("FENDERS_LAKE", HOME / "lake"))
DB = HOME / "transform" / "fenders.duckdb"
SITE_DATA = HOME / "site" / "data"
RETRIES = int(os.environ.get("FENDERS_RETRIES", "2"))


@dag(
    dag_id="fenders_daily",
    description="Seattle bike counters + weather -> dbt on DuckDB -> dashboard (and S3)",
    schedule="17 14 * * *",  # 07:17 Seattle time (PDT), same as the GitHub Actions schedule
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,  # extract already catches up by itself; no need to replay missed days
    max_active_runs=1,  # two runs writing the same lake would race
    default_args={
        "retries": RETRIES,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "execution_timeout": timedelta(minutes=45),
    },
    params={
        "full": Param(False, type="boolean", description="Reload everything from the sources"),
        "extract": Param(True, type="boolean", description="Pull new data first (false = rebuild from the lake)"),
    },
    tags=["fenders", "dbt", "duckdb"],
    doc_md=__doc__,
)
def fenders_daily():
    @task
    def extract() -> dict:
        params = get_current_context()["params"]
        if not params["extract"]:
            return {"skipped": True}
        from fenders.extract import run

        state = run(LAKE, full=params["full"])
        return {name: s.get("loaded_at") for name, s in state.items()}

    @task
    def dbt_build() -> None:
        env = os.environ | {"FENDERS_LAKE": str(LAKE), "FENDERS_DB": str(DB)}
        result = subprocess.run(
            ["dbt", "build", "--profiles-dir", "."],
            cwd=HOME / "transform",
            env=env,
            capture_output=True,
            text=True,
        )
        print(result.stdout[-8000:])
        if result.returncode != 0:
            raise RuntimeError("dbt build failed (a model or data test errored); nothing will be published")

    @task
    def export() -> dict:
        from fenders.export import export as export_dashboard
        from fenders.tableau import export as export_tableau

        summary = export_dashboard(DB, LAKE, SITE_DATA)
        tables = export_tableau(DB, SITE_DATA / "tableau")
        return {"counters": len(summary["counters"]), "tableau_rows": tables}

    @task.short_circuit
    def bucket_configured() -> bool:
        return bool(os.environ.get("FENDERS_BUCKET"))

    @task
    def publish_s3() -> dict:
        from fenders.publish_s3 import publish

        manifest = publish(DB, SITE_DATA, os.environ["FENDERS_BUCKET"], HOME / "build" / "marts")
        return {table: meta["rows"] for table, meta in manifest["marts"].items()}

    extract() >> dbt_build() >> export() >> bucket_configured() >> publish_s3()


fenders_daily()
