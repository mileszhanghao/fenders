"""The Airflow DAG loads, has the right shape, and runs end to end on the fixture lake.

Skipped when Airflow isn't installed (it needs Python <= 3.12 and its own constraints file):
    pip install "apache-airflow==3.1.8" --constraint \
      https://raw.githubusercontent.com/apache/airflow/constraints-3.1.8/constraints-3.12.txt
"""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow")

ROOT = Path(__file__).resolve().parent.parent
DAGS = ROOT / "dags"


def load_dag():
    from airflow.models.dagbag import DagBag

    bag = DagBag(dag_folder=str(DAGS), include_examples=False)
    assert bag.import_errors == {}
    return bag.dags["fenders_daily"]


def test_dag_shape():
    dag = load_dag()
    assert sorted(dag.task_ids) == ["bucket_configured", "dbt_build", "export", "extract", "publish_s3"]
    order = ["extract", "dbt_build", "export", "bucket_configured", "publish_s3"]
    for upstream, downstream in zip(order, order[1:], strict=False):
        assert downstream in dag.get_task(upstream).downstream_task_ids
    assert dag.max_active_runs == 1
    assert dag.get_task("extract").retries == 2


def run_dag(tmp_path, corrupt: bool = False) -> tuple[dict, Path]:
    """Copy the repo, put the fixture lake in place, and run the whole DAG with extract off."""
    home = tmp_path / "fenders"
    shutil.copytree(ROOT, home, ignore=shutil.ignore_patterns(".git", "lake", "site", "build", "*.duckdb"))
    (home / "site").mkdir()
    from tests.fixture_lake import make

    make(home / "lake")
    if corrupt:  # a negative bike count: the non-negative data test must catch it
        import pyarrow.compute as pc
        import pyarrow.parquet as pq

        month = sorted((home / "lake" / "counters" / "fremont").glob("*.parquet"))[0]
        table = pq.read_table(month)
        col = table.column("fremont_bridge_nb").to_pylist()
        col[100] = "-50"
        i = table.schema.get_field_index("fremont_bridge_nb")
        table = table.set_column(i, "fremont_bridge_nb", pc.cast(col, table.schema.field(i).type))
        pq.write_table(table, month)
    env = os.environ | {
        "FENDERS_HOME": str(home),
        "FENDERS_LAKE": str(home / "lake"),
        "FENDERS_BUCKET": "",
        "FENDERS_RETRIES": "0",
        "AIRFLOW_HOME": str(tmp_path / "airflow_home"),
        "AIRFLOW__CORE__DAGS_FOLDER": str(home / "dags"),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "PYTHONPATH": str(home),
    }
    script = "\n".join(
        [
            f"import json, sys; sys.path.insert(0, {str(home)!r})",
            "from airflow.models.dagbag import DagBag",
            f"dag = DagBag(dag_folder={str(home / 'dags')!r}, include_examples=False).get_dag('fenders_daily')",
            "run = dag.test(run_conf={'extract': False})",
            "print('STATES', json.dumps({ti.task_id: str(ti.state) for ti in run.get_task_instances()}))",
        ]
    )
    subprocess.run([sys.executable, "-m", "airflow", "db", "migrate"], env=env, capture_output=True, check=True)
    result = subprocess.run([sys.executable, "-c", script], env=env, capture_output=True, text=True, cwd=home)
    states_line = [line for line in result.stdout.splitlines() if line.startswith("STATES ")]
    assert states_line, result.stdout[-3000:] + result.stderr[-3000:]
    return json.loads(states_line[-1][len("STATES ") :]), home


def test_dag_runs_end_to_end_on_fixture_lake(tmp_path):
    states, home = run_dag(tmp_path)
    assert states == {
        "extract": "success",
        "dbt_build": "success",
        "export": "success",
        "bucket_configured": "success",
        "publish_s3": "skipped",
    }
    summary = json.loads((home / "site" / "data" / "summary.json").read_text())
    assert {c["counter_id"] for c in summary["counters"]} == {"fremont", "thomas_st"}
    assert (home / "site" / "data" / "tableau" / "daily.csv").exists()


def test_failed_data_test_stops_the_run_before_publishing(tmp_path):
    states, home = run_dag(tmp_path, corrupt=True)
    assert states["dbt_build"] == "failed"
    assert states["export"] == "upstream_failed"
    assert not (home / "site" / "data").exists()
