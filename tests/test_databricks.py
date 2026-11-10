"""The Databricks Spark SQL must give the same rain effect as the dbt model on DuckDB.

Runs the SQL on a local Spark session against the fct_hourly mart built from the fixture lake.
Skipped when PySpark (and Java) aren't installed: `pip install pyspark` to run it.
"""

from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

SQL = Path(__file__).resolve().parent.parent / "databricks" / "rain_effect.sql"


def statements(text: str) -> list[str]:
    lines = [line for line in text.splitlines() if not line.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("fenders-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_spark_sql_matches_dbt(spark, db, tmp_path):
    parquet = tmp_path / "fct_hourly.parquet"
    db.execute(f"copy (select * from fct_hourly) to '{parquet}' (format parquet)")
    spark.read.parquet(str(parquet)).createOrReplaceTempView("fct_hourly")
    for statement in statements(SQL.read_text()):
        spark.sql(statement)

    cols = "counter_id, sky, daypart, wet_hours, observed, expected, change"
    expected = sorted(tuple(r) for r in db.execute(f"select {cols} from rain_effect").fetchall())
    got = sorted(
        (r.counter_id, r.sky, r.daypart, r.wet_hours, int(r.observed), float(r.expected), float(r.change))
        for r in spark.sql(f"select {cols} from rain_effect").collect()
    )
    expected = [(c, s, d, w, int(o), float(e), float(ch)) for c, s, d, w, o, e, ch in expected]
    assert got == expected
    assert len(got) > 0


def test_year_view_runs(spark, db, tmp_path):
    parquet = tmp_path / "fct_hourly.parquet"
    db.execute(f"copy (select * from fct_hourly) to '{parquet}' (format parquet)")
    spark.read.parquet(str(parquet)).createOrReplaceTempView("fct_hourly")
    for statement in statements(SQL.read_text()):
        spark.sql(statement)
    rows = spark.sql("select * from rain_effect_by_year").collect()
    assert [r.year for r in rows] == [2025]
    assert rows[0].change == pytest.approx(-0.5)
