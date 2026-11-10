# Databricks notebook source
# MAGIC %md
# MAGIC # Fenders on Databricks: does rain keep Seattle off its bikes?
# MAGIC
# MAGIC Reads the `fct_hourly` mart that the daily pipeline publishes to S3 as Parquet, recomputes the
# MAGIC rain effect in Spark SQL, and adds a year-by-year view. The SQL lives in `rain_effect.sql`
# MAGIC next to this notebook and is tested against the DuckDB/dbt version in `tests/test_databricks.py`.
# MAGIC
# MAGIC **Where the data comes from** (set the widget):
# MAGIC - `s3://<bucket>/marts/fct_hourly/latest/` after `infra/` is applied, or
# MAGIC - a Unity Catalog volume / DBFS path where you uploaded `fct_hourly.parquet`
# MAGIC   (Free Edition: *Catalog > Create volume > Upload*).

# COMMAND ----------

dbutils.widgets.text("source", "/Volumes/workspace/default/fenders/fct_hourly.parquet", "fct_hourly Parquet path")  # noqa: F821
source = dbutils.widgets.get("source")  # noqa: F821

fct_hourly = spark.read.parquet(source)  # noqa: F821
fct_hourly.createOrReplaceTempView("fct_hourly")
display(fct_hourly.groupBy("counter_id").count().orderBy("counter_id"))  # noqa: F821

# COMMAND ----------


# Run every statement in rain_effect.sql (it defines the temp views rain_effect and rain_effect_by_year).
# In a Databricks Git folder the notebook runs from its own folder, so the file sits right next to it.
def statements(text: str) -> list[str]:
    lines = [line for line in text.splitlines() if not line.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


with open("rain_effect.sql") as f:
    for statement in statements(f.read()):
        spark.sql(statement)  # noqa: F821

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All counters, by part of the day
# MAGIC select sky, daypart, wet_hours, observed, expected, change
# MAGIC from rain_effect
# MAGIC where counter_id = 'all'
# MAGIC order by sky, daypart

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Has the rain penalty changed over the years?
# MAGIC select * from rain_effect_by_year order by year

# COMMAND ----------

# Save the results as Delta tables so a dashboard or another notebook can use them.
spark.table("rain_effect").write.mode("overwrite").saveAsTable("fenders_rain_effect")  # noqa: F821
spark.table("rain_effect_by_year").write.mode("overwrite").saveAsTable("fenders_rain_effect_by_year")  # noqa: F821
