"""AWS Lambda: is the Fenders pipeline still alive?

Runs once a day (EventBridge schedule). Reads s3://$BUCKET/_manifest.json, written by
`python -m fenders.publish_s3` at the end of every pipeline run, and checks three things:

  1. the pipeline published recently            (published_at within MAX_AGE_HOURS)
  2. the weather source is still advancing      (weather_through within MAX_WEATHER_LAG_DAYS)
  3. no mart came out empty                     (every mart has rows > 0)

Each check is reported as a CloudWatch metric (namespace Fenders), so it can be graphed and
alarmed on. If any check fails, one SNS message lists what's wrong. A missing manifest counts
as a failure, not a crash.

Environment: BUCKET, TOPIC_ARN, MAX_AGE_HOURS (default 36), MAX_WEATHER_LAG_DAYS (default 7).
"""

from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime

import boto3
from botocore.exceptions import ClientError


def check(manifest: dict | None, now: datetime, max_age_hours: float, max_weather_lag_days: int) -> list[str]:
    """Return a list of problems; empty means healthy."""
    if manifest is None:
        return ["_manifest.json is missing: the pipeline has never published, or the bucket is wrong"]

    problems = []
    published = datetime.fromisoformat(manifest["published_at"])
    age_hours = (now - published).total_seconds() / 3600
    if age_hours > max_age_hours:
        problems.append(f"last publish was {age_hours:.0f} h ago (limit {max_age_hours:.0f} h)")

    through = manifest.get("weather_through")
    if not through:
        problems.append("weather_through is missing from the manifest")
    else:
        lag = (now.date() - date.fromisoformat(through[:10])).days
        if lag > max_weather_lag_days:
            problems.append(f"weather data stops at {through[:10]}, {lag} days behind (limit {max_weather_lag_days})")

    for table, meta in sorted(manifest.get("marts", {}).items()):
        if not meta.get("rows"):
            problems.append(f"mart {table} is empty")
    return problems


def load_manifest(s3, bucket: str) -> dict | None:
    try:
        obj = s3.get_object(Bucket=bucket, Key="_manifest.json")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise
    return json.loads(obj["Body"].read())


def handler(event=None, context=None, *, s3=None, sns=None, cloudwatch=None, now: datetime | None = None) -> dict:
    s3 = s3 or boto3.client("s3")
    sns = sns or boto3.client("sns")
    cloudwatch = cloudwatch or boto3.client("cloudwatch")
    now = now or datetime.now(UTC)

    bucket = os.environ["BUCKET"]
    max_age = float(os.environ.get("MAX_AGE_HOURS", "36"))
    max_lag = int(os.environ.get("MAX_WEATHER_LAG_DAYS", "7"))

    manifest = load_manifest(s3, bucket)
    problems = check(manifest, now, max_age, max_lag)

    metrics = [{"MetricName": "Healthy", "Value": 0.0 if problems else 1.0, "Unit": "Count"}]
    if manifest:
        age = (now - datetime.fromisoformat(manifest["published_at"])).total_seconds() / 3600
        metrics.append({"MetricName": "HoursSincePublish", "Value": round(age, 2), "Unit": "None"})
        for table, meta in manifest.get("marts", {}).items():
            metrics.append(
                {
                    "MetricName": "MartRows",
                    "Dimensions": [{"Name": "Table", "Value": table}],
                    "Value": float(meta.get("rows", 0)),
                    "Unit": "Count",
                }
            )
    cloudwatch.put_metric_data(Namespace="Fenders", MetricData=metrics)

    if problems:
        sns.publish(
            TopicArn=os.environ["TOPIC_ARN"],
            Subject="Fenders pipeline needs attention",
            Message="Problems found in s3://{}:\n\n- {}".format(bucket, "\n- ".join(problems)),
        )
    return {"healthy": not problems, "problems": problems}
