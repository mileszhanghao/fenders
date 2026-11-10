"""S3 publishing and the freshness Lambda, against moto's in-memory AWS."""

import json
from datetime import UTC, datetime, timedelta

import boto3
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

from aws.freshness.handler import check, handler
from fenders.publish_s3 import MARTS, publish

NOW = datetime(2025, 11, 10, 15, 0, tzinfo=UTC)


@pytest.fixture
def aws(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-2")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-west-2")
        s3.create_bucket(Bucket="fenders-test", CreateBucketConfiguration={"LocationConstraint": "us-west-2"})
        yield s3


def healthy_manifest(**overrides) -> dict:
    m = {
        "published_at": (NOW - timedelta(hours=8)).isoformat(),
        "weather_through": (NOW - timedelta(days=2)).date().isoformat(),
        "marts": {"daily_counts": {"rows": 10}, "fct_hourly": {"rows": 240}, "rain_effect": {"rows": 4}},
    }
    return m | overrides


# --- publish_s3 -----------------------------------------------------------------------------------------------------


def test_publish_uploads_dashboard_marts_and_manifest(aws, db_path, tmp_path):
    site = tmp_path / "site"
    (site / "tableau").mkdir(parents=True)
    (site / "summary.json").write_text(
        json.dumps({"generated_at": "2025-11-10T14:30", "weather_through": "2025-11-09"})
    )
    (site / "tableau" / "daily.csv").write_text("counter,date,bikes\n")

    manifest = publish(db_path, site, "fenders-test", tmp_path / "work", s3=aws, now=NOW)

    keys = {o["Key"] for o in aws.list_objects_v2(Bucket="fenders-test")["Contents"]}
    assert {"dashboard/summary.json", "dashboard/tableau/daily.csv", "_manifest.json"} <= keys
    for table in MARTS:
        assert f"marts/{table}/dt=2025-11-10/{table}.parquet" in keys
        assert f"marts/{table}/latest/{table}.parquet" in keys
        assert manifest["marts"][table]["rows"] > 0

    # The uploaded Parquet is the real mart, readable by any engine.
    aws.download_file("fenders-test", "marts/rain_effect/latest/rain_effect.parquet", str(tmp_path / "re.parquet"))
    table = pq.read_table(tmp_path / "re.parquet")
    assert table.num_rows == manifest["marts"]["rain_effect"]["rows"]
    assert "change" in table.column_names

    stored = json.loads(aws.get_object(Bucket="fenders-test", Key="_manifest.json")["Body"].read())
    assert stored["weather_through"] == "2025-11-09"
    assert stored["published_at"] == "2025-11-10T15:00:00+00:00"
    assert aws.head_object(Bucket="fenders-test", Key="dashboard/summary.json")["ContentType"] == "application/json"


# --- freshness checks (pure) ----------------------------------------------------------------------------------------


def test_healthy_manifest_has_no_problems():
    assert check(healthy_manifest(), NOW, 36, 7) == []


def test_stale_publish_is_reported():
    m = healthy_manifest(published_at=(NOW - timedelta(hours=50)).isoformat())
    assert check(m, NOW, 36, 7) == ["last publish was 50 h ago (limit 36 h)"]


def test_weather_that_stopped_advancing_is_reported():
    m = healthy_manifest(weather_through="2025-10-30")
    assert check(m, NOW, 36, 7) == ["weather data stops at 2025-10-30, 11 days behind (limit 7)"]


def test_empty_mart_is_reported():
    m = healthy_manifest(marts={"daily_counts": {"rows": 10}, "rain_effect": {"rows": 0}})
    assert check(m, NOW, 36, 7) == ["mart rain_effect is empty"]


def test_missing_manifest_is_a_problem_not_a_crash():
    assert len(check(None, NOW, 36, 7)) == 1


# --- the Lambda end to end ------------------------------------------------------------------------------------------


@pytest.fixture
def lambda_env(aws, monkeypatch):
    sns = boto3.client("sns", region_name="us-west-2")
    topic = sns.create_topic(Name="fenders-alerts")["TopicArn"]
    sqs = boto3.client("sqs", region_name="us-west-2")
    queue = sqs.create_queue(QueueName="inbox")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=queue_arn)
    monkeypatch.setenv("BUCKET", "fenders-test")
    monkeypatch.setenv("TOPIC_ARN", topic)
    cw = boto3.client("cloudwatch", region_name="us-west-2")
    return {"s3": aws, "sns": sns, "cloudwatch": cw, "sqs": sqs, "queue": queue}


def alerts(env) -> list[str]:
    msgs = env["sqs"].receive_message(QueueUrl=env["queue"], MaxNumberOfMessages=10).get("Messages", [])
    return [json.loads(m["Body"])["Message"] for m in msgs]


def run(env):
    return handler({}, None, s3=env["s3"], sns=env["sns"], cloudwatch=env["cloudwatch"], now=NOW)


def test_lambda_healthy_sends_no_alert_and_records_metrics(lambda_env):
    lambda_env["s3"].put_object(Bucket="fenders-test", Key="_manifest.json", Body=json.dumps(healthy_manifest()))
    assert run(lambda_env) == {"healthy": True, "problems": []}
    assert alerts(lambda_env) == []
    names = {m["MetricName"] for m in lambda_env["cloudwatch"].list_metrics(Namespace="Fenders")["Metrics"]}
    assert names == {"Healthy", "HoursSincePublish", "MartRows"}


def test_lambda_alerts_when_pipeline_is_stale(lambda_env):
    stale = healthy_manifest(published_at=(NOW - timedelta(days=3)).isoformat())
    lambda_env["s3"].put_object(Bucket="fenders-test", Key="_manifest.json", Body=json.dumps(stale))
    result = run(lambda_env)
    assert not result["healthy"]
    [message] = alerts(lambda_env)
    assert "last publish was 72 h ago" in message


def test_lambda_alerts_when_nothing_was_ever_published(lambda_env):
    result = run(lambda_env)
    assert not result["healthy"]
    [message] = alerts(lambda_env)
    assert "_manifest.json is missing" in message
