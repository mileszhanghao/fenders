"""Publish the dashboard data and the analysis marts to S3.

    python -m fenders.publish_s3 --db transform/fenders.duckdb --site site/data --bucket my-fenders-bucket

Layout in the bucket:
    dashboard/<file>                         the same JSON/CSV files the web dashboard reads
    marts/<table>/dt=<YYYY-MM-DD>/<table>.parquet   a dated snapshot of each mart (Hive-style partition)
    marts/<table>/latest/<table>.parquet     the newest snapshot, for tools that just want "now"
    _manifest.json                           what was published, when, with row counts and checksums

The Lambda in aws/freshness reads _manifest.json to check that the pipeline is still running.
Credentials come from the environment (in CI: GitHub's OIDC token exchanged for a short-lived role).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import mimetypes
from datetime import UTC, datetime
from pathlib import Path

import boto3
import duckdb

log = logging.getLogger("fenders.publish_s3")

MARTS = ("daily_counts", "fct_hourly", "rain_effect")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def export_marts(db: Path, out: Path) -> dict[str, dict]:
    """Write each mart to Parquet; return row counts."""
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db), read_only=True)
    info = {}
    for table in MARTS:
        path = out / f"{table}.parquet"
        con.execute(f"copy (select * from {table}) to '{path}' (format parquet, compression zstd)")
        info[table] = {"rows": con.execute(f"select count(*) from {table}").fetchone()[0]}
    con.close()
    return info


def publish(db: Path, site: Path, bucket: str, work: Path, s3=None, now: datetime | None = None) -> dict:
    s3 = s3 or boto3.client("s3")
    now = now or datetime.now(UTC)
    day = now.date().isoformat()

    uploaded: list[dict] = []

    def put(path: Path, key: str) -> None:
        ctype = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        s3.upload_file(str(path), bucket, key, ExtraArgs={"ContentType": ctype})
        uploaded.append({"key": key, "bytes": path.stat().st_size, "sha256": sha256(path)})

    for path in sorted(p for p in site.rglob("*") if p.is_file()):
        put(path, f"dashboard/{path.relative_to(site).as_posix()}")

    marts = export_marts(db, work)
    for table, meta in marts.items():
        path = work / f"{table}.parquet"
        put(path, f"marts/{table}/dt={day}/{table}.parquet")
        put(path, f"marts/{table}/latest/{table}.parquet")
        meta["sha256"] = sha256(path)

    summary_path = site / "summary.json"
    summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}
    manifest = {
        "published_at": now.isoformat(timespec="seconds"),
        "pipeline_generated_at": summary.get("generated_at"),
        "weather_through": summary.get("weather_through"),
        "marts": marts,
        "files": uploaded,
    }
    s3.put_object(
        Bucket=bucket,
        Key="_manifest.json",
        Body=json.dumps(manifest, indent=2).encode(),
        ContentType="application/json",
    )
    log.info("published %d files and %d marts to s3://%s", len(uploaded), len(marts), bucket)
    return manifest


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", type=Path, required=True)
    p.add_argument("--site", type=Path, required=True, help="the dashboard data folder, e.g. site/data")
    p.add_argument("--bucket", required=True)
    p.add_argument("--work", type=Path, default=Path("build/marts"))
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    publish(args.db, args.site, args.bucket, args.work)


if __name__ == "__main__":
    main()
