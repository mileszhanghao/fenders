"""Shared fixtures: build the dbt project once against the fixture lake."""

import os
import subprocess
from pathlib import Path

import duckdb
import pytest

from tests.fixture_lake import make

TRANSFORM = Path(__file__).resolve().parent.parent / "transform"


@pytest.fixture(scope="session")
def db_path(tmp_path_factory) -> Path:
    """Path to a DuckDB warehouse built by `dbt build` from the fixture lake."""
    tmp = tmp_path_factory.mktemp("dbt")
    make(tmp / "lake")
    env = os.environ | {"FENDERS_LAKE": str(tmp / "lake"), "FENDERS_DB": str(tmp / "test.duckdb")}
    result = subprocess.run(
        ["dbt", "build", "--profiles-dir", ".", "--target-path", str(tmp / "target"), "--log-path", str(tmp / "logs")],
        cwd=TRANSFORM,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout[-4000:]
    return tmp / "test.duckdb"


@pytest.fixture(scope="session")
def db(db_path):
    con = duckdb.connect(str(db_path), read_only=True)
    yield con
    con.close()
