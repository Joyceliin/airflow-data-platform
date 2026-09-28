"""Testes unitários dos helpers do relatório de ops (sem Airflow/MinIO)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

DAGS_OPS = Path(__file__).resolve().parents[2] / "dags"
if str(DAGS_OPS) not in sys.path:
    sys.path.insert(0, str(DAGS_OPS))

from ops._ops_report import (  # noqa: E402
    classify_trigger,
    failure_rows,
    format_bytes,
    format_growth_pct,
    minio_rows_from_snapshot,
    resolve_report_params,
)


@pytest.mark.parametrize(
    "run_type,external,triggered_by,expected",
    [
        ("scheduled", False, None, "agendado"),
        ("manual", False, None, "manual"),
        ("MANUAL", True, None, "manual"),
        ("backfill", False, None, "backfill"),
        ("dataset_triggered", False, None, "dataset"),
        ("asset_triggered", False, None, "asset"),
        ("", True, None, "manual"),
        ("", False, None, "outro"),
        ("", False, "ui", "manual"),
        ("custom", False, None, "custom"),
    ],
)
def test_classify_trigger(
    run_type: str, external: bool, triggered_by: str | None, expected: str
) -> None:
    assert (
        classify_trigger(run_type, external_trigger=external, triggered_by=triggered_by)
        == expected
    )


def test_format_bytes() -> None:
    assert format_bytes(0) == "0 B"
    assert format_bytes(1024) == "1.0 KiB"
    assert format_bytes(1024**3) == "1.0 GiB"


def test_format_growth_pct() -> None:
    assert format_growth_pct(None) == "sem base anterior"
    assert format_growth_pct(12.34) == "+12.3%"
    assert format_growth_pct(-5.0) == "-5.0%"


def test_minio_rows_marks_growth_warn() -> None:
    rows, summary = minio_rows_from_snapshot(
        {
            "ts": "2026-09-21T10:00:00+00:00",
            "alert_count": 1,
            "buckets": {
                "s3a://bronze": {
                    "total_bytes": 2 * 1024**3,
                    "object_count": 100,
                    "growth_pct": 55.0,
                    "loose_pct": 2.5,
                },
                "s3a://prata": {
                    "total_bytes": 512 * 1024**2,
                    "object_count": 10,
                    "growth_pct": 5.0,
                },
            },
        },
        growth_warn_pct=40.0,
    )
    assert summary["growth_warns"] == 1
    assert summary["bucket_count"] == 2
    assert any("acima do limiar" in detail for _, detail in rows)
    assert any("s3a://bronze" in label for label, _ in rows)


def test_failure_rows_counts() -> None:
    runs = [
        {"dag_id": "a", "run_id": "manual__1", "run_type": "manual", "end_date": "t1"},
        {"dag_id": "b", "run_id": "sched__1", "run_type": "scheduled", "end_date": "t2"},
        {"dag_id": "c", "run_id": "bf__1", "run_type": "backfill", "end_date": "t3"},
    ]
    rows, counts = failure_rows(runs, max_rows=10)
    assert counts == {"manual": 1, "agendado": 1, "outro": 1}
    assert len(rows) == 3
    assert rows[0][0].startswith("Falha manual")


def test_resolve_report_params_priority() -> None:
    params = resolve_report_params(
        {"report": {"lookback_hours": 12}, "thresholds": {"bucket_growth_pct": 30}},
        conf={"lookback_hours": 48},
    )
    assert params["lookback_hours"] == 48
    assert params["growth_warn_pct"] == 30.0
    assert params["api_conn_id"] == "airflow_api"
    assert "ops_airflow_failures_report" in params["exclude_dag_ids"]
    assert "ops_minio_growth_report" in params["exclude_dag_ids"]


def test_connection_base_url() -> None:
    from ops._ops_report import _connection_base_url

    class _C:
        def __init__(self, host, schema=None, port=None):
            self.host = host
            self.schema = schema
            self.port = port

    assert _connection_base_url(_C("http://airflow-webserver:8080")) == (
        "http://airflow-webserver:8080"
    )
    assert _connection_base_url(_C("airflow-webserver", "http", 8080)) == (
        "http://airflow-webserver:8080"
    )
