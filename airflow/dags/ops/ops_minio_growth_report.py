"""DAG: relatório de crescimento MinIO — segunda e sexta 14h."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import dataops_core
from airflow import DAG
from airflow.operators.python import PythonOperator
from dataops_core.notify import ExecucaoMetric, failure_callback, send_execucao_email, send_quality_email

from ops._ops_delta import load_health_snapshot, load_ops_config
from ops._ops_report import (
    REPORT_LABEL,
    format_bytes,
    minio_rows_from_snapshot,
    resolve_report_params,
)

dataops_core.require("1.0.0")

default_args = {
    "owner": "data-ops",
    "email_on_failure": False,
    "on_failure_callback": failure_callback(label=REPORT_LABEL),
    "retries": 1,
}


def _run_minio_report(**context: Any) -> dict[str, Any]:
    config = load_ops_config()
    conf = (context.get("dag_run").conf if context.get("dag_run") else None) or {}
    params = resolve_report_params(config, conf=conf)
    snapshot = load_health_snapshot()

    minio_rows, minio_summary = minio_rows_from_snapshot(
        snapshot,
        growth_warn_pct=float(params["growth_warn_pct"]),
    )
    growth_warns = int(minio_summary.get("growth_warns") or 0)
    health_alerts = int(minio_summary.get("health_alert_count") or 0)
    snap_ts = str(minio_summary.get("snapshot_ts") or "ausente")

    print(
        f"[OPS-MINIO-REPORT] buckets={minio_summary.get('bucket_count')} "
        f"minio={format_bytes(minio_summary.get('total_bytes'))} "
        f"growth_warns={growth_warns} health_alerts={health_alerts} "
        f"snapshot_ts={snap_ts}",
        flush=True,
    )

    rows: list[tuple[str, str]] = list(minio_rows)
    if not minio_rows:
        rows.append(
            (
                "MinIO",
                "Snapshot ausente — rode ops_delta_health (diário 06:00) "
                "antes deste relatório.",
            )
        )
    if health_alerts:
        for alert in list((snapshot or {}).get("alerts") or [])[:12]:
            target = str(alert.get("target") or "?")
            code = str(alert.get("code") or "?")
            msg = str(alert.get("message") or "")[:240]
            rows.append((f"Health [{code}] {target}", msg))

    intro = (
        f"MinIO {format_bytes(minio_summary.get('total_bytes'))} "
        f"({minio_summary.get('total_objects')} objs) · "
        f"buckets={minio_summary.get('bucket_count')} · "
        f"crescimento acima limiar: {growth_warns} · "
        f"alertas health: {health_alerts} · "
        f"snapshot={snap_ts}."
    )

    has_issues = growth_warns > 0 or health_alerts > 0 or not minio_rows
    if has_issues:
        send_quality_email(
            context,
            rows=rows,
            intro=intro,
            label=REPORT_LABEL,
            max_rows=max(len(rows), 12),
        )
    else:
        send_execucao_email(
            context,
            intro=intro,
            metrics=[
                ExecucaoMetric("MinIO", format_bytes(minio_summary.get("total_bytes"))),
                ExecucaoMetric("Buckets", str(minio_summary.get("bucket_count") or 0)),
                ExecucaoMetric("Δ acima limiar", "0"),
                ExecucaoMetric("Snapshot", snap_ts[:19] if snap_ts else "—"),
            ],
            label=REPORT_LABEL,
        )

    ti = context["ti"]
    ti.xcom_push(key="growth_warns", value=growth_warns)
    ti.xcom_push(key="minio_bytes", value=int(minio_summary.get("total_bytes") or 0))

    return {
        "growth_warns": growth_warns,
        "minio_bytes": int(minio_summary.get("total_bytes") or 0),
        "health_alerts": health_alerts,
    }


with DAG(
    dag_id="ops_minio_growth_report",
    description=(
        "Relatório de crescimento MinIO (snapshot health) — segunda e sexta 14h"
    ),
    default_args=default_args,
    schedule="0 14 * * 1,5",  # segunda e sexta 14:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ops", "minio", "monitoring", "report"],
    max_active_runs=1,
) as dag:
    PythonOperator(
        task_id="minio_growth_report",
        python_callable=_run_minio_report,
    )
