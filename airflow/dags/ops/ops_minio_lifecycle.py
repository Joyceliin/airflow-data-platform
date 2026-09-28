"""DAG: aplica lifecycle MinIO (versões noncurrent) — só manual."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import dataops_core
from airflow import DAG
from airflow.operators.python import PythonOperator
from dataops_core.notify import ExecucaoMetric, failure_callback, send_execucao_email, send_quality_email

from ops._ops_delta import (
    DEFAULT_LIFECYCLE_NONCURRENT_DAYS,
    OPS_LABEL,
    ensure_noncurrent_lifecycle,
    load_ops_config,
    resolve_lifecycle_params,
)

dataops_core.require("1.0.0")

default_args = {
    "owner": "data-ops",
    "email_on_failure": False,
    "on_failure_callback": failure_callback(label=OPS_LABEL),
    "retries": 0,
}


def _run_lifecycle(**context: Any) -> dict[str, Any]:
    conf = dict((context.get("dag_run") and context["dag_run"].conf) or {})
    params = dict(context.get("params") or {})
    targets = resolve_lifecycle_params(load_ops_config(), conf=conf, params=params)
    days = int(targets["noncurrent_days"])
    dry_run = bool(targets["dry_run"])
    buckets: list[str] = list(targets["buckets"])

    print(
        f"[LIFECYCLE] buckets={buckets} noncurrent_days={days} dry_run={dry_run}",
        flush=True,
    )

    results: list[dict[str, Any]] = []
    failures: list[tuple[str, str]] = []
    for bucket in buckets:
        try:
            results.append(
                ensure_noncurrent_lifecycle(
                    bucket,
                    noncurrent_days=days,
                    dry_run=dry_run,
                )
            )
        except Exception as exc:  # noqa: BLE001
            failures.append((bucket, str(exc)))
            print(f"[LIFECYCLE][erro] s3://{bucket}: {exc}", flush=True)

    ok = len(results)
    if failures:
        send_quality_email(
            context,
            rows=[(b, err[:320]) for b, err in failures],
            intro=(
                f"Lifecycle parcial: {ok} ok, {len(failures)} falha(s); "
                f"NoncurrentDays={days} dry_run={dry_run}."
            ),
            label=OPS_LABEL,
        )
    else:
        mode = "dry-run" if dry_run else "aplicado"
        send_execucao_email(
            context,
            intro=(
                f"Lifecycle MinIO {mode} em {ok} bucket(s) "
                f"(NoncurrentDays={days})."
            ),
            metrics=[
                ExecucaoMetric("Buckets", str(ok)),
                ExecucaoMetric("NoncurrentDays", str(days)),
                ExecucaoMetric("Modo", mode),
            ],
            label=OPS_LABEL,
        )

    return {
        "ok": ok,
        "failures": len(failures),
        "noncurrent_days": days,
        "dry_run": dry_run,
        "buckets": buckets,
    }


with DAG(
    dag_id="ops_minio_lifecycle",
    description=(
        "Aplica lifecycle MinIO (expira versões noncurrent). "
        "Manual; default 10 dias — override via conf/Variable. "
        "Requer Connection com permissão de lifecycle."
    ),
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ops", "minio", "lifecycle", "maintenance"],
    max_active_runs=1,
    params={
        "noncurrent_days": DEFAULT_LIFECYCLE_NONCURRENT_DAYS,
        "dry_run": False,
    },
) as dag:
    PythonOperator(
        task_id="ensure_minio_lifecycle",
        python_callable=_run_lifecycle,
    )
