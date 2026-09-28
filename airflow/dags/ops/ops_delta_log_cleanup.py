"""DAG: enxuga `_delta_log` (checkpoint + cleanup_metadata). Alivia long_history."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import dataops_core
from airflow import DAG
from airflow.operators.python import PythonOperator
from dataops_core.delta import cleanup_log
from dataops_core.notify import ExecucaoMetric, failure_callback, send_execucao_email, send_quality_email

from ops._ops_delta import (
    OPS_LABEL,
    apply_minio_for_bucket,
    bucket_from_uri,
    load_ops_config,
    resolve_ops_table_uris,
)

dataops_core.require("1.0.0")

default_args = {
    "owner": "data-ops",
    "email_on_failure": False,
    "on_failure_callback": failure_callback(label=OPS_LABEL),
    "retries": 0,
}


def _run_log_cleanup(**context: Any) -> dict[str, Any]:
    config = load_ops_config()
    conf = (context.get("dag_run").conf if context.get("dag_run") else None) or {}
    table_uris = resolve_ops_table_uris(config, conf=conf, log_prefix="[LOG-CLEANUP]")

    raw_days = conf.get("log_retention_days")
    if raw_days is None:
        raw_days = (config.get("log_cleanup") or {}).get("log_retention_days")
    log_retention_days = int(raw_days) if raw_days is not None else None

    print(
        f"[LOG-CLEANUP] tabelas={len(table_uris)} "
        f"log_retention_days={log_retention_days or 'table-default/30d'}",
        flush=True,
    )

    results: list[tuple[str, dict[str, Any] | None, str | None]] = []
    for uri in table_uris:
        try:
            apply_minio_for_bucket(bucket_from_uri(uri))
            metrics = cleanup_log(uri, log_retention_days=log_retention_days)
            results.append((uri, metrics, None))
        except Exception as exc:  # noqa: BLE001
            results.append((uri, None, str(exc)))
            print(f"[LOG-CLEANUP][erro] {uri}: {exc}", flush=True)

    failures = [(uri, err) for uri, _m, err in results if err]
    ok_count = len(results) - len(failures)

    if failures:
        send_quality_email(
            context,
            rows=[(uri, err[:320]) for uri, err in failures],
            intro=f"Log cleanup parcial: {ok_count} ok, {len(failures)} falha(s).",
            label=OPS_LABEL,
        )
    else:
        send_execucao_email(
            context,
            intro=f"Log cleanup concluído em {ok_count} tabela(s).",
            metrics=[ExecucaoMetric("Tabelas", str(ok_count))],
            label=OPS_LABEL,
        )

    return {"ok": ok_count, "failures": len(failures)}


with DAG(
    dag_id="ops_delta_log_cleanup",
    description=(
        "Checkpoint + cleanup do _delta_log (long_history). "
        "Trigger conf: uri_prefix, table_uris, log_retention_days, only_listed_uris"
    ),
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ops", "delta", "maintenance", "log"],
    max_active_runs=1,
) as dag:
    PythonOperator(
        task_id="delta_log_cleanup_all",
        python_callable=_run_log_cleanup,
    )
