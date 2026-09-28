"""DAG: compactação de small files (delta-rs OPTIMIZE). Depois rode vacuum (168h)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import dataops_core
from airflow import DAG
from airflow.operators.python import PythonOperator
from dataops_core.delta import optimize_compact
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


def _run_optimize(**context: Any) -> dict[str, Any]:
    config = load_ops_config()
    conf = (context.get("dag_run").conf if context.get("dag_run") else None) or {}
    table_uris = resolve_ops_table_uris(config, conf=conf, log_prefix="[OPTIMIZE]")

    raw_target = conf.get("target_size")
    if raw_target is None:
        raw_target = (config.get("optimize") or {}).get("target_size")
    target_size = int(raw_target) if raw_target is not None else None

    print(
        f"[OPTIMIZE] tabelas={len(table_uris)} target_size={target_size or 'default'}",
        flush=True,
    )

    results: list[tuple[str, dict[str, Any] | None, str | None]] = []
    for uri in table_uris:
        try:
            apply_minio_for_bucket(bucket_from_uri(uri))
            metrics = optimize_compact(uri, target_size=target_size)
            results.append((uri, metrics, None))
        except Exception as exc:  # noqa: BLE001
            results.append((uri, None, str(exc)))
            print(f"[OPTIMIZE][erro] {uri}: {exc}", flush=True)

    failures = [(uri, err) for uri, _m, err in results if err]
    ok_count = len(results) - len(failures)

    if failures:
        send_quality_email(
            context,
            rows=[(uri, err[:320]) for uri, err in failures],
            intro=f"OPTIMIZE parcial: {ok_count} ok, {len(failures)} falha(s).",
            label=OPS_LABEL,
        )
    else:
        send_execucao_email(
            context,
            intro=f"OPTIMIZE concluído em {ok_count} tabela(s).",
            metrics=[ExecucaoMetric("Tabelas", str(ok_count))],
            label=OPS_LABEL,
        )

    return {"ok": ok_count, "failures": len(failures)}


with DAG(
    dag_id="ops_delta_optimize",
    description=(
        "Compacta small files (delta-rs). "
        "Trigger conf: uri_prefix, table_uris, target_size, only_listed_uris. "
        "Após compactar, rode ops_delta_vacuum (168h) para liberar órfãos."
    ),
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ops", "delta", "maintenance", "optimize"],
    max_active_runs=1,
) as dag:
    PythonOperator(
        task_id="delta_optimize_all",
        python_callable=_run_optimize,
    )
