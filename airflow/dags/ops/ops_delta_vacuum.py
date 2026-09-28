"""DAG: vacuum Delta agendado (delta-rs). Retenção padrão de 7 dias (168h)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import dataops_core
from airflow import DAG
from airflow.operators.python import PythonOperator
from dataops_core.delta import resolve_targets, vacuum
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


def _run_vacuum(**context: Any) -> dict[str, Any]:
    config = load_ops_config()
    targets = resolve_targets(config)
    conf = (context.get("dag_run").conf if context.get("dag_run") else None) or {}

    # Retenção: conf do trigger → Variable → 168h.
    retention = int(
        conf.get("retention_hours", targets["vacuum"].get("retention_hours", 168))
    )
    dry_run = bool(conf.get("dry_run", targets["vacuum"].get("dry_run", False)))
    table_uris = resolve_ops_table_uris(config, conf=conf, log_prefix="[VACUUM]")

    print(
        f"[VACUUM] retention_hours={retention} ({retention / 24:g} dias) "
        f"dry_run={dry_run} tabelas={len(table_uris)}",
        flush=True,
    )

    results: list[tuple[str, int | None, str | None]] = []
    removed_total = 0
    for uri in table_uris:
        try:
            apply_minio_for_bucket(bucket_from_uri(uri))
            removed = vacuum(uri, retention_hours=retention, dry_run=dry_run)
            results.append((uri, removed, None))
            removed_total += int(removed or 0)
        except Exception as exc:  # noqa: BLE001
            results.append((uri, None, str(exc)))
            print(f"[VACUUM][erro] {uri}: {exc}", flush=True)

    failures = [(uri, err) for uri, _n, err in results if err]
    ok_count = len(results) - len(failures)

    if failures:
        rows = [(uri, err[:320]) for uri, err in failures]
        send_quality_email(
            context,
            rows=rows,
            intro=(
                f"Vacuum parcial: {ok_count} ok, {len(failures)} falha(s); "
                f"retention={retention}h dry_run={dry_run}; "
                f"arquivos removidos/candidatos={removed_total}."
            ),
            label=OPS_LABEL,
        )
    else:
        mode = "dry-run" if dry_run else "aplicado"
        send_execucao_email(
            context,
            intro=(
                f"Vacuum {mode} concluído em {ok_count} tabela(s) "
                f"(retention={retention}h)."
            ),
            metrics=[
                ExecucaoMetric("Tabelas", str(ok_count)),
                ExecucaoMetric("Arquivos", str(removed_total)),
                ExecucaoMetric("Modo", mode),
            ],
            label=OPS_LABEL,
        )

    return {
        "ok": ok_count,
        "failures": len(failures),
        "removed": removed_total,
        "dry_run": dry_run,
    }


with DAG(
    dag_id="ops_delta_vacuum",
    description=(
        "Vacuum Delta (padrão 168h). "
        "Trigger conf: uri_prefix, table_uris, retention_hours, dry_run, only_listed_uris"
    ),
    default_args=default_args,
    schedule="0 3 * * 0",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ops", "delta", "maintenance"],
    max_active_runs=1,
) as dag:
    PythonOperator(
        task_id="delta_vacuum_all",
        python_callable=_run_vacuum,
    )
