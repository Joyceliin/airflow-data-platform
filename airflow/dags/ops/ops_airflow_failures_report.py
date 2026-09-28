"""DAG: relatório de falhas Airflow (manual × agendado) — fim do dia."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import dataops_core
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from dataops_core.notify import ExecucaoMetric, failure_callback, send_execucao_email, send_quality_email

from ops._ops_delta import load_ops_config
from ops._ops_report import (
    REPORT_LABEL,
    failure_rows,
    query_failed_dag_runs,
    resolve_report_params,
)

dataops_core.require("1.0.0")

default_args = {
    "owner": "data-ops",
    "email_on_failure": False,
    "on_failure_callback": failure_callback(label=REPORT_LABEL),
    "retries": 1,
}


def _run_failures_report(**context: Any) -> dict[str, Any]:
    config = load_ops_config()
    conf = (context.get("dag_run").conf if context.get("dag_run") else None) or {}
    params = resolve_report_params(config, conf=conf)

    lookback = int(params["lookback_hours"])
    since = datetime.now(timezone.utc) - timedelta(hours=lookback)
    failed = query_failed_dag_runs(
        since=since,
        exclude_dag_ids=params["exclude_dag_ids"],
        api_conn_id=str(params["api_conn_id"]),
    )
    fail_rows, fail_counts = failure_rows(
        failed,
        max_rows=int(params["max_failures"]),
    )

    n_manual = int(fail_counts.get("manual") or 0)
    n_sched = int(fail_counts.get("agendado") or 0)
    n_other = int(fail_counts.get("outro") or 0)
    n_fail = n_manual + n_sched + n_other

    print(
        f"[OPS-FAIL-REPORT] lookback={lookback}h since={since.isoformat()} "
        f"falhas={n_fail} (manual={n_manual} agendado={n_sched} outro={n_other})",
        flush=True,
    )

    rows: list[tuple[str, str]] = list(fail_rows)
    if n_fail == 0:
        rows.append(
            (
                "Falhas Airflow",
                f"Nenhuma DagRun failed nas últimas {lookback}h "
                f"(excluídos: {', '.join(params['exclude_dag_ids']) or '—'}).",
            )
        )

    intro = (
        f"Janela {lookback}h · falhas: {n_fail} "
        f"(manual={n_manual}, agendado={n_sched}, outro={n_other})."
    )

    if n_fail > 0:
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
                ExecucaoMetric("Falhas", "0"),
                ExecucaoMetric("Manual", "0"),
                ExecucaoMetric("Agendado", "0"),
                ExecucaoMetric("Janela (h)", str(lookback)),
            ],
            label=REPORT_LABEL,
        )

    ti = context["ti"]
    ti.xcom_push(key="fail_manual", value=n_manual)
    ti.xcom_push(key="fail_scheduled", value=n_sched)
    ti.xcom_push(key="fail_other", value=n_other)

    return {"failures": n_fail, "manual": n_manual, "scheduled": n_sched, "other": n_other}


with DAG(
    dag_id="ops_airflow_failures_report",
    description=(
        "Relatório de fim de dia: DagRuns failed classificadas em manual × agendado"
    ),
    default_args=default_args,
    schedule="0 18 * * *",  # fim do dia operacional
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ops", "airflow", "monitoring", "report"],
    max_active_runs=1,
) as dag:
    PythonOperator(
        task_id="airflow_failures_report",
        python_callable=_run_failures_report,
    )
