"""DAG: saúde de buckets MinIO e tabelas Delta (sem Spark)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import dataops_core
from airflow import DAG
from airflow.operators.python import PythonOperator
from dataops_core.delta import (
    bucket_usage,
    evaluate_health,
    growth_pct,
    resolve_targets,
    table_stats,
)
from dataops_core.notify import ExecucaoMetric, failure_callback, send_execucao_email, send_quality_email

from ops._ops_delta import (
    OPS_LABEL,
    apply_minio_for_bucket,
    bucket_from_uri,
    bucket_key,
    load_health_snapshot,
    load_ops_config,
    save_health_snapshot,
)

dataops_core.require("1.0.0")

default_args = {
    "owner": "data-ops",
    "email_on_failure": False,
    "on_failure_callback": failure_callback(label=OPS_LABEL),
    "retries": 1,
}


def _run_health(**context: Any) -> dict[str, Any]:
    config = load_ops_config()
    targets = resolve_targets(config)
    previous = load_health_snapshot()
    prev_buckets = previous.get("buckets") or {}
    prev_tables = previous.get("tables") or {}

    bucket_rows: list[dict[str, Any]] = []
    scan_alerts: list[dict[str, Any]] = []
    discovered: list[str] = []
    seen_uris: set[str] = set(targets["table_uris"])
    object_total = 0

    for bucket in targets["buckets"]:
        try:
            apply_minio_for_bucket(bucket)
            usage = bucket_usage(bucket)
        except Exception as exc:  # noqa: BLE001
            key = bucket_key(bucket)
            print(f"[DELTA-HEALTH] bucket falhou {key}: {exc}", flush=True)
            scan_alerts.append(
                {
                    "severity": "warn",
                    "code": "bucket_scan_error",
                    "target": key,
                    "message": str(exc)[:320],
                }
            )
            continue
        key = bucket_key(usage["bucket"], usage.get("prefix") or "")
        prev_bytes = (prev_buckets.get(key) or {}).get("total_bytes")
        usage["growth_pct"] = growth_pct(usage["total_bytes"], prev_bytes)
        bucket_rows.append(usage)
        object_total += int(usage.get("object_count") or 0)
        for uri in usage.get("delta_uris") or []:
            if uri not in seen_uris:
                seen_uris.add(uri)
                discovered.append(uri)

    table_uris = list(targets["table_uris"]) + discovered
    table_rows: list[dict[str, Any]] = []
    for uri in table_uris:
        try:
            apply_minio_for_bucket(bucket_from_uri(uri))
            stats = table_stats(uri)
        except Exception as exc:  # noqa: BLE001
            print(f"[DELTA-HEALTH] tabela falhou {uri}: {exc}", flush=True)
            scan_alerts.append(
                {
                    "severity": "warn",
                    "code": "table_scan_error",
                    "target": uri,
                    "message": str(exc)[:320],
                }
            )
            continue
        prev_bytes = (prev_tables.get(uri) or {}).get("size_bytes")
        stats["growth_pct"] = growth_pct(stats.get("size_bytes"), prev_bytes)
        table_rows.append(stats)

    metrics = {"buckets": bucket_rows, "tables": table_rows}
    alerts = scan_alerts + evaluate_health(metrics, targets["thresholds"])

    print(
        f"[DELTA-HEALTH] buckets={targets['buckets']} "
        f"objects={object_total} tables={len(table_rows)} "
        f"(config={len(targets['table_uris'])} discovered={len(discovered)}) "
        f"alerts={len(alerts)}",
        flush=True,
    )
    if alerts:
        by_code: dict[str, int] = {}
        for alert in alerts:
            code = str(alert.get("code") or "unknown")
            by_code[code] = by_code.get(code, 0) + 1
        print(
            "[DELTA-HEALTH] alertas por código: "
            + ", ".join(f"{code}={count}" for code, count in sorted(by_code.items())),
            flush=True,
        )
        for i, alert in enumerate(alerts, start=1):
            print(
                f"[DELTA-HEALTH][ALERTA {i}/{len(alerts)}] "
                f"severity={alert.get('severity')} code={alert.get('code')} "
                f"target={alert.get('target')} | {alert.get('message')}",
                flush=True,
            )
    now = datetime.now(timezone.utc).isoformat()
    snapshot = {
        "ts": now,
        "buckets": {
            bucket_key(row["bucket"], row.get("prefix") or ""): {
                "total_bytes": row["total_bytes"],
                "object_count": row["object_count"],
                "growth_pct": row.get("growth_pct"),
                "loose_bytes": row.get("loose_bytes"),
                "loose_pct": row.get("loose_pct"),
                "ts": now,
            }
            for row in bucket_rows
        },
        "tables": {
            row["path"]: {
                "size_bytes": row.get("size_bytes") or 0,
                "num_files": row.get("num_files") or 0,
                "history_len": row.get("history_len") or 0,
                "growth_pct": row.get("growth_pct"),
                "ts": now,
            }
            for row in table_rows
        },
        "alert_count": len(alerts),
        "alerts": [
            {
                "severity": a.get("severity"),
                "code": a.get("code"),
                "target": a.get("target"),
                "message": a.get("message"),
            }
            for a in alerts[:40]
        ],
    }
    save_health_snapshot(snapshot)

    ti = context["ti"]
    ti.xcom_push(key="alert_count", value=len(alerts))
    ti.xcom_push(key="bucket_count", value=len(bucket_rows))
    ti.xcom_push(key="table_count", value=len(table_rows))
    ti.xcom_push(key="object_count", value=object_total)

    n_buckets = len(bucket_rows)
    n_tables = len(table_rows)
    n_alerts = len(alerts)
    summary_metrics = [
        ExecucaoMetric("Buckets", str(n_buckets)),
        ExecucaoMetric("Tabelas", str(n_tables)),
        ExecucaoMetric("Alertas", str(n_alerts)),
    ]

    if n_alerts:
        send_quality_email(
            context,
            rows=[(a["target"], f"[{a['code']}] {a['message']}") for a in alerts],
            intro=(
                f"Buckets: {n_buckets} · Tabelas: {n_tables} · Alertas: {n_alerts}. "
                "Detalhe dos alertas abaixo."
            ),
            label=OPS_LABEL,
            max_rows=max(n_alerts, 12),
        )
    else:
        send_execucao_email(
            context,
            intro="Saúde Delta/MinIO dentro dos limiares configurados.",
            metrics=summary_metrics,
            label=OPS_LABEL,
        )
    return {
        "alerts": n_alerts,
        "buckets": n_buckets,
        "objects": object_total,
        "tables": n_tables,
    }


with DAG(
    dag_id="ops_delta_health",
    description="Inventário e alertas de crescimento/fragmentação Delta + buckets MinIO",
    default_args=default_args,
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ops", "delta", "maintenance"],
    max_active_runs=1,
) as dag:
    PythonOperator(
        task_id="delta_health_scan",
        python_callable=_run_health,
    )
