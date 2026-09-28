"""Helpers do relatório diário de ops (MinIO + falhas Airflow)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

REPORT_LABEL = "OpsReport"
DEFAULT_LOOKBACK_HOURS = 24
DEFAULT_API_CONN_ID = "airflow_api"
DEFAULT_EXCLUDE_DAG_IDS = (
    "ops_airflow_failures_report",
    "ops_minio_growth_report",
)

# run_type Airflow → rótulo operacional
_TRIGGER_LABELS = {
    "scheduled": "agendado",
    "manual": "manual",
    "backfill": "backfill",
    "dataset_triggered": "dataset",
    "asset_triggered": "asset",
}

# AF3 triggered_by → trata como disparo manual quando run_type não ajuda
_MANUAL_TRIGGERED_BY = frozenset(
    {
        "ui",
        "rest_api",
        "cli",
        "operator",
    }
)


def classify_trigger(
    run_type: str | None,
    *,
    external_trigger: bool = False,
    triggered_by: str | None = None,
) -> str:
    """Classifica o gatilho: manual | agendado | backfill | dataset | asset | outro."""
    rt = str(run_type or "").strip().lower()
    if rt in _TRIGGER_LABELS:
        return _TRIGGER_LABELS[rt]
    tb = str(triggered_by or "").strip().lower()
    if external_trigger or tb in _MANUAL_TRIGGERED_BY:
        return "manual"
    return "outro" if not rt else rt


def format_bytes(n: int | float | None) -> str:
    """Bytes → string legível (KiB/MiB/GiB/TiB)."""
    if n is None:
        return "?"
    value = float(n)
    if value < 0:
        value = 0.0
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    idx = 0
    while value >= 1024.0 and idx < len(units) - 1:
        value /= 1024.0
        idx += 1
    if idx == 0:
        return f"{int(value)} {units[idx]}"
    return f"{value:.1f} {units[idx]}"


def format_growth_pct(growth: float | None) -> str:
    if growth is None:
        return "sem base anterior"
    sign = "+" if growth > 0 else ""
    return f"{sign}{growth:.1f}%"


def resolve_report_params(
    config: Mapping[str, Any] | None = None,
    *,
    conf: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Prioridade: conf do trigger → Variable `delta_ops.report` → defaults."""
    cfg = dict(config or {})
    report = dict(cfg.get("report") or {})
    conf = dict(conf or {})

    raw_hours = conf.get("lookback_hours")
    if raw_hours is None:
        raw_hours = report.get("lookback_hours", DEFAULT_LOOKBACK_HOURS)
    lookback_hours = max(1, int(raw_hours))

    raw_exclude = conf.get("exclude_dag_ids")
    if raw_exclude is None:
        raw_exclude = report.get("exclude_dag_ids", list(DEFAULT_EXCLUDE_DAG_IDS))
    if isinstance(raw_exclude, str):
        exclude = [x.strip() for x in raw_exclude.split(",") if x.strip()]
    else:
        exclude = [str(x).strip() for x in (raw_exclude or []) if str(x).strip()]

    include_success = conf.get("include_success")
    if include_success is None:
        include_success = report.get("include_success", False)

    max_failures = conf.get("max_failures")
    if max_failures is None:
        max_failures = report.get("max_failures", 40)
    max_failures = max(1, int(max_failures))

    growth_warn_pct = conf.get("growth_warn_pct")
    if growth_warn_pct is None:
        thresholds = dict(cfg.get("thresholds") or {})
        growth_warn_pct = report.get(
            "growth_warn_pct",
            thresholds.get("bucket_growth_pct", 40),
        )
    growth_warn_pct = float(growth_warn_pct)

    api_conn_id = conf.get("api_conn_id")
    if api_conn_id is None:
        api_conn_id = report.get("api_conn_id", DEFAULT_API_CONN_ID)
    api_conn_id = str(api_conn_id).strip() or DEFAULT_API_CONN_ID

    return {
        "lookback_hours": lookback_hours,
        "exclude_dag_ids": exclude,
        "include_success": bool(include_success),
        "max_failures": max_failures,
        "growth_warn_pct": growth_warn_pct,
        "api_conn_id": api_conn_id,
    }


def minio_rows_from_snapshot(
    snapshot: Mapping[str, Any] | None,
    *,
    growth_warn_pct: float = 40.0,
) -> tuple[list[tuple[str, str]], dict[str, Any]]:
    """Linhas (rótulo, detalhe) + resumo a partir do snapshot de health."""
    snap = dict(snapshot or {})
    buckets = dict(snap.get("buckets") or {})
    rows: list[tuple[str, str]] = []
    warn_count = 0
    total_bytes = 0
    total_objects = 0

    for key in sorted(buckets.keys()):
        info = dict(buckets.get(key) or {})
        nbytes = int(info.get("total_bytes") or 0)
        nobj = int(info.get("object_count") or 0)
        growth = info.get("growth_pct")
        total_bytes += nbytes
        total_objects += nobj
        growth_txt = format_growth_pct(growth if isinstance(growth, (int, float)) else None)
        flag = ""
        if isinstance(growth, (int, float)) and float(growth) >= growth_warn_pct:
            flag = " [acima do limiar]"
            warn_count += 1
        loose = info.get("loose_pct")
        loose_txt = ""
        if isinstance(loose, (int, float)) and float(loose) > 0:
            loose_txt = f"; loose={float(loose):.1f}%"
        rows.append(
            (
                f"MinIO {key}",
                f"{format_bytes(nbytes)} · {nobj} objs · Δ {growth_txt}{loose_txt}{flag}",
            )
        )

    ts = str(snap.get("ts") or "")
    alert_count = int(snap.get("alert_count") or len(snap.get("alerts") or []) or 0)
    summary = {
        "bucket_count": len(buckets),
        "total_bytes": total_bytes,
        "total_objects": total_objects,
        "growth_warns": warn_count,
        "health_alert_count": alert_count,
        "snapshot_ts": ts,
    }
    return rows, summary


def failure_rows(
    runs: Sequence[Mapping[str, Any]],
    *,
    max_rows: int = 40,
) -> tuple[list[tuple[str, str]], dict[str, int]]:
    """Linhas de falha classificadas + contadores por gatilho."""
    counts: dict[str, int] = {"manual": 0, "agendado": 0, "outro": 0}
    rows: list[tuple[str, str]] = []

    for run in runs:
        trigger = classify_trigger(
            run.get("run_type"),
            external_trigger=bool(run.get("external_trigger")),
            triggered_by=str(run.get("triggered_by") or "") or None,
        )
        bucket = trigger if trigger in ("manual", "agendado") else "outro"
        counts[bucket] = counts.get(bucket, 0) + 1
        if len(rows) >= max_rows:
            continue
        dag_id = str(run.get("dag_id") or "?")
        run_id = str(run.get("run_id") or "?")
        ended = run.get("end_date") or run.get("start_date") or ""
        if hasattr(ended, "isoformat"):
            ended = ended.isoformat()
        rows.append(
            (
                f"Falha {trigger} · {dag_id}",
                f"run_id={run_id} · fim={ended}",
            )
        )

    if len(runs) > max_rows:
        rows.append(
            (
                "Falhas (truncado)",
                f"exibindo {max_rows} de {len(runs)}; veja Airflow UI / metadados",
            )
        )
    return rows, counts


def _connection_base_url(conn: Any) -> str:
    host = str(getattr(conn, "host", None) or "").strip().rstrip("/")
    if host.startswith("http://") or host.startswith("https://"):
        return host
    schema = str(getattr(conn, "schema", None) or "http").strip().strip(":/") or "http"
    port = getattr(conn, "port", None)
    port_txt = f":{port}" if port else ""
    if not host:
        raise ValueError("Connection sem host — defina host do API server Airflow")
    return f"{schema}://{host}{port_txt}"


def _get_airflow_connection(conn_id: str) -> Any:
    try:
        from airflow.sdk import BaseHook
    except ImportError:  # pragma: no cover — AF2 fallback
        from airflow.hooks.base import BaseHook

    return BaseHook.get_connection(conn_id)


def _airflow_api_token(*, base_url: str, username: str, password: str) -> str:
    import requests

    url = f"{base_url.rstrip('/')}/auth/token"
    resp = requests.post(
        url,
        json={"username": username, "password": password},
        headers={"Content-Type": "application/json"},
        timeout=60,
    )
    if resp.status_code not in {200, 201}:
        raise RuntimeError(
            f"Falha ao obter JWT em {url}: HTTP {resp.status_code} "
            f"(verifique Connection '{DEFAULT_API_CONN_ID}' login/senha e rate limit)"
        )
    payload = resp.json() if resp.content else {}
    token = payload.get("access_token") if isinstance(payload, dict) else None
    if not token:
        raise RuntimeError(f"Resposta de {url} sem access_token")
    return str(token)


def query_failed_dag_runs(
    *,
    since: datetime,
    exclude_dag_ids: Sequence[str] | None = None,
    api_conn_id: str = DEFAULT_API_CONN_ID,
    page_size: int = 100,
) -> list[dict[str, Any]]:
    """Lista DagRuns failed via API REST v2 (Airflow 3 — sem ORM/metadata DB).

    Requer Connection HTTP (`api_conn_id`, default `airflow_api`) com
    login/senha de usuário que tenha leitura de DAG runs. Host interno típico:
    `http://airflow-webserver:8080`.
    """
    import requests

    exclude = {str(x) for x in (exclude_dag_ids or []) if str(x).strip()}
    if since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)
    since_iso = since.isoformat()

    conn = _get_airflow_connection(api_conn_id)
    base_url = _connection_base_url(conn)
    username = str(conn.login or "").strip()
    password = str(conn.password or "")
    if not username or not password:
        raise RuntimeError(
            f"Connection '{api_conn_id}' precisa de login e password "
            "(usuário Airflow com permissão de leitura de DAG runs)"
        )

    token = _airflow_api_token(base_url=base_url, username=username, password=password)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    list_url = f"{base_url.rstrip('/')}/api/v2/dags/~/dagRuns"

    out: list[dict[str, Any]] = []
    offset = 0
    page_size = max(1, min(int(page_size), 100))
    while True:
        params: list[tuple[str, str | int]] = [
            ("state", "failed"),
            ("start_date_gte", since_iso),
            ("order_by", "-start_date"),
            ("limit", page_size),
            ("offset", offset),
        ]
        resp = requests.get(list_url, headers=headers, params=params, timeout=120)
        if resp.status_code == 401:
            raise RuntimeError(
                f"API {list_url} retornou 401 — token/credenciais inválidos "
                f"(Connection '{api_conn_id}')"
            )
        if resp.status_code >= 400:
            raise RuntimeError(
                f"API {list_url} HTTP {resp.status_code}: {resp.text[:320]}"
            )
        payload = resp.json() if resp.content else {}
        batch = payload.get("dag_runs") if isinstance(payload, dict) else None
        if not isinstance(batch, list):
            batch = []
        for run in batch:
            if not isinstance(run, dict):
                continue
            dag_id = str(run.get("dag_id") or "")
            if dag_id in exclude:
                continue
            run_type = run.get("run_type")
            if hasattr(run_type, "value"):
                run_type = run_type.value
            triggered_by = run.get("triggered_by")
            if hasattr(triggered_by, "value"):
                triggered_by = triggered_by.value
            out.append(
                {
                    "dag_id": dag_id,
                    "run_id": str(run.get("dag_run_id") or run.get("run_id") or ""),
                    "run_type": str(run_type or ""),
                    "triggered_by": str(triggered_by or ""),
                    "external_trigger": False,
                    "start_date": run.get("start_date"),
                    "end_date": run.get("end_date"),
                    "state": str(run.get("state") or ""),
                }
            )
        if len(batch) < page_size:
            break
        offset += page_size
        # Segurança: evita loop infinito se a API ignorar offset
        if offset > 10_000:
            break
    return out
