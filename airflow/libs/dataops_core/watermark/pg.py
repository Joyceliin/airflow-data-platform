"""Control plane PostgreSQL — configuração, execução e watermark.

Contrato:
  - watermark_to = MAX da coluna de negócio da origem, nunca now()
  - to_ts da janela de leitura pode ser now_sp() só como teto de safety
  - Datas de origem/watermark na API = always aware America/Sao_Paulo
  - Persistência em etl_log = timestamptz UTC via sp_to_utc
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

import psycopg2
import psycopg2.extras

from dataops_core.clean.dates import (
    as_sp,
    now_sp,
    series_to_sp_naive_wallclock,
    sp_to_utc,
)

DEFAULT_LOOKBACK_HOURS = 25
SAFETY_OVERLAP_MINUTES = 10

__all__ = [
    "DEFAULT_LOOKBACK_HOURS",
    "SAFETY_OVERLAP_MINUTES",
    "close_run",
    "etl_project",
    "etl_schema",
    "get_last_watermark",
    "get_table_config",
    "list_bronze_tables",
    "max_ts",
    "open_run",
]


def etl_schema() -> str:
    return os.getenv("ETL_SCHEMA", "logs")


def etl_project(*, default: str | None = None) -> str:
    """Resolve ETL_PROJECT; usa `default` quando a variavel nao esta definida."""
    project = (os.getenv("ETL_PROJECT", "") or "").strip()
    if not project and default:
        project = default.strip()
    if not project:
        raise RuntimeError("ETL_PROJECT ausente.")
    return project


def _conn():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def max_ts(values: Any) -> datetime | None:
    """MAX de timestamps de negócio da origem → instante aware America/Sao_Paulo."""
    if values is None:
        return None
    if isinstance(values, datetime):
        return as_sp(values)
    try:
        import pandas as pd

        series = values if isinstance(values, pd.Series) else pd.Series(values)
    except Exception:
        return None
    if series.empty:
        return None
    ts = series_to_sp_naive_wallclock(series).dropna()
    if ts.empty:
        return None
    peak = ts.max()
    if getattr(peak, "__class__", None) and str(peak) == "NaT":
        return None
    try:
        import pandas as pd

        if pd.isna(peak):
            return None
    except Exception:
        pass
    py = peak.to_pydatetime() if hasattr(peak, "to_pydatetime") else peak
    if not isinstance(py, datetime):
        return None
    return as_sp(py)


def get_table_config(table_name: str, project: str | None = None) -> dict[str, Any]:
    proj = project or etl_project()
    with _conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT *
                FROM {etl_schema()}.etl_table_config
                WHERE project = %s AND table_name = %s AND enabled = true
                """,
                (proj, table_name),
            )
            row = cur.fetchone()
    if not row:
        raise ValueError(f"Tabela habilitada não encontrada: {table_name}.")
    return dict(row)


def list_bronze_tables(project: str | None = None) -> list[tuple[str, str]]:
    proj = project or etl_project()
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT table_name, delta_bronze_path
                FROM {etl_schema()}.etl_table_config
                WHERE project = %s AND enabled = true
                  AND delta_bronze_path IS NOT NULL
                  AND TRIM(delta_bronze_path) <> ''
                ORDER BY table_name
                """,
                (proj,),
            )
            return [(row[0], row[1]) for row in cur.fetchall()]


def get_last_watermark(
    table_name: str,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    overlap_minutes: int = SAFETY_OVERLAP_MINUTES,
    project: str | None = None,
) -> dict[str, Any]:
    """Janela de leitura com from_ts/to_ts always aware America/Sao_Paulo."""
    proj = project or etl_project()
    now = now_sp()
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT watermark_to
                FROM {etl_schema()}.etl_log
                WHERE project = %s AND table_name = %s
                  AND status = 'success' AND watermark_to IS NOT NULL
                ORDER BY finished_at DESC
                LIMIT 1
                """,
                (proj, table_name),
            )
            row = cur.fetchone()
    if not row or row[0] is None:
        return {
            "from_ts": now - timedelta(hours=lookback_hours),
            "to_ts": now,
            "first_run": True,
        }
    from_ts = as_sp(row[0])
    assert from_ts is not None
    return {
        "from_ts": from_ts - timedelta(minutes=overlap_minutes),
        "to_ts": now,
        "first_run": False,
    }


def open_run(
    table_name: str,
    watermark_col: str | None = None,
    watermark_from: datetime | None = None,
    extraction_type: str = "incremental",
    run_id: str = "manual",
    *,
    wm_col: str | None = None,
    from_ts: datetime | None = None,
    project: str | None = None,
) -> int:
    """Abre etl_log. Aliases: wm_col / from_ts."""
    col = watermark_col if watermark_col is not None else wm_col
    start = watermark_from if watermark_from is not None else from_ts
    if col is None:
        raise TypeError("open_run exige watermark_col (ou alias wm_col).")
    if start is None:
        raise TypeError("open_run exige watermark_from (ou alias from_ts).")

    proj = project or etl_project()
    wm_from_utc = sp_to_utc(as_sp(start))
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {etl_schema()}.etl_log
                    (project, table_name, watermark_col, extraction_type,
                     status, watermark_from, run_id, started_at)
                VALUES (%s, %s, %s, %s, 'running', %s, %s, NOW())
                RETURNING id
                """,
                (proj, table_name, col, extraction_type, wm_from_utc, run_id),
            )
            log_id = cur.fetchone()[0]
        conn.commit()
    return log_id


def close_run(
    log_id: int,
    status: str,
    rows_extracted: int = 0,
    rows_loaded: int = 0,
    error_message: str | None = None,
    watermark_to: datetime | None = None,
) -> None:
    """Persiste watermark_to (UTC) quando informado. None = não avança o cursor."""
    wm_utc = sp_to_utc(as_sp(watermark_to)) if watermark_to is not None else None
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {etl_schema()}.etl_log
                SET status = %s, finished_at = NOW(),
                    rows_extracted = %s, rows_loaded = %s,
                    error_message = %s, watermark_to = %s
                WHERE id = %s
                """,
                (
                    status,
                    rows_extracted,
                    rows_loaded,
                    error_message,
                    wm_utc,
                    log_id,
                ),
            )
        conn.commit()
