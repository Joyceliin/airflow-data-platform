"""Datas e janelas de execucao no fuso oficial da plataforma (America/Sao_Paulo)."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

TZ_SP = ZoneInfo("America/Sao_Paulo")

_DATE_FORMATS = ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y%m%d", "%d/%m/%y")
_TIMESTAMP_FORMATS = (
    "%d/%m/%Y %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%Y-%m-%d %H:%M",
)


def now_sp() -> datetime:
    """Agora em America/Sao_Paulo (sempre aware)."""
    return datetime.now(TZ_SP)


def as_sp(ts: datetime | None) -> datetime | None:
    """Garante datetime aware em America/Sao_Paulo.

    Naive e tratado como horario de SP; aware em outro fuso e convertido para SP.
    """
    if ts is None:
        return None
    if not isinstance(ts, datetime):
        raise TypeError(f"Esperado datetime, recebeu {type(ts).__name__}")
    if ts.tzinfo is None:
        return ts.replace(tzinfo=TZ_SP)
    return ts.astimezone(TZ_SP)


def sp_to_utc(ts: datetime | None) -> datetime | None:
    """Aware SP (ou naive tratado como SP) → UTC."""
    aware = as_sp(ts)
    if aware is None:
        return None
    return aware.astimezone(timezone.utc)


def sp_to_lake_ntz(ts: datetime | None) -> datetime | None:
    """Aware SP → naive wall-clock SP só para serialização timestampNtz no Delta."""
    aware = as_sp(ts)
    if aware is None:
        return None
    return aware.replace(tzinfo=None)


def parse_date_br(value: str | date | datetime | None) -> date | None:
    """Converte data em formato BR ou ISO. Retorna None em vez de levantar erro."""
    if value is None or isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = value.strip()
    if not text:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def parse_timestamp_br(value: str | datetime | None) -> datetime | None:
    """Converte timestamp BR ou ISO e ancora no fuso de Sao Paulo (sempre aware)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return as_sp(value)
    text = value.strip()
    if not text:
        return None
    for fmt in _TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=TZ_SP)
        except ValueError:
            continue
    parsed_date = parse_date_br(text)
    if parsed_date is None:
        return None
    return datetime.combine(parsed_date, datetime.min.time(), tzinfo=TZ_SP)


def day_window(reference: datetime | None = None, *, days: int = 1) -> tuple[datetime, datetime]:
    """Janela [inicio, fim) de `days` dias terminando na referencia (aware SP)."""
    end = as_sp(reference) or now_sp()
    return end - timedelta(days=days), end


def series_to_sp_naive_wallclock(series: Any) -> Any:
    """Pandas Series de timestamps → wall-clock SP sem tz (para MAX / comparação)."""
    import pandas as pd

    parsed = pd.to_datetime(series, errors="coerce")
    tz = getattr(parsed.dt, "tz", None) if hasattr(parsed, "dt") else None
    if tz is not None:
        localized = parsed.dt.tz_convert(TZ_SP)
        return pd.to_datetime(
            localized.dt.strftime("%Y-%m-%d %H:%M:%S.%f"),
            errors="coerce",
        )
    return parsed
