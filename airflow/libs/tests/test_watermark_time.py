"""Contrato de tempo e watermark (origem + cursor; não logs de aplicação)."""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from dataops_core.clean.dates import as_sp, now_sp, sp_to_lake_ntz, sp_to_utc
from dataops_core.watermark import max_ts

TZ_SP = ZoneInfo("America/Sao_Paulo")


def test_as_sp_localizes_naive_as_sao_paulo_not_utc():
    naive = datetime(2026, 1, 15, 12, 0, 0)
    aware = as_sp(naive)
    assert aware is not None and aware.tzinfo is not None
    assert aware.tzinfo == TZ_SP
    assert aware.hour == 12


def test_sp_to_utc_never_labels_naive_as_utc():
    """replace(tzinfo=UTC) em naive atrasaria −3h; sp_to_utc localiza SP antes."""
    naive = datetime(2026, 6, 1, 15, 0, 0)
    utc = sp_to_utc(naive)
    assert utc is not None
    assert utc.tzinfo == timezone.utc
    # 15h SP (sem DST desde 2019) = 18h UTC
    assert utc.hour == 18


def test_now_sp_is_aware():
    assert now_sp().tzinfo is not None


def test_sp_to_lake_ntz_strips_tz_keeping_wallclock():
    aware = datetime(2026, 3, 1, 10, 30, tzinfo=TZ_SP)
    ntz = sp_to_lake_ntz(aware)
    assert ntz is not None and ntz.tzinfo is None
    assert ntz.hour == 10 and ntz.minute == 30


def test_max_ts_returns_aware_sp():
    series = pd.Series(
        [
            datetime(2026, 1, 1, 8, 0, 0),
            datetime(2026, 1, 2, 9, 0, 0),
        ]
    )
    peak = max_ts(series)
    assert peak is not None
    assert peak.tzinfo is not None
    assert peak.day == 2 and peak.hour == 9


def test_max_ts_single_datetime():
    peak = max_ts(datetime(2026, 5, 5, 11, 0, tzinfo=TZ_SP))
    assert peak is not None and peak.tzinfo == TZ_SP
