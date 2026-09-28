"""Limpezas e normalizacoes comuns a todos os projetos da plataforma."""

from __future__ import annotations

from dataops_core.clean.dates import (
    as_sp,
    day_window,
    now_sp,
    parse_date_br,
    parse_timestamp_br,
    sp_to_lake_ntz,
    sp_to_utc,
)
from dataops_core.clean.demographics import (
    CD_IGNORADO,
    FX_ETARIA_PADRAO,
    calcular_cd_fx_etaria,
    ds_fx_etaria,
)
from dataops_core.clean.health_ids import (
    normalize_cid10,
    normalize_cnes,
    normalize_cns,
    normalize_cpf,
    validate_cnes,
    validate_cns,
    validate_cpf,
)
from dataops_core.clean.text import (
    collapse_spaces,
    digits_only,
    normalize_text,
    null_if_blank,
    strip_accents,
)

__all__ = [
    "CD_IGNORADO",
    "FX_ETARIA_PADRAO",
    "as_sp",
    "calcular_cd_fx_etaria",
    "collapse_spaces",
    "day_window",
    "digits_only",
    "ds_fx_etaria",
    "normalize_cid10",
    "normalize_cnes",
    "normalize_cns",
    "normalize_cpf",
    "normalize_text",
    "now_sp",
    "null_if_blank",
    "parse_date_br",
    "parse_timestamp_br",
    "sp_to_lake_ntz",
    "sp_to_utc",
    "strip_accents",
    "validate_cnes",
    "validate_cns",
    "validate_cpf",
]
