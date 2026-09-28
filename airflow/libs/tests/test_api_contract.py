"""Superficie publica da dataops_core: nomes de funcao e de parametro."""

from __future__ import annotations

import importlib
import inspect

import pytest

CONTRACT = [
    (
        "dataops_core.notify",
        "send_failure_email",
        ["context", "texto_relatorio", "variavel_email", "label"],
    ),
    (
        "dataops_core.notify",
        "send_quality_email",
        ["context", "texto_relatorio", "rows", "intro", "variavel_email", "label", "max_rows"],
    ),
    (
        "dataops_core.notify",
        "send_monitoring_email",
        ["context", "texto_relatorio", "variavel_email", "label"],
    ),
    (
        "dataops_core.notify",
        "send_vigilancia_email",
        ["context", "reference_date", "highlights", "intro", "panel_url", "headline", "label"],
    ),
    (
        "dataops_core.notify",
        "send_execucao_email",
        ["context", "intro", "metrics", "headline", "label"],
    ),
    ("dataops_core.notify", "on_failure_callback", ["context"]),
    ("dataops_core.notify", "failure_callback", ["label"]),
    ("dataops_core.clean", "normalize_text", ["value", "upper", "accents"]),
    ("dataops_core.clean", "calcular_cd_fx_etaria", ["idade", "unidade_idade"]),
    ("dataops_core.clean", "validate_cns", ["value"]),
    ("dataops_core.clean", "parse_date_br", ["value"]),
    ("dataops_core.security", "hash_value", ["value"]),
    ("dataops_core.security", "hash_series", ["series"]),
    ("dataops_core.clean", "as_sp", ["ts"]),
    ("dataops_core.clean", "sp_to_utc", ["ts"]),
    ("dataops_core.watermark", "etl_project", ["default"]),
    ("dataops_core.watermark", "get_table_config", ["table_name", "project"]),
    ("dataops_core.watermark", "get_last_watermark", ["table_name", "lookback_hours", "overlap_minutes", "project"]),
    ("dataops_core.watermark", "max_ts", ["values"]),
    ("dataops_core.watermark", "open_run", ["table_name", "watermark_col", "watermark_from", "extraction_type", "run_id", "wm_col", "from_ts", "project"]),
    ("dataops_core.watermark", "close_run", ["log_id", "status", "rows_extracted", "rows_loaded", "error_message", "watermark_to"]),
    ("dataops_core.delta", "delta_uri", ["path"]),
    ("dataops_core.delta", "uri_from_parts", ["bucket", "prefix"]),
    ("dataops_core.delta", "merge", ["df", "path", "merge_keys", "partition_col"]),
    ("dataops_core.delta", "append", ["df", "path", "fill_missing_columns"]),
    ("dataops_core.delta", "overwrite", ["df", "path"]),
    ("dataops_core.delta", "vacuum", ["path", "retention_hours", "dry_run"]),
    ("dataops_core.delta", "merge_bronze", ["df", "path", "merge_keys", "partition_col"]),
    ("dataops_core.delta", "append_bronze", ["df", "path", "fill_missing_columns"]),
    ("dataops_core.delta", "overwrite_bronze", ["df", "path"]),
    ("dataops_core.delta", "vacuum_bronze", ["path", "retention_hours", "dry_run"]),
    ("dataops_core.delta", "table_stats", ["path", "history_limit"]),
    ("dataops_core.delta", "bucket_usage", ["bucket", "prefix"]),
    ("dataops_core.delta", "delta_uri_from_object_key", ["bucket", "key"]),
    ("dataops_core.delta", "growth_pct", ["current", "previous"]),
    ("dataops_core.delta", "evaluate_health", ["metrics", "thresholds"]),
    ("dataops_core.delta", "resolve_targets", ["config", "table_resolver"]),
    ("dataops_core.delta", "resolve_s3_ca_bundle", []),
    ("dataops_core.delta", "resolve_s3_ssl_verify", []),
    ("dataops_core.delta", "s3_client", []),
]


@pytest.mark.parametrize("module_name, func_name, params", CONTRACT)
def test_public_signature_is_stable(module_name, func_name, params):
    module = importlib.import_module(module_name)
    func = getattr(module, func_name, None)
    assert func is not None, f"{module_name}.{func_name} sumiu da API publica"

    signature = inspect.signature(func)
    for param in params:
        assert param in signature.parameters, (
            f"{module_name}.{func_name} perdeu o parametro '{param}'"
        )


@pytest.mark.parametrize("name", ["VigilanciaHighlight", "ExecucaoMetric"])
def test_dataclasses_stay_exported(name):
    module = importlib.import_module("dataops_core.notify")
    assert hasattr(module, name)


def test_medallion_bucket_defaults_exported():
    from dataops_core.delta import DEFAULT_MEDALLION_BUCKETS

    assert list(DEFAULT_MEDALLION_BUCKETS) == ["bronze", "prata", "ouro"]


def test_vigilancia_highlight_fields():
    from dataops_core.notify import VigilanciaHighlight

    item = VigilanciaHighlight(indicador="SRAG", valor="120", tendencia="alta")
    assert item.nota == ""


def test_version_is_semver():
    import dataops_core

    parts = dataops_core.__version__.split(".")
    assert len(parts) == 3 and all(p.isdigit() for p in parts)


def test_require_accepts_current_and_rejects_future():
    import dataops_core

    dataops_core.require(dataops_core.__version__)
    with pytest.raises(RuntimeError):
        dataops_core.require("99.0.0")
