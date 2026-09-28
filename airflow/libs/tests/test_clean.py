"""Comportamento dos utilitarios de limpeza."""

from __future__ import annotations

from datetime import date

import pytest

from dataops_core.clean import (
    calcular_cd_fx_etaria,
    digits_only,
    ds_fx_etaria,
    normalize_cid10,
    normalize_cnes,
    normalize_text,
    null_if_blank,
    parse_date_br,
    parse_timestamp_br,
    validate_cns,
    validate_cpf,
)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("  José   da Silva ", "JOSE DA SILVA"),
        ("N/A", None),
        ("", None),
        (None, None),
    ],
)
def test_normalize_text_default_strips_accents_and_uppercases(raw, expected):
    assert normalize_text(raw) == expected


def test_normalize_text_can_preserve_display_form():
    assert normalize_text("josé da silva", upper=False, accents=True) == "josé da silva"


def test_null_if_blank_treats_absence_markers_as_null():
    assert null_if_blank("--") is None
    assert null_if_blank("ok") == "ok"


def test_digits_only_drops_mask():
    assert digits_only("123.456.789-09") == "12345678909"
    assert digits_only("sem numero") is None


@pytest.mark.parametrize("value", ["529.982.247-25", "52998224725"])
def test_validate_cpf_accepts_valid(value):
    assert validate_cpf(value) is True


@pytest.mark.parametrize("value", ["111.111.111-11", "12345678900", "123", None])
def test_validate_cpf_rejects_invalid(value):
    assert validate_cpf(value) is False


def test_validate_cns_definitivo():
    # Inicia em 1/2: PIS nos 11 primeiros digitos + dv calculado.
    assert validate_cns("115 4058 8280 0006") is True


def test_validate_cns_provisorio():
    # Inicia em 7/8/9: soma ponderada dos 15 digitos divisivel por 11.
    assert validate_cns("898001160125335") is True


@pytest.mark.parametrize("value", ["123456789012345", "115405882800001", "abc", None])
def test_validate_cns_rejects_invalid(value):
    assert validate_cns(value) is False


def test_normalize_cnes_pads_to_seven_digits():
    assert normalize_cnes("12345") == "0012345"
    assert normalize_cnes("123456789") is None


@pytest.mark.parametrize("raw, expected", [("a09.0", "A090"), ("J18", "J18"), ("xx", None)])
def test_normalize_cid10(raw, expected):
    assert normalize_cid10(raw) == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("31/12/2025", date(2025, 12, 31)),
        ("2025-12-31", date(2025, 12, 31)),
        ("20251231", date(2025, 12, 31)),
        ("data ruim", None),
    ],
)
def test_parse_date_br_accepts_br_and_iso(raw, expected):
    assert parse_date_br(raw) == expected


def test_parse_timestamp_br_anchors_naive_value_in_sao_paulo():
    parsed = parse_timestamp_br("31/12/2025 23:30:00")
    assert parsed is not None
    assert parsed.tzinfo is not None
    assert parsed.hour == 23


@pytest.mark.parametrize(
    "idade, unidade, esperado",
    [
        (3, "dia", 0),
        (20, "dia", 1),
        (40, "dia", 2),
        (5, "mes", 2),
        (0, "ano", 2),
        (2, "ano", 3),
        (19, "ano", 6),
        (85, "ano", 13),
        (None, "ano", 14),
        (30, "unidade desconhecida", 14),
    ],
)
def test_faixa_etaria(idade, unidade, esperado):
    assert calcular_cd_fx_etaria(idade, unidade) == esperado


def test_ds_fx_etaria_cai_para_ignorado():
    assert ds_fx_etaria(13) == "80 anos e mais"
    assert ds_fx_etaria(99) == "Ignorado"
