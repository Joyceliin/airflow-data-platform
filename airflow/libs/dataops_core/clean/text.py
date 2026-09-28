"""Normalizacao de texto aplicada na passagem Bronze -> Prata."""

from __future__ import annotations

import re

from unidecode import unidecode

_SPACES = re.compile(r"\s+")
_NON_DIGIT = re.compile(r"\D+")


def collapse_spaces(value: str | None) -> str | None:
    """Reduz espacos repetidos e remove os das pontas."""
    if value is None:
        return None
    return _SPACES.sub(" ", value).strip()


def strip_accents(value: str | None) -> str | None:
    if value is None:
        return None
    return unidecode(value)


def null_if_blank(value: str | None) -> str | None:
    """Trata string vazia, so-espacos e marcadores de ausencia como nulo."""
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned or cleaned.upper() in {"NA", "N/A", "NULL", "NONE", "-", "--"}:
        return None
    return cleaned


def normalize_text(
    value: str | None,
    *,
    upper: bool = True,
    accents: bool = False,
) -> str | None:
    """Normalizacao padrao de campo textual: nulo, espacos, acentos e caixa.

    `accents=False` remove acentos; `accents=True` os mantem.
    """
    cleaned = null_if_blank(value)
    if cleaned is None:
        return None
    cleaned = collapse_spaces(cleaned)
    if not accents:
        cleaned = strip_accents(cleaned)
    return cleaned.upper() if upper else cleaned


def digits_only(value: str | None) -> str | None:
    """Mantem apenas digitos; util para documentos e codigos vindos com mascara."""
    if value is None:
        return None
    digits = _NON_DIGIT.sub("", value)
    return digits or None
