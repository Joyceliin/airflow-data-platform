"""SHA-256 com salt sobre identificadores diretos (CNS, CPF).

O salt vem de `DATAOPS_HASH_SALT` e nao e logado.
"""

from __future__ import annotations

import hashlib
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

SALT_ENV = "DATAOPS_HASH_SALT"


def salt_configured() -> bool:
    return bool(os.environ.get(SALT_ENV, "").strip())


def _salt() -> str:
    salt = os.environ.get(SALT_ENV, "")
    if not salt:
        logger.warning(
            "%s ausente — hash gerado sem salt e vulneravel a forca bruta.", SALT_ENV
        )
    return salt


def hash_value(value: Any) -> str | None:
    """Hash de um identificador. None para valor ausente ou vazio."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return hashlib.sha256(f"{_salt()}{text}".encode("utf-8")).hexdigest()


def hash_series(series):
    """Versao vetorizada: calcula o hash dos valores distintos e mapeia de volta."""
    import pandas as pd

    salt = _salt()
    cleaned = series.astype("string").str.strip()
    cleaned = cleaned.mask(cleaned.eq(""), pd.NA)
    mapping = {
        value: hashlib.sha256(f"{salt}{value}".encode("utf-8")).hexdigest()
        for value in cleaned.dropna().drop_duplicates()
    }
    return cleaned.map(mapping).astype("string")
