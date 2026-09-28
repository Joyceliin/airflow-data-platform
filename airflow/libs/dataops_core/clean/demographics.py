"""Faixa etaria padrao da plataforma.

A classificacao e feita por quantidade + unidade canonica
(`minuto` | `hora` | `dia` | `mes` | `ano`). A conversao do codigo de idade da
fonte para essa unidade fica com quem chama.
"""

from __future__ import annotations

from typing import Any

FX_ETARIA_PADRAO: dict[int, str] = {
    0: "Neonatal precoce",
    1: "Neonatal tardio",
    2: "Pós-neonatal",
    3: "1 a 2 anos",
    4: "3 a 5 anos",
    5: "6 a 9 anos",
    6: "10 a 19 anos",
    7: "20 a 29 anos",
    8: "30 a 39 anos",
    9: "40 a 49 anos",
    10: "50 a 59 anos",
    11: "60 a 69 anos",
    12: "70 a 79 anos",
    13: "80 anos e mais",
    14: "Ignorado",
}

CD_IGNORADO = 14


def _is_missing(value: Any) -> bool:
    """Cobre None, pd.NA e NaN sem obrigar a lib a importar pandas."""
    if value is None:
        return True
    try:
        return bool(value != value)
    except (TypeError, ValueError):
        return True


def calcular_cd_fx_etaria(idade: Any, unidade_idade: Any) -> int:
    """Codigo 0-14 da faixa etaria padrao (neonatal + grupos em anos)."""
    if _is_missing(idade) or _is_missing(unidade_idade):
        return CD_IGNORADO
    try:
        qtd = int(idade)
    except (TypeError, ValueError):
        return CD_IGNORADO

    und = str(unidade_idade).strip().lower()
    if und in ("minuto", "hora", "dia"):
        # minuto e hora entram como neonato: <=6 precoce, <=27 tardio.
        if qtd <= 6:
            return 0
        if qtd <= 27:
            return 1
        return 2
    if und == "mes":
        return 2
    if und == "ano":
        if qtd < 1:
            return 2
        if qtd <= 2:
            return 3
        if qtd <= 5:
            return 4
        if qtd <= 9:
            return 5
        if qtd <= 19:
            return 6
        if qtd <= 29:
            return 7
        if qtd <= 39:
            return 8
        if qtd <= 49:
            return 9
        if qtd <= 59:
            return 10
        if qtd <= 69:
            return 11
        if qtd <= 79:
            return 12
        return 13
    return CD_IGNORADO


def ds_fx_etaria(codigo: Any) -> str:
    try:
        return FX_ETARIA_PADRAO[int(codigo)]
    except (TypeError, ValueError, KeyError):
        return FX_ETARIA_PADRAO[CD_IGNORADO]
