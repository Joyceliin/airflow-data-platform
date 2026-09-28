"""Identificadores de saude: CNS, CPF, CNES e CID-10.

As funcoes `validate_*` devolvem bool e nao levantam excecao. As `normalize_*`
devolvem a forma canonica ou None quando o valor e invalido.
"""

from __future__ import annotations

import re

from dataops_core.clean.text import digits_only

_CID10 = re.compile(r"^[A-Z]\d{2}\d?$")


def validate_cpf(value: str | None) -> bool:
    digits = digits_only(value)
    if digits is None or len(digits) != 11 or len(set(digits)) == 1:
        return False
    for size in (9, 10):
        weights = range(size + 1, 1, -1)
        total = sum(int(d) * w for d, w in zip(digits[:size], weights))
        check = (total * 10) % 11 % 10
        if check != int(digits[size]):
            return False
    return True


def normalize_cpf(value: str | None) -> str | None:
    """CPF em 11 digitos, sem mascara. None se o digito verificador nao bater."""
    digits = digits_only(value)
    return digits if validate_cpf(digits) else None


def validate_cns(value: str | None) -> bool:
    """Valida CNS definitivo (inicia em 1 ou 2) e provisorio (7, 8 ou 9)."""
    digits = digits_only(value)
    if digits is None or len(digits) != 15:
        return False

    if digits[0] in "789":
        total = sum(int(d) * (15 - i) for i, d in enumerate(digits))
        return total % 11 == 0

    if digits[0] not in "12":
        return False

    pis = digits[:11]
    total = sum(int(d) * (15 - i) for i, d in enumerate(pis))
    rest = total % 11
    check = 11 - rest
    if check == 11:
        check = 0
    if check == 10:
        total += 2
        rest = total % 11
        check = 11 - rest
        expected = f"{pis}001{check}"
    else:
        expected = f"{pis}000{check}"
    return digits == expected


def normalize_cns(value: str | None) -> str | None:
    """CNS em 15 digitos, sem mascara. None se o digito verificador nao bater."""
    digits = digits_only(value)
    return digits if validate_cns(digits) else None


def validate_cnes(value: str | None) -> bool:
    """CNES e um codigo de 7 digitos sem digito verificador — so o formato e checavel."""
    digits = digits_only(value)
    return digits is not None and len(digits) == 7


def normalize_cnes(value: str | None) -> str | None:
    """Completa com zeros a esquerda ate 7 digitos."""
    digits = digits_only(value)
    if digits is None or len(digits) > 7:
        return None
    return digits.zfill(7)


def normalize_cid10(value: str | None) -> str | None:
    """CID-10 na forma DATASUS: maiuscula, sem ponto (A09.0 -> A090)."""
    if value is None:
        return None
    code = re.sub(r"[^A-Za-z0-9]", "", value).upper()
    return code if _CID10.match(code) else None
