"""Biblioteca compartilhada da plataforma disponivel para todas as DAGs do orquestrador.

A pasta e montada em /opt/airflow/libs (read-only) e entra no PYTHONPATH do worker.
"""

from __future__ import annotations

__all__ = ["__version__", "require"]

__version__ = "1.0.0"


def _parse(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split(".")[:3])


def require(min_version: str) -> None:
    """Falha o parse da DAG se o worker tiver uma dataops_core anterior a `min_version`."""
    if _parse(__version__) < _parse(min_version):
        raise RuntimeError(
            f"dataops_core {min_version}+ e necessaria para esta DAG; o worker tem {__version__}."
        )
