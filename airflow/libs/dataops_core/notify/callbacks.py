"""Callbacks prontos para `default_args` das DAGs."""

from __future__ import annotations

from typing import Any, Callable, Mapping

from dataops_core.notify.email import send_failure_email

__all__ = ["failure_callback", "on_failure_callback"]


def on_failure_callback(context: Mapping[str, Any]) -> None:
    """Alerta de falha no template da plataforma.

    Uso na DAG:
        default_args = {"email_on_failure": False, "on_failure_callback": on_failure_callback}
    """
    send_failure_email(context)


def failure_callback(*, label: str) -> Callable[[Mapping[str, Any]], None]:
    """Mesmo callback, com rotulo fixo do projeto no subtitulo.

        default_args = {"on_failure_callback": failure_callback(label="Vendas")}
    """

    def _callback(context: Mapping[str, Any]) -> None:
        send_failure_email(context, label=label)

    return _callback
