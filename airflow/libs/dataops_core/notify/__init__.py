"""E-mails operacionais da plataforma com template unico.

O template fica em `dataops_core/notify/templates/shell.html`. Os modos
`error`, `quality`, `vigilancia` e `execucao` usam o mesmo desenho, mudando
cor de acento, cabecalho e rodape.
"""

from __future__ import annotations

from dataops_core.notify.callbacks import failure_callback, on_failure_callback
from dataops_core.notify.email import (
    ExecucaoMetric,
    VigilanciaHighlight,
    send_platform_email,
    send_execucao_email,
    send_failure_email,
    send_monitoring_email,
    send_quality_email,
    send_vigilancia_email,
)

__all__ = [
    "ExecucaoMetric",
    "VigilanciaHighlight",
    "failure_callback",
    "on_failure_callback",
    "send_platform_email",
    "send_execucao_email",
    "send_failure_email",
    "send_monitoring_email",
    "send_quality_email",
    "send_vigilancia_email",
]
