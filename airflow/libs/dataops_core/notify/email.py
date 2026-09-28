"""API publica de e-mail da plataforma.

Falha de envio nao derruba a task: e logada.
"""

from __future__ import annotations

import logging
from typing import Any, Iterable, Mapping, Sequence

from dataops_core.clean.dates import now_sp
from dataops_core.notify import branding, render, smtp
from dataops_core.notify.render import ExecucaoMetric, VigilanciaHighlight

logger = logging.getLogger(__name__)

__all__ = [
    "ExecucaoMetric",
    "VigilanciaHighlight",
    "send_platform_email",
    "send_execucao_email",
    "send_failure_email",
    "send_monitoring_email",
    "send_quality_email",
    "send_vigilancia_email",
]


def _dag_context(context: Mapping[str, Any] | None) -> dict[str, str]:
    context = context or {}
    task_instance = context.get("task_instance")
    dag = context.get("dag")
    try:
        log_url = task_instance.log_url if task_instance is not None else None
    except Exception:
        log_url = None
    return {
        "dag_id": getattr(dag, "dag_id", None) or "desconhecida",
        "task_id": getattr(task_instance, "task_id", None) or "desconhecida",
        "run_id": str(context.get("run_id") or "desconhecido"),
        "when": now_sp().strftime("%Y-%m-%d %H:%M:%S"),
        "log_url": log_url or "",
    }


def send_platform_email(
    context: Mapping[str, Any] | None = None,
    *,
    kind: str,
    subject: str,
    body_html: str,
    variavel_email: str = branding.EMAIL_VAR,
    headline: str | None = None,
    eyebrow: str | None = None,
    extra: str = "",
    label: str | None = None,
    action_url: str | None = None,
    conn_id: str = branding.SMTP_CONN_ID,
    raise_on_error: bool = False,
) -> None:
    """Monta o shell e entrega. Use os `send_*` especificos no dia a dia."""
    style = branding.style_for(kind)
    try:
        recipients = smtp.load_recipients(variavel_email)
        if not recipients:
            logger.warning("Nenhum e-mail em '%s'; alerta nao enviado.", variavel_email)
            return

        meta = _dag_context(context)
        html_body = render.render_shell(
            subject=subject,
            kind=kind,
            eyebrow=eyebrow,
            headline=headline,
            context=render.context_line(
                dag_id=meta["dag_id"],
                task_id=meta["task_id"],
                when=meta["when"],
                extra=extra,
                label=label,
            ),
            body_html=body_html,
            action=render.action_html(
                style["action_label"], action_url or meta["log_url"] or None
            ),
        )
        smtp.send(
            recipients=recipients,
            subject=subject,
            html_body=html_body,
            from_email=smtp.resolve_from_email(conn_id),
            conn_id=conn_id,
        )
        logger.info("E-mail da plataforma (%s) enviado a %s destinatario(s).", kind, len(recipients))
    except Exception as exc:
        logger.warning("Falha ao enviar e-mail (%s): %s: %s", kind, type(exc).__name__, exc)
        if "CERTIFICATE_VERIFY_FAILED" in str(exc) or type(exc).__name__ in {
            "SSLCertVerificationError",
            "SSLError",
        }:
            smtp.diagnose_tls(conn_id)
        if raise_on_error:
            raise


def send_failure_email(
    context: Mapping[str, Any] | None = None,
    *,
    texto_relatorio: str = "Erro na execução da DAG.",
    variavel_email: str = branding.EMAIL_VAR,
    label: str | None = None,
) -> None:
    """Alerta operacional de falha de task."""
    meta = _dag_context(context)
    style = branding.style_for("error")
    exception = (context or {}).get("exception")
    detail = str(exception) if exception else texto_relatorio
    send_platform_email(
        context,
        kind="error",
        subject=(
            f"[{branding.BRAND_NAME}] {style['subject_prefix']} · "
            f"{meta['dag_id']} · {meta['task_id']}"
        ),
        body_html=render.alert_body(
            summary=render.summarize_detail(detail), accent_color=style["accent_color"]
        ),
        variavel_email=variavel_email,
        label=label,
    )


def send_quality_email(
    context: Mapping[str, Any] | None = None,
    *,
    texto_relatorio: str = "",
    rows: Sequence[tuple[str, str]] = (),
    intro: str | None = None,
    variavel_email: str = branding.EMAIL_VAR,
    label: str | None = None,
    max_rows: int = 12,
) -> None:
    """Relatorio de qualidade warn-only.

    `rows` sao as divergencias como pares (tabela, descricao).
    """
    meta = _dag_context(context)
    style = branding.style_for("quality")
    if intro is None:
        intro = (
            f"{len(rows)} divergência(s) fonte x Delta acima do limiar configurado."
            if rows
            else render.summarize_detail(texto_relatorio)
        )
    suffix = f"{len(rows)} aviso(s)" if rows else "validação"
    send_platform_email(
        context,
        kind="quality",
        subject=(
            f"[{branding.BRAND_NAME}] {style['subject_prefix']} · {meta['dag_id']} · {suffix}"
        ),
        body_html=render.quality_body(
            rows=rows,
            intro=intro,
            accent_color=style["accent_color"],
            max_rows=max_rows,
        ),
        extra="count_validate",
        variavel_email=variavel_email,
        label=label,
    )


def send_monitoring_email(
    context: Mapping[str, Any] | None = None,
    *,
    texto_relatorio: str,
    variavel_email: str = branding.EMAIL_VAR,
    label: str | None = None,
) -> None:
    """Alias de `send_quality_email`."""
    send_quality_email(
        context,
        texto_relatorio=texto_relatorio,
        variavel_email=variavel_email,
        label=label,
    )


def send_vigilancia_email(
    context: Mapping[str, Any] | None = None,
    *,
    reference_date: str,
    highlights: Iterable[VigilanciaHighlight],
    intro: str,
    variavel_email: str = branding.EMAIL_VAR,
    panel_url: str | None = None,
    headline: str | None = None,
    label: str | None = None,
) -> None:
    """Alerta de vigilancia — indicadores em atencao."""
    style = branding.style_for("vigilancia")
    items = list(highlights)
    send_platform_email(
        context,
        kind="vigilancia",
        subject=(
            f"[{branding.BRAND_NAME}] {style['subject_prefix']} · {reference_date} · "
            f"{len(items)} indicador(es)"
        ),
        body_html=render.vigilancia_body(
            reference=reference_date,
            highlights=items,
            intro=intro,
            accent_color=style["accent_color"],
        ),
        headline=headline,
        extra=reference_date,
        action_url=panel_url,
        variavel_email=variavel_email,
        label=label,
    )


def send_execucao_email(
    context: Mapping[str, Any] | None = None,
    *,
    intro: str,
    metrics: Iterable[ExecucaoMetric],
    variavel_email: str = branding.EMAIL_VAR,
    headline: str | None = None,
    label: str | None = None,
) -> None:
    """Resumo de execucao bem-sucedida."""
    meta = _dag_context(context)
    style = branding.style_for("execucao")
    send_platform_email(
        context,
        kind="execucao",
        subject=f"[{branding.BRAND_NAME}] {style['subject_prefix']} · {meta['dag_id']} · OK",
        body_html=render.execucao_body(
            intro=intro, metrics=metrics, accent_color=style["accent_color"]
        ),
        headline=headline,
        variavel_email=variavel_email,
        label=label,
    )
