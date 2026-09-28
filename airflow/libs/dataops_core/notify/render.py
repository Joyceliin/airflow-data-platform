"""Montagem do HTML a partir do shell unico.

Os corpos recebem dados ja estruturados. Os marcadores `{slot}` do template
sao substituidos por `str.replace`.
"""

from __future__ import annotations

import html
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from dataops_core.notify import branding


@dataclass
class VigilanciaHighlight:
    """Indicador em destaque num alerta de vigilancia."""

    indicador: str
    valor: str
    tendencia: str
    nota: str = ""


@dataclass
class ExecucaoMetric:
    """Linha do resumo de uma execucao bem-sucedida."""

    rotulo: str
    valor: str


def escape(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def summarize_detail(text: str, *, max_len: int = branding.ERROR_SUMMARY_MAX) -> str:
    """Resume um stack trace nas primeiras linhas uteis."""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return "Sem detalhes adicionais."
    summary = lines[0]
    extra = [line for line in lines[1:4] if line != summary]
    if extra:
        summary = f"{summary} {' · '.join(extra[:2])}"
    if len(summary) > max_len:
        return summary[: max_len - 1].rstrip() + "…"
    return summary


def context_line(
    *,
    dag_id: str,
    task_id: str,
    when: str,
    extra: str = "",
    label: str | None = None,
) -> str:
    base = f"{branding.pipeline_label(dag_id, label)} · {dag_id} · {task_id} · {when}"
    return f"{base} · {extra}" if extra else base


def action_html(label: str, url: str | None) -> str:
    if not url:
        return f'<span style="font-size:13px;color:#607085;">{escape(label)} indisponível</span>'
    return (
        f'<a href="{html.escape(url, quote=True)}" target="_blank" '
        'style="display:inline-block;padding:10px 18px;border-radius:7px;'
        'background:#1a3190;color:#ffffff;text-decoration:none;'
        f'font-size:14px;font-weight:700;">{escape(label)}</a>'
    )


def box_html(*, accent_color: str, inner_html: str) -> str:
    return (
        '<div style="background:#f6f8fb;border:1px solid #d4dce5;'
        f"border-left:3px solid {accent_color};border-radius:8px;"
        f'padding:14px 16px;">{inner_html}</div>'
    )


def _paragraph(text: str, *, margin: str = "0") -> str:
    return (
        f'<p style="margin:{margin};font-size:14px;line-height:1.6;color:#1e2d3d;">'
        f"{escape(text)}</p>"
    )


def alert_body(*, summary: str, accent_color: str) -> str:
    return box_html(accent_color=accent_color, inner_html=_paragraph(summary))


def quality_body(
    *,
    rows: Sequence[tuple[str, str]] = (),
    intro: str,
    accent_color: str,
    max_rows: int = 12,
) -> str:
    """Divergencias de qualidade em tabela."""
    parts = [_paragraph(intro, margin="0 0 12px 0")]
    if rows:
        header = "".join(
            '<th align="left" style="padding:8px 10px;font-size:11px;'
            'text-transform:uppercase;letter-spacing:.06em;color:#607085;'
            f'border-bottom:1px solid #d4dce5;">{escape(title)}</th>'
            for title in ("Tabela", "Divergência")
        )
        cells = [
            "<tr>"
            '<td style="padding:8px 10px;font-size:13px;font-weight:600;color:#122370;'
            f'border-bottom:1px solid #edf1f5;">{escape(name)}</td>'
            '<td style="padding:8px 10px;font-size:13px;color:#1e2d3d;'
            f'border-bottom:1px solid #edf1f5;">{escape(detail)}</td>'
            "</tr>"
            for name, detail in rows[:max_rows]
        ]
        if len(rows) > max_rows:
            cells.append(
                '<tr><td colspan="2" style="padding:8px 10px;font-size:12px;color:#607085;">'
                f"+ {len(rows) - max_rows} tabela(s) nos logs</td></tr>"
            )
        parts.append(
            '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
            f'style="border-collapse:collapse;"><tr>{header}</tr>{"".join(cells)}</table>'
        )
    return box_html(accent_color=accent_color, inner_html="".join(parts))


def _trend_color(tendencia: str) -> str:
    key = tendencia.lower()
    if "alta" in key and "persist" in key:
        return "#D83E3E"
    if "alta" in key or "atencao" in key or "atenção" in key:
        return "#F5A020"
    if "queda" in key or "baixa" in key:
        return "#29A44B"
    return "#607085"


def vigilancia_body(
    *,
    reference: str,
    highlights: Iterable[VigilanciaHighlight],
    intro: str,
    accent_color: str,
    max_items: int = 5,
) -> str:
    parts = [
        f'<p style="margin:0 0 4px 0;font-size:12px;color:#607085;">'
        f"Referencia · {escape(reference)}</p>",
        _paragraph(intro, margin="0 0 14px 0"),
    ]
    for item in list(highlights)[:max_items]:
        parts.append(
            '<div style="margin-bottom:10px;padding:12px 14px;background:#ffffff;'
            'border:1px solid #d4dce5;border-radius:8px;">'
            f'<div style="font-size:13px;font-weight:600;color:#122370;">'
            f"{escape(item.indicador)}</div>"
            f'<div style="margin-top:6px;font-size:18px;font-weight:700;color:#0f1923;">'
            f"{escape(item.valor)}"
            f'<span style="margin-left:8px;font-size:12px;font-weight:700;'
            f'color:{_trend_color(item.tendencia)};">{escape(item.tendencia)}</span></div>'
        )
        if item.nota:
            parts.append(
                f'<div style="margin-top:4px;font-size:12px;color:#607085;">'
                f"{escape(item.nota)}</div>"
            )
        parts.append("</div>")
    return box_html(accent_color=accent_color, inner_html="".join(parts))


def execucao_body(
    *,
    intro: str,
    metrics: Iterable[ExecucaoMetric],
    accent_color: str,
    max_rows: int = 8,
) -> str:
    rows = "".join(
        "<tr>"
        f'<td style="padding:8px 0;font-size:13px;color:#607085;width:45%;">'
        f"{escape(metric.rotulo)}</td>"
        f'<td style="padding:8px 0;font-size:13px;font-weight:600;color:#122370;">'
        f"{escape(metric.valor)}</td>"
        "</tr>"
        for metric in list(metrics)[:max_rows]
    )
    inner = (
        _paragraph(intro, margin="0 0 12px 0")
        + '<table role="presentation" width="100%" cellspacing="0" cellpadding="0">'
        + rows
        + "</table>"
    )
    return box_html(accent_color=accent_color, inner_html=inner)


def render_shell(
    *,
    subject: str,
    kind: str,
    body_html: str,
    eyebrow: str | None = None,
    headline: str | None = None,
    context: str = "",
    action: str = "",
    footer_note: str | None = None,
) -> str:
    style = branding.style_for(kind)
    chrome = {
        "{subject}": escape(subject),
        "{logo_html}": branding.logo_html(),
        "{eyebrow}": escape(eyebrow or style["eyebrow"]),
        "{headline}": escape(headline or style["headline"]),
        "{context_line}": escape(context),
        "{brand_name}": escape(branding.BRAND_NAME),
        "{brand_org}": escape(branding.BRAND_ORG),
        "{copyright_line}": escape(
            f"{branding.COPYRIGHT_LINE} · {footer_note or style['footer_note']}"
        ),
    }

    try:
        shell = branding.SHELL_PATH.read_text(encoding="utf-8")
    except OSError:
        return (
            f"<html><body>{escape(headline or style['headline'])}<br>"
            f"{body_html}{action}</body></html>"
        )

    for slot, value in chrome.items():
        shell = shell.replace(slot, value)
    # Conteudo por ultimo, para que texto do corpo nunca seja tratado como slot.
    return shell.replace("{body_html}", body_html).replace("{action_html}", action)
