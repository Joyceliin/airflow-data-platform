"""Identidade visual e configuracao dos e-mails da plataforma.

Nome, organizacao, unidade e rodape vem de variaveis de ambiente
(`DATAOPS_BRAND_*`). O rotulo do pipeline vem do argumento `label=`, de
`DATAOPS_PIPELINE_LABEL` ou do `dag_id`.
"""

from __future__ import annotations

import base64
import html
import os
from pathlib import Path

BRAND_NAME = os.getenv("DATAOPS_BRAND_NAME", "Data Platform")
BRAND_ORG = os.getenv("DATAOPS_BRAND_ORG", "Engenharia de Dados")
BRAND_UNIT = os.getenv("DATAOPS_BRAND_UNIT", "Data Ops")
COPYRIGHT_LINE = os.getenv("DATAOPS_BRAND_COPYRIGHT", "Mensagem automática da plataforma de dados")

TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"
SHELL_PATH = TEMPLATE_DIR / "shell.html"
LOGO_PATH = Path(os.getenv("DATAOPS_EMAIL_LOGO_PATH", TEMPLATE_DIR / "logo.svg"))

EMAIL_VAR = os.getenv("DATAOPS_EMAIL_VAR", "emails_alerta")
SMTP_CONN_ID = os.getenv("DATAOPS_SMTP_CONN_ID", "smtp_default")
ERROR_SUMMARY_MAX = 320

KINDS: dict[str, dict[str, str]] = {
    "error": {
        "accent_color": "#D83E3E",
        "headline": "Falha na execução",
        "eyebrow": f"{BRAND_UNIT} · Alerta operacional",
        "footer_note": "Detalhes completos nos logs do Airflow.",
        "subject_prefix": "Alerta",
        "action_label": "Ver logs no Airflow",
    },
    "quality": {
        "accent_color": "#F5A020",
        "headline": "Relatório interno de qualidade",
        "eyebrow": f"{BRAND_UNIT} · Qualidade de dados",
        "footer_note": "Validação warn-only — o pipeline não foi interrompido.",
        "subject_prefix": "Qualidade",
        "action_label": "Ver logs da validação",
    },
    "vigilancia": {
        "accent_color": "#29A44B",
        "headline": "Alerta de vigilância",
        "eyebrow": f"{BRAND_UNIT} · Indicadores em atenção",
        "footer_note": "Leitura operacional D-1 — confira o painel para detalhes.",
        "subject_prefix": "Vigilância",
        "action_label": "Abrir painel",
    },
    "execucao": {
        "accent_color": "#2652D0",
        "headline": "Execução concluída",
        "eyebrow": f"{BRAND_UNIT} · Resumo de pipeline",
        "footer_note": "Notificação informativa — nenhuma ação obrigatória.",
        "subject_prefix": "Execução",
        "action_label": "Ver run no Airflow",
    },
}

# Nomes alternativos aceitos para os modos.
_ALIASES = {"monitor": "quality", "success": "execucao"}


def style_for(kind: str) -> dict[str, str]:
    return KINDS.get(_ALIASES.get(kind, kind), KINDS["error"])


def pipeline_label(dag_id: str | None = None, override: str | None = None) -> str:
    """Rotulo do pipeline no subtitulo.

    Precedencia: argumento da chamada, depois `DATAOPS_PIPELINE_LABEL`, depois o
    `dag_id`.
    """
    return override or os.getenv("DATAOPS_PIPELINE_LABEL") or dag_id or BRAND_NAME


def logo_html() -> str:
    """Logo em base64; sem o arquivo, usa nome e organizacao em texto."""
    try:
        encoded = base64.b64encode(LOGO_PATH.read_bytes()).decode("ascii")
    except OSError:
        return (
            '<div style="font-size:22px;font-weight:700;color:#3155a4;'
            f'letter-spacing:-.02em;">{html.escape(BRAND_NAME)}</div>'
            '<div style="margin-top:4px;font-size:13px;color:#607085;">'
            f"{html.escape(BRAND_ORG)}</div>"
        )
    mime = "image/svg+xml" if LOGO_PATH.suffix.lower() == ".svg" else "image/png"
    return (
        f'<img src="data:{mime};base64,{encoded}" alt="{BRAND_NAME}" width="148" '
        'style="display:block;border:0;height:auto;max-width:148px;" />'
    )
