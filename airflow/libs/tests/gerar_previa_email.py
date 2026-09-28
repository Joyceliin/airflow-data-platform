"""Gera os quatro modos de e-mail em HTML para conferencia visual.

Offline: nao usa Airflow, SMTP nem rede. Roda de qualquer diretorio:

    python airflow/libs/tests/gerar_previa_email.py

Os arquivos saem em `tests/previa/` (gitignored).
"""

from __future__ import annotations

import sys
import webbrowser
from pathlib import Path

LIBS_ROOT = Path(__file__).resolve().parent.parent
if str(LIBS_ROOT) not in sys.path:
    sys.path.insert(0, str(LIBS_ROOT))

from dataops_core.notify import ExecucaoMetric, VigilanciaHighlight  # noqa: E402
from dataops_core.notify import branding, render  # noqa: E402

OUT_DIR = Path(__file__).resolve().parent / "previa"
CONTEXT = render.context_line(
    dag_id="vendas_pipeline", task_id="extract_vendas", when="2026-09-11 13:40:00", label="Vendas"
)


def _body(kind: str, accent: str) -> str:
    """Corpo de exemplo de cada modo."""
    if kind == "error":
        return render.alert_body(
            summary=render.summarize_detail(
                "oracledb.exceptions.DatabaseError: ORA-12170: TNS:Connect timeout occurred\n"
                "  durante extracao incremental da tabela pedidos"
            ),
            accent_color=accent,
        )
    if kind == "quality":
        return render.quality_body(
            rows=[
                ("tb_pedidos", "fonte 12.401 x Delta 12.388 (-13)"),
                ("tb_itens", "fonte 45.002 x Delta 44.991 (-11)"),
            ],
            intro="2 divergencia(s) fonte x Delta acima do limiar configurado.",
            accent_color=accent,
        )
    if kind == "vigilancia":
        return render.vigilancia_body(
            reference="2026-09-10",
            highlights=[
                VigilanciaHighlight(indicador="SRAG", valor="120 casos", tendencia="alta"),
                VigilanciaHighlight(
                    indicador="Dengue", valor="87 casos", tendencia="estavel", nota="3 municipios"
                ),
            ],
            intro="Indicadores em atencao na semana epidemiologica.",
            accent_color=accent,
        )
    return render.execucao_body(
        intro="Carga concluida sem divergencias.",
        metrics=[ExecucaoMetric("Linhas", "12.401"), ExecucaoMetric("Tabelas", "7")],
        accent_color=accent,
    )


def main(abrir: bool = True) -> None:
    OUT_DIR.mkdir(exist_ok=True)
    for kind in ("error", "quality", "vigilancia", "execucao"):
        style = branding.style_for(kind)
        subject = f"[{branding.BRAND_NAME}] {style['subject_prefix']} · vendas_pipeline"
        html = render.render_shell(
            subject=subject,
            kind=kind,
            eyebrow=None,
            headline=None,
            context=CONTEXT,
            body_html=_body(kind, style["accent_color"]),
            action=render.action_html(style["action_label"], "https://airflow.exemplo/log"),
        )
        destino = OUT_DIR / f"previa_{kind}.html"
        destino.write_text(html, encoding="utf-8")
        faltando = [slot for slot in ("{body_html}", "{context_line}", "{eyebrow}") if slot in html]
        status = f"SLOT NAO SUBSTITUIDO: {faltando}" if faltando else "ok"
        print(f"{destino}  [{status}]")
        if abrir:
            webbrowser.open(destino.as_uri())


if __name__ == "__main__":
    main(abrir="--no-open" not in sys.argv)
