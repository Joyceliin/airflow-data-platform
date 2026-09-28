"""Renderizacao do template unico — roda sem Airflow e sem SMTP."""

from __future__ import annotations

import re

import pytest

from dataops_core.notify import ExecucaoMetric, VigilanciaHighlight
from dataops_core.notify.branding import KINDS, style_for
from dataops_core.notify.render import (
    action_html,
    alert_body,
    context_line,
    execucao_body,
    quality_body,
    render_shell,
    summarize_detail,
    vigilancia_body,
)

SLOT = re.compile(r"\{[a-z_]+\}")


def _render(**kwargs):
    defaults = {"subject": "assunto", "kind": "error", "body_html": "<p>corpo</p>"}
    return render_shell(**{**defaults, **kwargs})


@pytest.mark.parametrize("kind", list(KINDS))
def test_shell_has_no_unreplaced_slot(kind):
    assert not SLOT.search(_render(kind=kind))


@pytest.mark.parametrize(
    "kind, accent",
    [("error", "#D83E3E"), ("quality", "#F5A020"), ("vigilancia", "#29A44B"), ("execucao", "#2652D0")],
)
def test_accent_color_reaches_the_body(kind, accent):
    assert accent in alert_body(summary="x", accent_color=style_for(kind)["accent_color"])


@pytest.mark.parametrize("alias, target", [("monitor", "quality"), ("success", "execucao")])
def test_kind_aliases_resolve(alias, target):
    assert style_for(alias) == KINDS[target]


def test_logo_ships_with_the_lib_and_is_embedded():
    from dataops_core.notify.branding import LOGO_PATH, logo_html

    assert LOGO_PATH.is_file(), "logo.svg sumiu de notify/templates"
    assert 'src="data:image/svg+xml;base64,' in logo_html()


def test_logo_falls_back_to_text_when_asset_is_missing(monkeypatch, tmp_path):
    from dataops_core.notify import branding

    monkeypatch.setattr(branding, "LOGO_PATH", tmp_path / "inexistente.svg")
    fallback = branding.logo_html()
    assert "base64" not in fallback
    assert "Data Platform" in fallback


def test_institutional_identity_is_present():
    html = _render()
    assert "Data Platform" in html
    assert "#29A44B" in html and "#D83E3E" in html  # barra de 4 cores
    assert "Engenharia de Dados" in html


def test_footer_note_joins_the_copyright_line():
    assert "Detalhes completos nos logs do Airflow." in _render(kind="error")


def test_detail_is_escaped():
    html = _render(body_html=alert_body(summary="<script>alert(1)</script>", accent_color="#000"))
    assert "<script>" not in html
    assert "&lt;script&gt;" in html


def test_body_containing_slot_text_is_not_substituted():
    html = _render(body_html=alert_body(summary="erro perto de {eyebrow}", accent_color="#000"))
    assert "{eyebrow}" in html


def test_summarize_keeps_first_lines_and_truncates():
    assert summarize_detail("") == "Sem detalhes adicionais."
    assert summarize_detail("linha unica") == "linha unica"
    longo = summarize_detail("x" * 500)
    assert len(longo) <= 320 and longo.endswith("…")


def test_context_line_uses_pipeline_label_from_env(monkeypatch):
    monkeypatch.setenv("DATAOPS_PIPELINE_LABEL", "Estoque")
    line = context_line(dag_id="estoque_pipeline", task_id="extract", when="agora")
    assert line.startswith("Estoque · estoque_pipeline · extract · agora")


def test_context_line_falls_back_to_dag_id(monkeypatch):
    monkeypatch.delenv("DATAOPS_PIPELINE_LABEL", raising=False)
    assert context_line(dag_id="vendas_pipeline", task_id="t", when="w").startswith("vendas_pipeline")


def test_context_line_label_argument_wins_over_env(monkeypatch):
    """O rotulo passado na chamada tem precedencia sobre o ambiente."""
    monkeypatch.setenv("DATAOPS_PIPELINE_LABEL", "Estoque")
    line = context_line(dag_id="vendas_pipeline", task_id="t", when="w", label="Vendas")
    assert line.startswith("Vendas · vendas_pipeline")


def test_quality_body_lists_rows_and_counts_overflow():
    rows = [(f"tabela_{i}", f"diff {i}") for i in range(15)]
    body = quality_body(rows=rows, intro="15 divergencias", accent_color="#F5A020")
    assert "tabela_0" in body
    assert "+ 3 tabela(s) nos logs" in body


def test_quality_body_without_rows_is_just_the_intro():
    body = quality_body(intro="validacao sem divergencia", accent_color="#F5A020")
    assert "validacao sem divergencia" in body
    assert "<table" not in body


def test_vigilancia_body_marks_trend_colors():
    body = vigilancia_body(
        reference="2026-09-10",
        highlights=[VigilanciaHighlight("SRAG", "120", "alta persistente", nota="D-1")],
        intro="indicadores em atencao",
        accent_color="#29A44B",
    )
    assert "SRAG" in body and "#D83E3E" in body and "D-1" in body


def test_execucao_body_renders_metrics():
    body = execucao_body(
        intro="carga ok",
        metrics=[ExecucaoMetric("Linhas", "1.204"), ExecucaoMetric("Tabelas", "7")],
        accent_color="#2652D0",
    )
    assert "Linhas" in body and "1.204" in body


def test_action_degrades_without_url():
    assert "indisponível" in action_html("Ver logs no Airflow", None)
    assert 'href="https://airflow/log"' in action_html("Ver logs", "https://airflow/log")
