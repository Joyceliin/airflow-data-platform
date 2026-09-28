"""Primitivas delta sem MinIO — URI e predicado."""

from __future__ import annotations

import pandas as pd

from dataops_core.delta import delta_uri, uri_from_parts
from dataops_core.delta.io import _partition_predicate


def test_delta_uri_converts_s3a():
    assert delta_uri("s3a://bronze/projeto_a/tb") == "s3://bronze/projeto_a/tb"
    assert delta_uri("s3://bronze/projeto_a/tb") == "s3://bronze/projeto_a/tb"


def test_uri_from_parts():
    assert uri_from_parts("bronze", "projeto_a/tb") == "s3a://bronze/projeto_a/tb"
    assert uri_from_parts("bronze", "") == "s3a://bronze"


def test_partition_predicate_single_and_multi():
    df = pd.DataFrame({"origem": ["hra", "hra"], "id": [1, 2]})
    assert _partition_predicate(df, "origem") == "target.`origem` = 'hra'"

    df2 = pd.DataFrame({"origem": ["hra", "hbl"], "id": [1, 2]})
    pred = _partition_predicate(df2, "origem")
    assert pred is not None
    assert "IN" in pred and "hra" in pred and "hbl" in pred
