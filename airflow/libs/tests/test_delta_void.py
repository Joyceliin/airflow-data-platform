"""Coerção void/Null na borda de escrita Delta."""

from __future__ import annotations

import pandas as pd
import pyarrow as pa

from dataops_core.delta.io import _as_arrow, _coerce_void_fields


def test_as_arrow_all_null_string_not_void():
    df = pd.DataFrame(
        {
            "ds_idade_unidade": pd.Series([pd.NA, pd.NA], dtype="string"),
            "nu_competencia": [201001, 201001],
        }
    )
    table = _as_arrow(df)
    field = table.schema.field("ds_idade_unidade")
    assert not pa.types.is_null(field.type)
    assert pa.types.is_string(field.type) or pa.types.is_large_string(field.type)


def test_coerce_void_fields_null_column():
    n = 2
    table = pa.table(
        {
            "a": pa.array([None, None], type=pa.null()),
            "b": pa.array([1, 2], type=pa.int64()),
        }
    )
    out = _coerce_void_fields(table)
    assert pa.types.is_string(out.schema.field("a").type)
    assert out.column("a").null_count == n


def test_is_null_type_accepts_arro3_like_without_id():
    """delta-rs recente expõe arro3.DataType — sem atributo `.id` do pyarrow."""
    from dataops_core.delta.io import _as_pa_data_type, _is_null_type

    class Arro3LikeNull:
        def __str__(self) -> str:
            return "Null"

    assert _is_null_type(Arro3LikeNull()) is True
    assert _as_pa_data_type(Arro3LikeNull()) == pa.string()
    assert _is_null_type(pa.null()) is True
    assert _is_null_type(pa.string()) is False
