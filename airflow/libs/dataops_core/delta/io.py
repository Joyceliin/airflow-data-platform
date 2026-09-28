"""Escrita / MERGE / vacuum no Delta Lake via delta-rs.

Path canônico: URI s3a://. Datas de negócio entram aware America/Sao_Paulo;
timestampNtz só na serialização (borda de escrita).
"""

from __future__ import annotations

import os
import time
from collections.abc import Sequence
from typing import Any

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, TableFeatures, write_deltalake
from deltalake.exceptions import TableNotFoundError

from dataops_core.clean.dates import TZ_SP, series_to_sp_naive_wallclock

_COMMIT_CONFLICT_MARKERS = (
    "concurrent transaction",
    "concurrent transactions",
    "commit failed",
    "metadata changed",
    "version already exists",
)

__all__ = [
    "append",
    "append_bronze",
    "cleanup_log",
    "count_key_duplicates",
    "count_rows",
    "dedupe_table",
    "delta_uri",
    "is_delta_table",
    "merge",
    "merge_bronze",
    "optimize_compact",
    "overwrite",
    "overwrite_bronze",
    "resolve_s3_ca_bundle",
    "resolve_s3_ssl_verify",
    "s3_client",
    "storage_options",
    "uri_from_parts",
    "vacuum",
    "vacuum_bronze",
]

# O diagnóstico TLS é impresso uma vez por processo.
_ssl_diag_emitted = False


def _merge_retries() -> int:
    return int(os.environ.get("DATAOPS_DELTA_MERGE_RETRIES") or "5")


def _merge_retry_sec() -> float:
    return float(os.environ.get("DATAOPS_DELTA_MERGE_RETRY_SEC") or "2")


def _void_strategy() -> str:
    return (os.environ.get("DATAOPS_DELTA_VOID_STRATEGY") or "skip").strip().lower()


def _env_truthy(name: str, default: str = "false") -> bool:
    return (os.environ.get(name) or default).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def resolve_s3_ca_bundle() -> str | None:
    """Caminho PEM/CA para HTTPS MinIO/S3.

    Ordem: ``DATAOPS_S3_CA_BUNDLE`` → ``AWS_CA_BUNDLE`` → ``REQUESTS_CA_BUNDLE``.
    """
    for key in (
        "DATAOPS_S3_CA_BUNDLE",
        "AWS_CA_BUNDLE",
        "REQUESTS_CA_BUNDLE",
    ):
        path = (os.environ.get(key) or "").strip()
        if path:
            return path
    return None


def resolve_s3_allow_invalid_certs() -> bool:
    """True se a validação TLS estiver desligada via env. Default False."""
    return _env_truthy("DATAOPS_S3_ALLOW_INVALID_CERTS") or _env_truthy(
        "AWS_ALLOW_INVALID_CERTIFICATES"
    )


def resolve_s3_ssl_verify() -> bool | str:
    """Valor para ``boto3`` ``verify=``: ``True``, ``False`` ou path do CA."""
    if resolve_s3_allow_invalid_certs():
        return False
    ca = resolve_s3_ca_bundle()
    if ca:
        return ca
    return True


def _emit_ssl_diag(verify: bool | str) -> None:
    global _ssl_diag_emitted
    if _ssl_diag_emitted:
        return
    _ssl_diag_emitted = True
    if verify is False:
        print(
            "[DeltaOps] TLS: verificação desligada "
            "(DATAOPS_S3_ALLOW_INVALID_CERTS / AWS_ALLOW_INVALID_CERTIFICATES). "
            "Use só em lab; em homologação/produção monte a CA em "
            "DATAOPS_S3_CA_BUNDLE ou AWS_CA_BUNDLE.",
            flush=True,
        )
    elif isinstance(verify, str):
        print(f"[DeltaOps] TLS: CA bundle={verify}", flush=True)


def storage_options() -> dict[str, str]:
    """Opções object_store/delta-rs (credenciais + TLS para MinIO HTTPS).

    TLS (delta-rs / object_store):
    - ``certificate_path`` a partir do CA bundle resolvido;
    - ``allow_invalid_certificates=true`` só se explicitamente liberado via env.
    """
    endpoint = (
        os.environ.get("AWS_ENDPOINT_URL")
        or os.environ.get("MINIO_ENDPOINT")
        or ""
    ).strip()
    options = {
        "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
        "AWS_S3_ALLOW_UNSAFE_RENAME": os.environ.get(
            "AWS_S3_ALLOW_UNSAFE_RENAME", "true"
        ),
        "AWS_ALLOW_HTTP": os.environ.get("AWS_ALLOW_HTTP", "true"),
        "allow_http": os.environ.get("AWS_ALLOW_HTTP", "true"),
    }
    if endpoint:
        options["AWS_ENDPOINT_URL"] = endpoint

    ca = resolve_s3_ca_bundle()
    if ca:
        options["certificate_path"] = ca
    if resolve_s3_allow_invalid_certs():
        options["allow_invalid_certificates"] = "true"
        _emit_ssl_diag(False)
    elif ca:
        _emit_ssl_diag(ca)

    return options


def s3_client(**client_kwargs: Any) -> Any:
    """Cliente boto3 S3 alinhado a ``storage_options`` (endpoint + verify TLS).

    ``verify`` segue ``resolve_s3_ssl_verify()`` salvo se o chamador passar
    ``verify=`` explicitamente.
    """
    import boto3
    from botocore.client import Config

    options = storage_options()
    endpoint = (options.get("AWS_ENDPOINT_URL") or "").strip() or None
    verify = client_kwargs.pop("verify", resolve_s3_ssl_verify())
    _emit_ssl_diag(verify)

    kwargs: dict[str, Any] = {
        "service_name": "s3",
        "endpoint_url": endpoint,
        "aws_access_key_id": options.get("AWS_ACCESS_KEY_ID") or None,
        "aws_secret_access_key": options.get("AWS_SECRET_ACCESS_KEY") or None,
        "region_name": options.get("AWS_REGION") or "us-east-1",
        "config": client_kwargs.pop("config", None) or Config(signature_version="s3v4"),
        "verify": verify,
    }
    kwargs.update(client_kwargs)
    return boto3.client(**kwargs)


def delta_uri(path: str) -> str:
    clean = path.strip()
    return "s3://" + clean[len("s3a://") :] if clean.startswith("s3a://") else clean


def uri_from_parts(bucket: str, prefix: str) -> str:
    """Traduz (bucket, prefix) → URI s3a://."""
    b = bucket.strip().strip("/")
    p = prefix.strip().strip("/")
    return f"s3a://{b}/{p}" if p else f"s3a://{b}"


def _open_table(path: str) -> DeltaTable | None:
    try:
        return DeltaTable(delta_uri(path), storage_options=storage_options())
    except TableNotFoundError:
        return None


def is_delta_table(path: str) -> bool:
    return _open_table(path) is not None


def _protocol_has_timestamp_ntz(table: DeltaTable) -> bool:
    protocol = table.protocol()
    names: set[str] = set()
    for attr in ("writer_features", "reader_features"):
        for feat in getattr(protocol, attr, None) or []:
            names.add(str(feat).lower().replace("_", "").replace("-", ""))
    return "timestampntz" in names or "timestampwithouttimezone" in names


def _ensure_timestamp_ntz(table: DeltaTable | None) -> None:
    if table is None or _protocol_has_timestamp_ntz(table):
        return
    table.alter.add_feature(
        [TableFeatures.TimestampWithoutTimezone],
        allow_protocol_versions_increase=True,
    )
    print("[DELTA] feature TimestampWithoutTimezone habilitada", flush=True)


def _drop_feature_tblproperties(
    configuration: dict[str, str | None] | None,
) -> dict[str, str | None] | None:
    if not configuration:
        return configuration
    cleaned = {
        key: value
        for key, value in configuration.items()
        if not str(key).lower().startswith("delta.feature.")
    }
    return cleaned or None


def _frame_business_timestamps_to_ntz(df: pd.DataFrame) -> pd.DataFrame:
    """Colunas datetime aware SP → wall-clock naive só na borda de escrita."""
    out = df.copy()
    for col in out.columns:
        series = out[col]
        if not pd.api.types.is_datetime64_any_dtype(series):
            continue
        out[col] = series_to_sp_naive_wallclock(series)
    return out


def _as_arrow(data: Any) -> Any:
    if isinstance(data, pd.DataFrame):
        data = _frame_business_timestamps_to_ntz(data)
        table = pa.Table.from_pandas(data, preserve_index=False)
    elif isinstance(data, pa.Table):
        table = data
    else:
        return data
    fields = []
    changed = False
    for field in table.schema:
        if pa.types.is_timestamp(field.type) and field.type.tz is None:
            target = pa.timestamp("us")
            if field.type != target:
                fields.append(field.with_type(target))
                changed = True
            else:
                fields.append(field)
        elif pa.types.is_timestamp(field.type) and field.type.tz is not None:
            # Aware no Arrow → NTZ wall-clock SP
            fields.append(field.with_type(pa.timestamp("us")))
            changed = True
        else:
            fields.append(field)
    if changed:
        table = table.cast(pa.schema(fields))
    return _coerce_void_fields(table)


def _coerce_void_fields(table: pa.Table) -> pa.Table:
    """Converte colunas null/void (ou string toda nula) para string."""
    n = len(table)
    fields: list[pa.Field] = []
    cols: list[pa.Array] = []
    changed = False
    for field, col in zip(table.schema, table.columns, strict=False):
        as_null = _is_null_type(field.type) or (
            (
                isinstance(field.type, pa.DataType)
                and (pa.types.is_string(field.type) or pa.types.is_large_string(field.type))
            )
            and col.null_count == n
            and n > 0
        )
        if as_null:
            fields.append(pa.field(field.name, pa.string(), nullable=True))
            cols.append(pa.array([None] * n, type=pa.string()))
            changed = True
        else:
            fields.append(field)
            cols.append(col)
    return pa.Table.from_arrays(cols, schema=pa.schema(fields)) if changed else table


def _is_null_type(arrow_type: Any) -> bool:
    """Aceita pyarrow e arro3 (delta-rs recente) — `pa.types.is_null` exige `.id`."""
    if isinstance(arrow_type, pa.DataType):
        return bool(pa.types.is_null(arrow_type))
    type_s = str(arrow_type).lower()
    return (
        type_s in ("null", "void")
        or "nulltype" in type_s
        or type_s.endswith("(null)")
        or "data_type(null)" in type_s
    )


def _as_pa_data_type(arrow_type: Any) -> pa.DataType:
    """Normaliza tipo para `pa.DataType`; null/void → string."""
    if isinstance(arrow_type, pa.DataType):
        return pa.string() if pa.types.is_null(arrow_type) else arrow_type
    if _is_null_type(arrow_type):
        return pa.string()
    for attr in ("to_pyarrow", "into_pyarrow", "to_arrow"):
        fn = getattr(arrow_type, attr, None)
        if not callable(fn):
            continue
        try:
            converted = fn()
        except Exception:  # noqa: BLE001
            continue
        if isinstance(converted, pa.DataType):
            return pa.string() if pa.types.is_null(converted) else converted
    raise TypeError(f"tipo Arrow não conversível para pyarrow: {arrow_type!r}")


def _target_pyarrow_schema(delta: DeltaTable) -> pa.Schema | None:
    """Schema do target em pyarrow nativo."""
    try:
        return delta.to_pyarrow_dataset().schema
    except Exception:  # noqa: BLE001
        pass
    schema = delta.schema()
    to_pa = getattr(schema, "to_pyarrow", None) or getattr(schema, "to_arrow", None)
    if to_pa is None:
        return None
    try:
        raw = to_pa()
    except Exception:  # noqa: BLE001
        return None
    if isinstance(raw, pa.Schema):
        return raw
    # Schema arro3 / estrangeiro: monta pa.Schema campo a campo.
    try:
        fields = []
        for field in raw:
            fields.append(
                pa.field(field.name, _as_pa_data_type(field.type), nullable=True)
            )
        return pa.schema(fields)
    except Exception:  # noqa: BLE001
        return None


def _align_arrow_to_table(table: pa.Table, delta: DeltaTable) -> pa.Table:
    """Alinha o chunk ao schema existente; nunca faz cast para null/void."""
    n = len(table)
    raw_by_name = {name: table.column(name) for name in table.column_names}
    target = _target_pyarrow_schema(delta)

    if target is None:
        # Fallback: só nomes do Delta; tipos do source (ou string se coluna ausente/nula).
        names = [field.name for field in delta.schema().fields]
        fields: list[pa.Field] = []
        cols: list[pa.Array] = []
        for name in names:
            if name in raw_by_name:
                col = raw_by_name[name]
                if _is_null_type(col.type) or col.null_count == n:
                    fields.append(pa.field(name, pa.string(), nullable=True))
                    cols.append(pa.array([None] * n, type=pa.string()))
                else:
                    fields.append(pa.field(name, col.type, nullable=True))
                    cols.append(col)
            else:
                fields.append(pa.field(name, pa.string(), nullable=True))
                cols.append(pa.array([None] * n, type=pa.string()))
        target_names = set(names)
        for name in table.column_names:
            if name in target_names:
                continue
            col = table.column(name)
            field = table.schema.field(name)
            if _is_null_type(field.type) or col.null_count == n:
                fields.append(pa.field(name, pa.string(), nullable=True))
                cols.append(pa.array([None] * n, type=pa.string()))
            else:
                fields.append(field)
                cols.append(col)
        return pa.Table.from_arrays(cols, schema=pa.schema(fields))

    fields = []
    cols = []
    for tgt in target:
        tgt_type = _as_pa_data_type(tgt.type)
        name = tgt.name
        if name in raw_by_name:
            col = raw_by_name[name]
            try:
                if _is_null_type(col.type) or col.null_count == n:
                    cols.append(pa.array([None] * n, type=tgt_type))
                elif col.type == tgt_type:
                    cols.append(col)
                else:
                    cols.append(col.cast(tgt_type, safe=False))
            except Exception:  # noqa: BLE001
                cols.append(pa.array([None] * n, type=tgt_type))
        else:
            cols.append(pa.array([None] * n, type=tgt_type))
        fields.append(pa.field(name, tgt_type, nullable=True))

    target_names = {f.name for f in target}
    for name in table.column_names:
        if name in target_names:
            continue
        col = table.column(name)
        field = table.schema.field(name)
        if _is_null_type(field.type) or col.null_count == n:
            fields.append(pa.field(name, pa.string(), nullable=True))
            cols.append(pa.array([None] * n, type=pa.string()))
        else:
            fields.append(field)
            cols.append(col)

    return pa.Table.from_arrays(cols, schema=pa.schema(fields))


def _is_void_schema_error(exc: BaseException) -> bool:
    msg = str(exc)
    low = msg.lower()
    return "to Null" in msg or "to null" in low or "void" in low


def _needs_timestamp_ntz_bootstrap(exc: BaseException) -> bool:
    msg = str(exc).lower()
    if "parsing property" in msg:
        return False
    return "timestampntz" in msg.replace("_", "") or "timestamp without timezone" in msg


def _bootstrap_timestamp_ntz(uri: str, options: dict[str, str] | None) -> None:
    stub = pa.table({"_delta_bootstrap": pa.array([0], type=pa.int32())})
    write_deltalake(
        uri, stub, mode="overwrite", schema_mode="overwrite", storage_options=options
    )
    table = DeltaTable(uri, storage_options=options)
    _ensure_timestamp_ntz(table)


def _write_deltalake(table_or_uri: Any, data: Any, **kwargs: Any) -> None:
    cfg = _drop_feature_tblproperties(kwargs.pop("configuration", None))
    if cfg:
        kwargs["configuration"] = cfg
    data = _as_arrow(data)
    try:
        write_deltalake(table_or_uri, data, **kwargs)
        return
    except Exception as exc:
        if not _needs_timestamp_ntz_bootstrap(exc):
            raise
        uri = table_or_uri if isinstance(table_or_uri, str) else delta_uri(str(table_or_uri))
        options = kwargs.get("storage_options")
        print(
            "[DELTA] CREATE sem timestampNtz no protocolo — bootstrap da feature e retry",
            flush=True,
        )
        _bootstrap_timestamp_ntz(str(uri), options)
        kwargs["mode"] = "overwrite"
        kwargs["schema_mode"] = "overwrite"
        write_deltalake(str(uri), data, **kwargs)


def _align_column_case(df: pd.DataFrame, target_columns: Sequence[str]) -> pd.DataFrame:
    target_by_lower = {name.lower(): name for name in target_columns}
    renames = {
        source: target_by_lower[source.lower()]
        for source in df.columns
        if source.lower() in target_by_lower
    }
    return df.rename(columns=renames)


def _frame_for_delta_write(
    df: pd.DataFrame,
    target_columns: Sequence[str],
    *,
    fill_missing_columns: bool = False,
) -> pa.Table:
    aligned = _align_column_case(df, target_columns)
    extras = [name for name in aligned.columns if name not in target_columns]
    if extras:
        print(f"[DELTA] descartando colunas extras na escrita: {extras}", flush=True)
        aligned = aligned.drop(columns=extras)
    missing = [name for name in target_columns if name not in aligned.columns]
    if missing:
        if not fill_missing_columns:
            raise ValueError(f"Colunas ausentes para escrita Delta: {missing}")
        for name in missing:
            aligned[name] = pd.NA
    ordered = aligned.loc[:, list(target_columns)]
    return _as_arrow(ordered)


def _resolve_columns(columns: Sequence[str], requested: Sequence[str]) -> list[str]:
    by_lower = {name.lower(): name for name in columns}
    resolved: list[str] = []
    for name in requested:
        actual = by_lower.get(name.lower())
        if not actual:
            raise ValueError(f"Coluna Delta obrigatória ausente: {name}.")
        resolved.append(actual)
    return resolved


def _is_commit_conflict(exc: BaseException) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in _COMMIT_CONFLICT_MARKERS)


def _void_delta_columns(table: DeltaTable) -> set[str]:
    schema = table.schema()
    for attr in ("to_arrow", "to_pyarrow"):
        convert = getattr(schema, attr, None)
        if convert is None:
            continue
        try:
            return {f.name for f in convert() if _is_null_type(f.type)}
        except Exception:
            pass
    try:
        return {f.name for f in table.to_pyarrow_dataset().schema if _is_null_type(f.type)}
    except Exception:
        pass
    names: set[str] = set()
    for field in schema.fields:
        type_name = str(field.type).lower()
        if type_name in ("null", "void") or "nulltype" in type_name or '"null"' in type_name:
            names.add(field.name)
    return names


def _drop_void_columns(
    source: pd.DataFrame,
    void_cols: set[str],
    *,
    required: Sequence[str],
    path: str,
) -> pd.DataFrame:
    if not void_cols:
        return source

    strategy = _void_strategy()
    required_lower = {name.lower() for name in required}
    blocking = sorted(name for name in void_cols if name.lower() in required_lower)
    if blocking:
        raise RuntimeError(
            f"Chave de MERGE com tipo void em {path}: {blocking}. "
            "Reescreva a tabela (carga full)."
        )
    if strategy == "fail":
        raise RuntimeError(
            f"Colunas void em {path}: {sorted(void_cols)}. Reescreva a tabela (carga full)."
        )

    by_lower = {name.lower(): name for name in source.columns}
    present = [by_lower[name.lower()] for name in void_cols if name.lower() in by_lower]
    with_data = sorted(name for name in present if source[name].notna().any())
    if with_data and strategy != "force":
        raise RuntimeError(
            f"Colunas void em {path} com valor no source: {with_data} — o MERGE as "
            "descartaria. Reescreva a tabela (carga full) ou use "
            "DATAOPS_DELTA_VOID_STRATEGY=force para ignorar esses valores."
        )
    print(
        f"[DELTA] void no target {sorted(void_cols)} — fora do MERGE (seguem nulas); "
        "reescreva a tabela (carga full) para corrigir o schema",
        flush=True,
    )
    return source.drop(columns=present) if present else source


def _partition_predicate(source: pd.DataFrame, partition_col: str) -> str | None:
    try:
        col = _resolve_columns(list(source.columns), [partition_col])[0]
    except ValueError:
        return None
    values = sorted(
        {
            str(value).strip()
            for value in source[col].dropna().unique()
            if str(value).strip()
        }
    )
    if not values:
        return None
    for value in values:
        if not all(ch.isalnum() or ch in "-_" for ch in value):
            return None
    if len(values) == 1:
        return f"target.`{col}` = '{values[0]}'"
    listed = ", ".join(f"'{value}'" for value in values)
    return f"target.`{col}` IN ({listed})"


def _execute_merge(
    table: DeltaTable,
    source: pd.DataFrame,
    *,
    merge_keys: Sequence[str],
    partition_col: str | None = None,
) -> None:
    keys = _resolve_columns(list(source.columns), merge_keys)
    parts = [f"target.`{key}` = source.`{key}`" for key in keys]
    if partition_col:
        partition_filter = _partition_predicate(source, partition_col)
        if partition_filter:
            parts.append(partition_filter)
    predicate = " AND ".join(parts)
    (
        table.merge(
            source=_as_arrow(source),
            predicate=predicate,
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )


def merge(
    df: pd.DataFrame,
    path: str,
    *,
    merge_keys: Sequence[str],
    partition_col: str | None = None,
) -> int:
    """MERGE idempotente em qualquer URI Delta (bronze/prata/ouro).

    `path` é URI `s3a://…` — a camada/bucket não entra na API.
    `partition_col` opcional (ex.: tabela multi-origem).
    """
    if df.empty:
        return 0

    uri = delta_uri(path)
    options = storage_options()
    table = _open_table(path)
    if table is None:
        write_kwargs: dict[str, Any] = {
            "mode": "overwrite",
            "schema_mode": "overwrite",
            "storage_options": options,
        }
        if partition_col:
            partition_actual = _resolve_columns(list(df.columns), [partition_col])[0]
            write_kwargs["partition_by"] = [partition_actual]
        _write_deltalake(uri, df, **write_kwargs)
        _ensure_timestamp_ntz(_open_table(path))
        return len(df)

    _ensure_timestamp_ntz(table)
    target_columns = [field.name for field in table.schema().fields]
    source = _align_column_case(df, target_columns)
    required = list(merge_keys) + ([partition_col] if partition_col else [])
    source = _drop_void_columns(
        source, _void_delta_columns(table), required=required, path=path
    )

    last_error: BaseException | None = None
    attempts = _merge_retries()
    base_sec = _merge_retry_sec()
    for attempt in range(1, attempts + 1):
        try:
            current = table if attempt == 1 else _open_table(path)
            if current is None:
                raise RuntimeError(f"Tabela Delta sumiu durante o MERGE: {path}")
            _ensure_timestamp_ntz(current)
            _execute_merge(
                current, source, merge_keys=merge_keys, partition_col=partition_col
            )
            return len(source)
        except Exception as exc:
            last_error = exc
            if not _is_commit_conflict(exc) or attempt >= attempts:
                raise
            delay = base_sec * (2 ** (attempt - 1))
            print(
                f"[DELTA] commit conflitante (tentativa {attempt}/{attempts}); "
                f"retry em {delay:.1f}s: {str(exc).replace(chr(10), ' ')[:200]}",
                flush=True,
            )
            time.sleep(delay)

    assert last_error is not None
    raise last_error


def overwrite(
    df: pd.DataFrame,
    path: str,
    *,
    predicate: str | None = None,
) -> int:
    """Sobrescreve a tabela Delta em `path` (qualquer camada).

    Com `predicate` (replaceWhere): sobrescreve só a fatia que casa o predicado.
    Se o schema existente tiver colunas void/Null, faz overwrite full deste `df`
    e recria o schema.
    """
    if df.empty and not predicate:
        return 0

    uri = delta_uri(path)
    options = storage_options()
    table = _open_table(path)

    if not predicate:
        _ensure_timestamp_ntz(table)
        _write_deltalake(
            uri,
            df,
            mode="overwrite",
            schema_mode="overwrite",
            storage_options=options,
        )
        return len(df)

    arrow = _as_arrow(df)
    if table is None:
        _write_deltalake(
            uri,
            arrow,
            mode="overwrite",
            schema_mode="overwrite",
            storage_options=options,
        )
        return len(df)

    void_cols = _void_delta_columns(table)
    if void_cols:
        print(
            f"[DELTA] void no schema {path}: {sorted(void_cols)} — "
            "overwrite full (recria schema; outras fatias somem até reload)",
            flush=True,
        )
        _write_deltalake(
            uri,
            arrow,
            mode="overwrite",
            schema_mode="overwrite",
            storage_options=options,
        )
        return len(df)

    _ensure_timestamp_ntz(table)
    arrow = _align_arrow_to_table(arrow, table)
    try:
        _write_deltalake(
            uri,
            arrow,
            mode="overwrite",
            predicate=predicate,
            storage_options=options,
        )
    except Exception as exc:  # noqa: BLE001
        if not _is_void_schema_error(exc):
            raise
        print(
            f"[DELTA] SchemaMismatch void/Null em {path} ({exc}) — "
            "overwrite full heal",
            flush=True,
        )
        _write_deltalake(
            uri,
            arrow,
            mode="overwrite",
            schema_mode="overwrite",
            storage_options=options,
        )
    return len(df)


def append(
    df: pd.DataFrame,
    path: str,
    *,
    fill_missing_columns: bool = False,
) -> int:
    """Append em `path` (qualquer camada)."""
    if df.empty:
        return 0

    uri = delta_uri(path)
    options = storage_options()
    table = _open_table(path)
    if table is None:
        return overwrite(df, path)

    void_cols = _void_delta_columns(table)
    if void_cols:
        raise RuntimeError(
            f"Colunas void/Null no schema Delta {path}: {sorted(void_cols)}. "
            "Reescreva a tabela (overwrite full / carga completa) — "
            "append não pode evoluir Null→Utf8."
        )

    _ensure_timestamp_ntz(table)
    target_columns = [field.name for field in table.schema().fields]
    arrow = _frame_for_delta_write(
        df, target_columns, fill_missing_columns=fill_missing_columns
    )
    _write_deltalake(uri, arrow, mode="append", storage_options=options)
    return len(df)


def vacuum(
    path: str,
    *,
    retention_hours: int | None = None,
    dry_run: bool = False,
) -> int:
    """Vacuum em qualquer URI Delta."""
    hours = retention_hours
    if hours is None:
        hours = int(os.environ.get("DATAOPS_DELTA_VACUUM_RETENTION_HOURS") or "168")

    table = _open_table(path)
    if table is None:
        print(f"[VACUUM] tabela ausente, pulando: {path}", flush=True)
        return 0

    deleted = table.vacuum(
        retention_hours=hours,
        dry_run=dry_run,
        enforce_retention_duration=False,
    )
    removed = len(deleted) if deleted is not None else 0
    if dry_run:
        print(
            f"[VACUUM][dry-run] {path}: {removed} arquivo(s) seriam removidos "
            f"(retain={hours}h)",
            flush=True,
        )
    else:
        print(
            f"[VACUUM] {path}: {removed} arquivo(s) removido(s) (retain={hours}h)",
            flush=True,
        )
    return removed


def optimize_compact(
    path: str,
    *,
    target_size: int | None = None,
) -> dict[str, Any]:
    """Compacta small files (OPTIMIZE). Os arquivos antigos saem do storage no vacuum."""
    table = _open_table(path)
    if table is None:
        print(f"[OPTIMIZE] tabela ausente, pulando: {path}", flush=True)
        return {}

    kwargs: dict[str, Any] = {}
    if target_size is not None:
        kwargs["target_size"] = int(target_size)

    metrics = table.optimize.compact(**kwargs)
    print(f"[OPTIMIZE] {path}: {metrics}", flush=True)
    return dict(metrics) if isinstance(metrics, dict) else {"result": metrics}


def cleanup_log(
    path: str,
    *,
    log_retention_days: int | None = None,
) -> dict[str, Any]:
    """Checkpoint + limpeza de commits expirados no `_delta_log`.

    Usa `delta.logRetentionDuration` (default Delta: 30 dias). Se
    `log_retention_days` for informado, atualiza a property antes do cleanup.
    """
    table = _open_table(path)
    if table is None:
        print(f"[LOG-CLEANUP] tabela ausente, pulando: {path}", flush=True)
        return {"skipped": True}

    before = int(table.version())
    if log_retention_days is not None:
        days = max(1, int(log_retention_days))
        prop = f"interval {days} days"
        table.alter.set_table_properties({"delta.logRetentionDuration": prop})
        print(f"[LOG-CLEANUP] {path}: logRetentionDuration={prop}", flush=True)

    table.create_checkpoint()
    table.cleanup_metadata()
    # Tamanho do histórico após o cleanup, limitado a 500 versões.
    after_hist = 0
    try:
        after_hist = len(table.history(limit=500))
    except Exception as exc:  # noqa: BLE001
        print(f"[LOG-CLEANUP] history pós-cleanup falhou em {path}: {exc}", flush=True)

    out = {"version": before, "history_len_capped": after_hist}
    print(f"[LOG-CLEANUP] {path}: checkpoint+cleanup ok {out}", flush=True)
    return out


# Aliases com nome de camada.
merge_bronze = merge
overwrite_bronze = overwrite
append_bronze = append
vacuum_bronze = vacuum


def count_key_duplicates(path: str, *, key_cols: Sequence[str]) -> dict[str, int]:
    table = _open_table(path)
    if table is None:
        return {"total": 0, "unique": 0, "duplicates": 0}

    schema_names = [field.name for field in table.schema().fields]
    resolved = _resolve_columns(schema_names, key_cols)
    frame = table.to_pandas(columns=resolved)
    if frame.empty:
        return {"total": 0, "unique": 0, "duplicates": 0}

    total = len(frame)
    unique = int(frame.drop_duplicates(subset=resolved).shape[0])
    return {"total": total, "unique": unique, "duplicates": total - unique}


def count_rows(path: str) -> int:
    table = _open_table(path)
    if table is None:
        return 0
    return int(table.to_pyarrow_dataset().count_rows())


def dedupe_table(
    path: str,
    *,
    key_cols: Sequence[str],
    version_col: str = "_extracted_at",
    date_col: str = "dt_atualz",
    since: str | pd.Timestamp | None = "2023-01-01",
    dry_run: bool = False,
) -> dict[str, int]:
    table = _open_table(path)
    if table is None:
        raise RuntimeError(f"Tabela Delta ausente: {path}")

    schema_names = [field.name for field in table.schema().fields]
    keys = _resolve_columns(schema_names, key_cols)
    version_actual = next(
        (name for name in schema_names if name.lower() == version_col.lower()),
        None,
    )
    date_actual = next(
        (name for name in schema_names if name.lower() == date_col.lower()),
        None,
    )

    frame = table.to_pandas()
    before = len(frame)
    if frame.empty:
        return {
            "before": 0,
            "after": 0,
            "removed": 0,
            "purged_pre": 0,
            "deduped": 0,
        }

    working = frame
    purged_pre = 0
    if since is not None and date_actual is not None:
        since_ts = pd.Timestamp(since)
        dates = series_to_sp_naive_wallclock(working[date_actual])
        if since_ts.tzinfo is not None:
            since_cmp = since_ts.tz_convert(TZ_SP).tz_localize(None)
        else:
            since_cmp = since_ts
        keep_mask = dates.notna() & (dates >= since_cmp)
        purged_pre = int((~keep_mask).sum())
        working = working.loc[keep_mask].copy()
        print(
            f"[DEDUPE] purge pré-{since_cmp.date()} "
            f"→ removidas={purged_pre} restantes={len(working)}",
            flush=True,
        )

    sort_cols = [version_actual] if version_actual else []
    ascending = [False] if version_actual else []
    for key in keys:
        if key not in sort_cols:
            sort_cols.append(key)
            ascending.append(True)

    if sort_cols and not working.empty:
        ordered = working.sort_values(
            by=sort_cols, ascending=ascending, na_position="last", kind="mergesort"
        )
    else:
        ordered = working

    if working.empty:
        deduped = working
        deduped_n = 0
    else:
        deduped = ordered.drop_duplicates(subset=keys, keep="first").copy()
        deduped_n = max(0, (before - purged_pre) - len(deduped))
    after = len(deduped)
    removed = before - after

    print(
        f"[DEDUPE] before={before} after={after} removed={removed} "
        f"(pre={purged_pre} dup={deduped_n}) keys={list(keys)}",
        flush=True,
    )
    if dry_run or removed == 0:
        if dry_run:
            print("[DEDUPE] dry-run — nenhuma escrita", flush=True)
        else:
            print("[DEDUPE] sem alteração — nenhuma escrita", flush=True)
        return {
            "before": before,
            "after": after,
            "removed": removed,
            "purged_pre": purged_pre,
            "deduped": deduped_n,
        }

    arrow = _frame_for_delta_write(deduped, schema_names)
    _ensure_timestamp_ntz(table)
    _write_deltalake(
        delta_uri(path), arrow, mode="overwrite", storage_options=storage_options()
    )
    print(
        f"[DEDUPE] overwrite → {after} linhas / {arrow.num_columns} colunas",
        flush=True,
    )
    return {
        "before": before,
        "after": after,
        "removed": removed,
        "purged_pre": purged_pre,
        "deduped": deduped_n,
    }
