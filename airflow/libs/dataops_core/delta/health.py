"""Saúde operacional de buckets MinIO e tabelas Delta (sem Spark/OPTIMIZE)."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from dataops_core.delta.io import _open_table, s3_client

__all__ = [
    "DEFAULT_MEDALLION_BUCKETS",
    "DEFAULT_THRESHOLDS",
    "bucket_usage",
    "delta_uri_from_object_key",
    "evaluate_health",
    "growth_pct",
    "is_ephemeral_delta_uri",
    "is_temp_object_key",
    "resolve_targets",
    "table_stats",
]

_HISTORY_CAP = 500

# Buckets usados quando a Variable `delta_ops` omite `buckets`.
DEFAULT_MEDALLION_BUCKETS: tuple[str, ...] = ("bronze", "prata", "ouro")

# Limiares sobrescreviveis via Variable `delta_ops.thresholds`.
DEFAULT_THRESHOLDS: dict[str, float | int] = {
    "bucket_growth_pct": 40,
    "table_growth_pct": 50,
    "max_num_files": 5000,
    # Histórico: cap do inventário (_HISTORY_CAP) vs limiar de alerta
    "max_history_len": 450,
    "assumed_log_retention_days": 7,
    "max_commits_per_day": 60,
    # Fragmentação: só avaliada acima de um volume mínimo
    "min_avg_file_bytes": 1_048_576,
    "min_files_for_small_files": 60,
    "min_table_bytes_for_small_files": 83_886_080,  # 80 MiB
    # Qualidade de carga
    "max_loose_bytes_pct": 15,
    "max_loose_bytes": 5_368_709_120,  # 5 GiB
    "max_ephemeral_delta_tables": 0,  # qualquer delta "por run" já alerta
}

_EPHEMERAL_MARKERS = ("run_id=", "/_meta/")
_TEMP_KEY_MARKERS = (
    "/_tmp/",
    "/tmp/",
    "/.trash/",
    "/_staging/",
    "/staging/",
    "/__pycache__/",
)


def growth_pct(current: float | int | None, previous: float | int | None) -> float | None:
    """Crescimento percentual de `previous` → `current`. None se não comparável."""
    if current is None or previous is None:
        return None
    prev = float(previous)
    cur = float(current)
    if prev <= 0:
        return None if cur <= 0 else 100.0
    return ((cur - prev) / prev) * 100.0


def table_stats(path: str, *, history_limit: int = _HISTORY_CAP) -> dict[str, Any]:
    """Métricas leves de uma URI Delta (fragmentação / versão / tamanho)."""
    table = _open_table(path)
    if table is None:
        return {
            "path": path,
            "is_delta": False,
            "version": None,
            "num_files": 0,
            "history_len": 0,
            "size_bytes": 0,
            "avg_file_bytes": None,
        }

    num_files, size_bytes = _file_stats(table)
    history_len = 0
    try:
        history_len = len(table.history(limit=history_limit))
    except Exception as exc:  # noqa: BLE001
        print(f"[DELTA-HEALTH] history falhou em {path}: {exc}", flush=True)

    avg = (size_bytes / num_files) if num_files and size_bytes else None
    return {
        "path": path,
        "is_delta": True,
        "version": int(table.version()),
        "num_files": num_files,
        "history_len": history_len,
        "size_bytes": size_bytes,
        "avg_file_bytes": avg,
    }


def _file_stats(table: Any) -> tuple[int, int]:
    """(num_files, size_bytes) via API deltalake 1.x (sem `.files()`)."""
    actions = None
    try:
        actions = table.get_add_actions(flatten=True)
    except TypeError:
        try:
            actions = table.get_add_actions()
        except Exception:  # noqa: BLE001
            actions = None
    except Exception:  # noqa: BLE001
        actions = None

    if actions is not None and hasattr(actions, "num_rows"):
        num_files = int(actions.num_rows)
        size_bytes = 0
        names = [field.name for field in actions.schema]
        size_col = next(
            (name for name in names if name.lower() in ("size_bytes", "size")),
            None,
        )
        if size_col is not None:
            col = actions.column(size_col)
            for i in range(len(col)):
                val = col[i].as_py()
                if val is not None:
                    size_bytes += int(val)
        return num_files, size_bytes

    for attr in ("file_uris", "files"):
        method = getattr(table, attr, None)
        if not callable(method):
            continue
        try:
            listed = list(method())
            return len(listed), 0
        except Exception:  # noqa: BLE001
            continue
    return 0, 0


def _sum_add_action_sizes(table: Any) -> int:
    """Compat: só o tamanho; preferir `_file_stats`."""
    return _file_stats(table)[1]


def delta_uri_from_object_key(bucket: str, key: str) -> str | None:
    """Se a chave pertence a `_delta_log`, devolve a URI s3a:// da tabela."""
    b = bucket.strip().strip("/")
    k = (key or "").lstrip("/")
    marker = "/_delta_log"
    if marker not in f"/{k}":
        return None
    # Normaliza: ".../tabela/_delta_log/..." → root ".../tabela"
    padded = k if k.startswith("/") else f"/{k}"
    idx = padded.find("/_delta_log")
    if idx < 0:
        return None
    root = padded[:idx].lstrip("/")
    return f"s3a://{b}/{root}" if root else f"s3a://{b}"


def is_ephemeral_delta_uri(uri: str) -> bool:
    """True se a URI for de uma tabela Delta por execução (`run_id=`) ou meta aninhada."""
    path = uri.split("://", 1)[-1].lower()
    if "run_id=" in path:
        return True
    if "/_meta/" in path and ("system=" in path or "run_id=" in path):
        return True
    return False


def is_temp_object_key(key: str) -> bool:
    padded = f"/{(key or '').lstrip('/').lower()}"
    return any(marker in padded for marker in _TEMP_KEY_MARKERS)


def _uri_to_root_prefix(uri: str, bucket: str) -> str:
    b = bucket.strip().strip("/")
    prefix = f"s3a://{b}/"
    if uri.startswith(prefix):
        return uri[len(prefix) :].strip("/")
    if uri.rstrip("/") == f"s3a://{b}":
        return ""
    return uri.split("://", 1)[-1].split("/", 1)[-1].strip("/")


def _key_under_root(key: str, root: str) -> bool:
    k = key.lstrip("/")
    if root == "":
        return True
    return k == root or k.startswith(root + "/")


def bucket_usage(bucket: str, prefix: str = "") -> dict[str, Any]:
    """Inventário S3-compat: bytes, Delta, e qualidade de carga (lixo / efêmero).

    TLS/MinIO: usa ``dataops_core.delta.s3_client`` (CA via ``DATAOPS_S3_CA_BUNDLE`` /
    ``AWS_CA_BUNDLE``, ou ``DATAOPS_S3_ALLOW_INVALID_CERTS`` só em lab).
    """
    client = s3_client()

    b = bucket.strip().strip("/")
    p = prefix.strip().lstrip("/")
    entries: list[tuple[str, int]] = []
    delta_uris: set[str] = set()
    paginator = client.get_paginator("list_objects_v2")
    kwargs: dict[str, Any] = {"Bucket": b}
    if p:
        kwargs["Prefix"] = p if p.endswith("/") else f"{p}/"

    try:
        pages = paginator.paginate(**kwargs)
        for page in pages:
            for obj in page.get("Contents") or []:
                key = str(obj.get("Key") or "")
                size = int(obj.get("Size") or 0)
                entries.append((key, size))
                uri = delta_uri_from_object_key(b, key)
                if uri:
                    delta_uris.add(uri)
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        if "CERTIFICATE_VERIFY_FAILED" in msg or "SSL" in type(exc).__name__:
            raise RuntimeError(
                f"Falha TLS ao listar s3a://{b}: {exc}. "
                "Monte a CA do MinIO em DATAOPS_S3_CA_BUNDLE ou AWS_CA_BUNDLE "
                "(preferível), ou — só em lab — DATAOPS_S3_ALLOW_INVALID_CERTS=true."
            ) from exc
        raise

    roots = sorted(
        (_uri_to_root_prefix(uri, b) for uri in delta_uris),
        key=len,
        reverse=True,
    )
    ephemeral_uris = sorted(uri for uri in delta_uris if is_ephemeral_delta_uri(uri))
    ephemeral_roots = {
        _uri_to_root_prefix(uri, b) for uri in ephemeral_uris
    }

    object_count = len(entries)
    total_bytes = 0
    delta_bytes = 0
    loose_bytes = 0
    loose_objects = 0
    temp_bytes = 0
    temp_objects = 0
    ephemeral_bytes = 0
    loose_samples: list[str] = []

    for key, size in entries:
        total_bytes += size
        under_delta = any(_key_under_root(key, root) for root in roots)
        if under_delta:
            delta_bytes += size
            if any(_key_under_root(key, root) for root in ephemeral_roots):
                ephemeral_bytes += size
        else:
            loose_bytes += size
            loose_objects += 1
            if len(loose_samples) < 8:
                loose_samples.append(key)
        if is_temp_object_key(key):
            temp_bytes += size
            temp_objects += 1

    loose_pct = (loose_bytes / total_bytes * 100.0) if total_bytes else 0.0
    print(
        f"[DELTA-HEALTH] bucket=s3a://{b}"
        + (f"/{p}" if p else "")
        + f" objects={object_count} bytes={total_bytes} delta_tables={len(delta_uris)}"
        + f" loose_bytes={loose_bytes} ({loose_pct:.1f}%)"
        + f" ephemeral_delta={len(ephemeral_uris)} ephemeral_bytes={ephemeral_bytes}"
        + f" temp_objects={temp_objects}",
        flush=True,
    )
    return {
        "bucket": b,
        "prefix": p,
        "object_count": object_count,
        "total_bytes": total_bytes,
        "delta_uris": sorted(delta_uris),
        "delta_bytes": delta_bytes,
        "loose_bytes": loose_bytes,
        "loose_objects": loose_objects,
        "loose_pct": loose_pct,
        "loose_samples": loose_samples,
        "ephemeral_delta_uris": ephemeral_uris,
        "ephemeral_bytes": ephemeral_bytes,
        "temp_bytes": temp_bytes,
        "temp_objects": temp_objects,
    }


def evaluate_health(
    metrics: Mapping[str, Any],
    thresholds: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Gera alertas a partir de métricas já agregadas; não levanta exceção.

    `small_files` só dispara acima do mínimo de arquivos e bytes. Histórico
    longo vira `stale_log` (no cap) ou `high_commit_rate` (abaixo do cap / taxa
    alta).
    """
    th = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    alerts: list[dict[str, Any]] = []

    for bucket_row in metrics.get("buckets") or []:
        key = f"s3a://{bucket_row.get('bucket', '')}/{bucket_row.get('prefix', '')}".rstrip(
            "/"
        )
        growth = bucket_row.get("growth_pct")
        limit = float(th["bucket_growth_pct"])
        if growth is not None and growth >= limit:
            alerts.append(
                {
                    "severity": "warn",
                    "code": "bucket_growth",
                    "target": key,
                    "message": (
                        f"Bucket cresceu {growth:.1f}% "
                        f"(limiar {limit:g}%; "
                        f"{bucket_row.get('total_bytes', 0)} bytes, "
                        f"{bucket_row.get('object_count', 0)} objetos)"
                    ),
                }
            )

        loose_bytes = int(bucket_row.get("loose_bytes") or 0)
        loose_pct = float(bucket_row.get("loose_pct") or 0.0)
        loose_objects = int(bucket_row.get("loose_objects") or 0)
        max_loose_pct = float(th["max_loose_bytes_pct"])
        max_loose_bytes = int(th["max_loose_bytes"])
        if loose_bytes > 0 and (
            loose_pct >= max_loose_pct or loose_bytes >= max_loose_bytes
        ):
            samples = list(bucket_row.get("loose_samples") or [])[:5]
            sample_txt = ", ".join(samples) if samples else "(sem amostra)"
            alerts.append(
                {
                    "severity": "warn",
                    "code": "loose_load_volume",
                    "target": key,
                    "message": (
                        f"Objetos fora de tabelas Delta: {loose_bytes} bytes "
                        f"({loose_pct:.1f}%, {loose_objects} objs) — "
                        f"carga/dump inapropriado. Amostras: {sample_txt}"
                    ),
                }
            )

        ephemeral = list(bucket_row.get("ephemeral_delta_uris") or [])
        max_eph = int(th["max_ephemeral_delta_tables"])
        if len(ephemeral) > max_eph:
            eph_bytes = int(bucket_row.get("ephemeral_bytes") or 0)
            alerts.append(
                {
                    "severity": "warn",
                    "code": "ephemeral_delta_load",
                    "target": key,
                    "message": (
                        f"{len(ephemeral)} tabela(s) Delta de carga efêmera "
                        f"(run_id= / _meta aninhado), {eph_bytes} bytes — "
                        "padrão de carga que gera volume desnecessário"
                    ),
                }
            )
            for uri in ephemeral:
                alerts.append(
                    {
                        "severity": "info",
                        "code": "ephemeral_delta_table",
                        "target": uri,
                        "message": (
                            "Delta por execução/meta aninhada; "
                            "candidato a limpeza após consolidar inventário"
                        ),
                    }
                )

        temp_objects = int(bucket_row.get("temp_objects") or 0)
        temp_bytes = int(bucket_row.get("temp_bytes") or 0)
        if temp_objects > 0:
            alerts.append(
                {
                    "severity": "warn",
                    "code": "temp_load_volume",
                    "target": key,
                    "message": (
                        f"{temp_objects} objeto(s) em path temporário "
                        f"(_tmp/staging/trash), {temp_bytes} bytes"
                    ),
                }
            )

    for table_row in metrics.get("tables") or []:
        path = str(table_row.get("path") or "")
        if not table_row.get("is_delta", True):
            alerts.append(
                {
                    "severity": "warn",
                    "code": "not_delta",
                    "target": path,
                    "message": "URI configurada não é tabela Delta",
                }
            )
            continue

        growth = table_row.get("growth_pct")
        t_limit = float(th["table_growth_pct"])
        if growth is not None and growth >= t_limit:
            alerts.append(
                {
                    "severity": "warn",
                    "code": "table_growth",
                    "target": path,
                    "message": (
                        f"Tabela cresceu {growth:.1f}% "
                        f"(limiar {t_limit:g}%; "
                        f"{table_row.get('size_bytes', 0)} bytes)"
                    ),
                }
            )

        num_files = int(table_row.get("num_files") or 0)
        max_files = int(th["max_num_files"])
        if num_files >= max_files:
            alerts.append(
                {
                    "severity": "warn",
                    "code": "too_many_files",
                    "target": path,
                    "message": (
                        f"{num_files} arquivos de dados "
                        f"(limiar {max_files}; possível fragmentação)"
                    ),
                }
            )

        history_len = int(table_row.get("history_len") or 0)
        max_hist = int(th["max_history_len"])
        history_cap = int(th.get("history_cap", _HISTORY_CAP))
        assumed_days = max(1, int(th["assumed_log_retention_days"]))
        max_cpd = float(th["max_commits_per_day"])
        commits_per_day = history_len / assumed_days

        # No teto do inventário → warn; abaixo do teto e acima do limiar → info.
        if history_len >= history_cap:
            alerts.append(
                {
                    "severity": "warn",
                    "code": "stale_log",
                    "target": path,
                    "message": (
                        f"Histórico no teto do inventário ({history_len}+ versões; "
                        f"cap {history_cap}). Rode ops_delta_log_cleanup "
                        f"(vacuum não enxuga o _delta_log)."
                    ),
                }
            )
        elif history_len >= max_hist or commits_per_day >= max_cpd:
            alerts.append(
                {
                    "severity": "info",
                    "code": "high_commit_rate",
                    "target": path,
                    "message": (
                        f"Histórico com {history_len} versões "
                        f"(~{commits_per_day:.0f}/dia assumindo "
                        f"{assumed_days}d de retenção; limiar "
                        f"{max_hist} versões ou {max_cpd:g}/dia). "
                        "Revisar frequência de escrita; cleanup periódico "
                        "via ops_delta_log_cleanup."
                    ),
                }
            )

        avg = table_row.get("avg_file_bytes")
        min_avg = float(th["min_avg_file_bytes"])
        min_files = int(th["min_files_for_small_files"])
        min_table_bytes = int(th["min_table_bytes_for_small_files"])
        size_bytes = int(table_row.get("size_bytes") or 0)
        if (
            avg is not None
            and num_files >= min_files
            and size_bytes >= min_table_bytes
            and float(avg) < min_avg
        ):
            alerts.append(
                {
                    "severity": "info",
                    "code": "small_files",
                    "target": path,
                    "message": (
                        f"Arquivo médio {int(avg)} bytes "
                        f"(abaixo de {int(min_avg)}; "
                        f"{num_files} arquivos, {size_bytes} bytes — "
                        "fragmentação; candidato a ops_delta_optimize)"
                    ),
                }
            )

    return alerts


def resolve_targets(
    config: Mapping[str, Any] | None,
    *,
    table_resolver: Callable[[str], Sequence[tuple[str, str]]] | None = None,
) -> dict[str, Any]:
    """Normaliza Variable `delta_ops` → buckets, URIs, limiares, vacuum.

    Sem `buckets` (ausente ou lista vazia), usa `bronze` / `prata` / `ouro`.
    """
    cfg = dict(config or {})
    raw_buckets = cfg.get("buckets")
    if raw_buckets is None or (isinstance(raw_buckets, (list, tuple)) and len(raw_buckets) == 0):
        buckets = list(DEFAULT_MEDALLION_BUCKETS)
    else:
        buckets = [
            str(b).strip().strip("/") for b in raw_buckets if str(b).strip()
        ] or list(DEFAULT_MEDALLION_BUCKETS)
    extra_uris = [str(u).strip() for u in (cfg.get("extra_uris") or []) if str(u).strip()]
    projects = [str(p).strip() for p in (cfg.get("projects") or []) if str(p).strip()]

    thresholds = {**DEFAULT_THRESHOLDS, **(cfg.get("thresholds") or {})}
    vacuum_cfg = dict(cfg.get("vacuum") or {})
    vacuum = {
        "retention_hours": int(vacuum_cfg.get("retention_hours", 168)),
        "dry_run": bool(vacuum_cfg.get("dry_run", False)),
    }

    table_uris: list[str] = []
    seen: set[str] = set()
    for uri in extra_uris:
        if uri not in seen:
            seen.add(uri)
            table_uris.append(uri)

    resolver = table_resolver
    if resolver is None and projects:
        from dataops_core.watermark import list_bronze_tables as resolver  # type: ignore[assignment]

    if resolver is not None:
        for project in projects:
            for _name, path in resolver(project):
                uri = str(path).strip()
                if uri and uri not in seen:
                    seen.add(uri)
                    table_uris.append(uri)

    return {
        "projects": projects,
        "buckets": buckets,
        "table_uris": table_uris,
        "thresholds": thresholds,
        "vacuum": vacuum,
    }
