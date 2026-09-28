"""Helpers compartilhados das DAGs ops de manutenção Delta."""

from __future__ import annotations

import json
import os
from typing import Any

OPS_CONFIG_VAR = "delta_ops"
HEALTH_SNAPSHOT_VAR = "delta_health_snapshot"
OPS_LABEL = "DeltaOps"

# Bronze: ingestão. Prata/Ouro: processamento.
MINIO_CONN_BY_BUCKET = {
    "bronze": "minio_lakehouse",
    "prata": "minio_lakehouse_process",
    "ouro": "minio_lakehouse_process",
}
DEFAULT_MINIO_CONN = "minio_lakehouse_process"


def load_ops_config() -> dict[str, Any]:
    """Lê Airflow Variable `delta_ops` (JSON ou dict). Sem secrets."""
    try:
        from airflow.sdk import Variable as _Variable

        raw = _Variable.get(OPS_CONFIG_VAR, default={})
    except ImportError:
        from airflow.models import Variable as _Variable

        raw = _Variable.get(OPS_CONFIG_VAR, default_var={})
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return {}
        return json.loads(text)
    return {}


def resolve_ops_table_uris(
    config: dict[str, Any] | None = None,
    *,
    conf: dict[str, Any] | None = None,
    log_prefix: str = "[OPS]",
) -> list[str]:
    """Lista URIs Delta para DAGs ops.

    Prioridade do trigger (`conf`):
    - `table_uris` / `extra_uris` (+ `only_listed_uris`)
    - `uri_prefix` / `prefix` (filtra após descoberta)
    Sem `only_listed`, varre buckets da Variable / medalhão.
    """
    from dataops_core.delta import bucket_usage, resolve_targets

    cfg = config if config is not None else load_ops_config()
    targets = resolve_targets(cfg)
    conf = conf or {}

    uri_prefix = str(conf.get("uri_prefix") or conf.get("prefix") or "").strip()
    conf_uris = conf.get("table_uris") or conf.get("extra_uris")
    only_listed = bool(conf.get("only_listed_uris", False)) or bool(conf_uris)

    seen: set[str] = set()
    table_uris: list[str] = []

    if conf_uris:
        for uri in conf_uris:
            u = str(uri).strip()
            if u and u not in seen:
                seen.add(u)
                table_uris.append(u)
    else:
        for uri in targets["table_uris"]:
            if uri not in seen:
                seen.add(uri)
                table_uris.append(uri)

    bucket_errors: list[tuple[str, str]] = []
    if not only_listed:
        for bucket in targets["buckets"]:
            try:
                apply_minio_for_bucket(bucket)
                usage = bucket_usage(bucket)
            except Exception as exc:  # noqa: BLE001
                err = str(exc)
                bucket_errors.append((bucket, err))
                print(f"{log_prefix} bucket falhou s3a://{bucket}: {exc}", flush=True)
                continue
            for uri in usage.get("delta_uris") or []:
                if uri not in seen:
                    seen.add(uri)
                    table_uris.append(uri)

    if uri_prefix:
        table_uris = [u for u in table_uris if u.startswith(uri_prefix)]
        print(
            f"{log_prefix} filtro uri_prefix={uri_prefix} → {len(table_uris)} tabela(s)",
            flush=True,
        )

    # Falha se a descoberta falhou em todos os buckets.
    if (
        not only_listed
        and not table_uris
        and bucket_errors
        and len(bucket_errors) >= len(targets["buckets"])
    ):
        sample = "; ".join(f"{b}: {e[:160]}" for b, e in bucket_errors[:3])
        raise RuntimeError(
            f"{log_prefix} descoberta falhou em todos os buckets "
            f"({len(bucket_errors)}/{len(targets['buckets'])}). {sample}"
        )

    return table_uris


def load_health_snapshot() -> dict[str, Any]:
    try:
        from airflow.sdk import Variable as _Variable

        raw = _Variable.get(HEALTH_SNAPSHOT_VAR, default={})
    except ImportError:
        from airflow.models import Variable as _Variable

        raw = _Variable.get(HEALTH_SNAPSHOT_VAR, default_var={})
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return {}
        return json.loads(text)
    return {}


def save_health_snapshot(snapshot: dict[str, Any]) -> None:
    payload = json.dumps(snapshot, ensure_ascii=True, sort_keys=True)
    try:
        from airflow.sdk import Variable as _Variable
    except ImportError:
        from airflow.models import Variable as _Variable

    _Variable.set(HEALTH_SNAPSHOT_VAR, payload)


def bucket_key(bucket: str, prefix: str = "") -> str:
    b = bucket.strip().strip("/")
    p = prefix.strip().strip("/")
    return f"s3a://{b}/{p}" if p else f"s3a://{b}"


def bucket_from_uri(uri: str) -> str:
    """Extrai o nome do bucket de `s3a://bucket/...`."""
    clean = (uri or "").strip()
    if "://" in clean:
        clean = clean.split("://", 1)[1]
    return clean.split("/", 1)[0].strip()


def apply_minio_for_bucket(bucket: str) -> str:
    """Injeta no env a Connection correta da camada (DAG → env → dataops_core).

    Extra opcional da Connection (sem secrets no Extra além do path de CA):
    - ``ca_bundle`` / ``DATAOPS_S3_CA_BUNDLE``: path PEM no worker
    - ``allow_invalid_certificates``: true só em lab (propaga
      ``DATAOPS_S3_ALLOW_INVALID_CERTS``)
    """
    from airflow.hooks.base import BaseHook

    b = (bucket or "").strip().strip("/")
    conn_id = MINIO_CONN_BY_BUCKET.get(b, DEFAULT_MINIO_CONN)
    conn = BaseHook.get_connection(conn_id)
    if conn.login:
        os.environ["AWS_ACCESS_KEY_ID"] = conn.login
    if conn.password:
        os.environ["AWS_SECRET_ACCESS_KEY"] = conn.password
    if conn.host:
        os.environ["AWS_ENDPOINT_URL"] = conn.host
        os.environ["MINIO_ENDPOINT"] = conn.host

    extra = {}
    try:
        extra = conn.extra_dejson or {}
    except Exception:  # noqa: BLE001
        extra = {}
    ca = (
        str(extra.get("ca_bundle") or extra.get("DATAOPS_S3_CA_BUNDLE") or "").strip()
    )
    if ca:
        os.environ["DATAOPS_S3_CA_BUNDLE"] = ca
    allow_invalid = extra.get("allow_invalid_certificates")
    if allow_invalid is True or str(allow_invalid).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        os.environ["DATAOPS_S3_ALLOW_INVALID_CERTS"] = "true"

    print(f"[DeltaOps] MinIO bucket={b or '?'} via Connection '{conn_id}'", flush=True)
    return conn_id


DEFAULT_LIFECYCLE_NONCURRENT_DAYS = 10
LIFECYCLE_RULE_ID = "platform-noncurrent-purge"
LIFECYCLE_MARKER_RULE_ID = "platform-expired-delete-markers"


def resolve_lifecycle_params(
    config: dict[str, Any] | None = None,
    *,
    conf: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Prioridade: conf do trigger → params da DAG → Variable → default 10 dias."""
    from dataops_core.delta import DEFAULT_MEDALLION_BUCKETS, resolve_targets

    cfg = config if config is not None else load_ops_config()
    life = dict(cfg.get("lifecycle") or {})
    conf = conf or {}
    params = params or {}

    raw_days = conf.get("noncurrent_days")
    if raw_days is None:
        raw_days = params.get("noncurrent_days")
    if raw_days is None:
        raw_days = life.get("noncurrent_days", DEFAULT_LIFECYCLE_NONCURRENT_DAYS)
    days = max(1, int(raw_days))

    raw_buckets = conf.get("buckets")
    if raw_buckets is None:
        raw_buckets = params.get("buckets")
    if raw_buckets is None:
        raw_buckets = life.get("buckets")
    if raw_buckets is None:
        raw_buckets = resolve_targets(cfg)["buckets"]
    if isinstance(raw_buckets, str):
        buckets = [b.strip() for b in raw_buckets.split(",") if b.strip()]
    else:
        buckets = [str(b).strip().strip("/") for b in (raw_buckets or []) if str(b).strip()]
    if not buckets:
        buckets = list(DEFAULT_MEDALLION_BUCKETS)

    dry_run = conf.get("dry_run")
    if dry_run is None:
        dry_run = params.get("dry_run")
    if dry_run is None:
        dry_run = life.get("dry_run", False)

    return {
        "noncurrent_days": days,
        "buckets": buckets,
        "dry_run": bool(dry_run),
    }


def _s3_client_from_env() -> Any:
    from dataops_core.delta import s3_client

    return s3_client()


def ensure_noncurrent_lifecycle(
    bucket: str,
    *,
    noncurrent_days: int = DEFAULT_LIFECYCLE_NONCURRENT_DAYS,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Garante regra de expiração de versões não atuais (idempotente)."""
    days = max(1, int(noncurrent_days))
    b = bucket.strip().strip("/")
    apply_minio_for_bucket(b)
    client = _s3_client_from_env()

    rules = [
        {
            "ID": LIFECYCLE_RULE_ID,
            "Status": "Enabled",
            "Filter": {"Prefix": ""},
            "NoncurrentVersionExpiration": {"NoncurrentDays": days},
        },
        {
            "ID": LIFECYCLE_MARKER_RULE_ID,
            "Status": "Enabled",
            "Filter": {"Prefix": ""},
            "Expiration": {"ExpiredObjectDeleteMarker": True},
        },
    ]
    desired = {"Rules": rules}

    current: dict[str, Any] | None = None
    try:
        resp = client.get_bucket_lifecycle_configuration(Bucket=b)
        current = {"Rules": resp.get("Rules") or []}
    except Exception as exc:  # noqa: BLE001 — boto/MinIO: NoSuchLifecycleConfiguration
        from botocore.exceptions import ClientError

        if isinstance(exc, ClientError):
            code = (exc.response.get("Error") or {}).get("Code", "")
            if code not in {"NoSuchLifecycleConfiguration", "404", "NoSuchBucket"}:
                raise
        elif "NoSuchLifecycleConfiguration" not in str(exc):
            raise

    if dry_run:
        print(
            f"[LIFECYCLE][dry-run] s3://{b}: aplicaria NoncurrentDays={days}; "
            f"atual={current}",
            flush=True,
        )
        return {"bucket": b, "noncurrent_days": days, "dry_run": True, "applied": False}

    client.put_bucket_lifecycle_configuration(
        Bucket=b,
        LifecycleConfiguration=desired,
    )
    print(
        f"[LIFECYCLE] s3://{b}: NoncurrentVersionExpiration={days}d + "
        f"ExpiredObjectDeleteMarker (rules={LIFECYCLE_RULE_ID}, {LIFECYCLE_MARKER_RULE_ID})",
        flush=True,
    )
    return {"bucket": b, "noncurrent_days": days, "dry_run": False, "applied": True}
