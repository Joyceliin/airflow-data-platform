"""Entrega SMTP dos alertas e diagnostico de certificado no STARTTLS."""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from dataops_core.notify.branding import SMTP_CONN_ID

logger = logging.getLogger(__name__)


def _get_base_hook():
    try:
        from airflow.sdk.bases.hook import BaseHook
    except ImportError:
        from airflow.hooks.base import BaseHook
    return BaseHook


def normalize_addresses(raw: Any) -> list[str]:
    """Aceita lista, JSON em string ou lista separada por virgula."""
    if raw is None:
        return []
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            return normalize_addresses(json.loads(text))
        except json.JSONDecodeError:
            return [part.strip() for part in text.split(",") if part.strip()]
    if isinstance(raw, (list, tuple, set)):
        out: list[str] = []
        for item in raw:
            out.extend(normalize_addresses(item))
        return out
    return [str(raw).strip()] if str(raw).strip() else []


def load_recipients(email_var: str) -> list[str]:
    """Le a Variable do Airflow e cai para variavel de ambiente de mesmo nome."""
    try:
        from airflow.models import Variable

        recipients = normalize_addresses(
            Variable.get(email_var, default_var="[]", deserialize_json=True)
        )
        if recipients:
            return recipients
    except Exception as exc:
        logger.warning(
            "Falha ao ler Variable '%s': %s. Tentando env.", email_var, type(exc).__name__
        )
    return normalize_addresses(os.getenv(email_var, "[]"))


def resolve_from_email(conn_id: str = SMTP_CONN_ID) -> str | None:
    for candidate in (
        os.getenv("AIRFLOW__SMTP__SMTP_MAIL_FROM"),
        os.getenv("SMTP_FROM_EMAIL"),
    ):
        if candidate:
            return candidate
    try:
        conn = _get_base_hook().get_connection(conn_id)
        extras = conn.extra_dejson or {}
        for key in ("from_email", "mail_from", "smtp_mail_from", "email_from"):
            if extras.get(key):
                return str(extras[key])
        if conn.login:
            return str(conn.login)
    except Exception as exc:
        logger.warning(
            "Nao foi possivel resolver from_email em '%s': %s", conn_id, type(exc).__name__
        )
    return None


def _extra_bool(extras: dict[str, Any], key: str, default: bool) -> bool:
    raw = extras.get(key)
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in {"1", "true", "yes", "y"}


def conn_settings(conn_id: str) -> dict[str, Any]:
    conn = _get_base_hook().get_connection(conn_id)
    extras = conn.extra_dejson or {}
    return {
        "host": conn.host,
        "port": int(conn.port or 587),
        "disable_ssl": _extra_bool(extras, "disable_ssl", False),
        "disable_tls": _extra_bool(extras, "disable_tls", False),
        "ssl_context": str(extras.get("ssl_context") or "default"),
        "timeout": float(extras.get("timeout") or 30),
    }


def _log_conn_settings(conn_id: str) -> dict[str, Any] | None:
    try:
        settings = conn_settings(conn_id)
    except Exception as exc:
        logger.warning("Extras SMTP de '%s' ilegiveis: %s", conn_id, type(exc).__name__)
        return None
    logger.info(
        "SMTP '%s' host=%s port=%s disable_ssl=%s disable_tls=%s ssl_context=%s",
        conn_id,
        settings["host"],
        settings["port"],
        settings["disable_ssl"],
        settings["disable_tls"],
        settings["ssl_context"],
    )
    if settings["disable_ssl"] and not settings["disable_tls"]:
        logger.warning(
            "SMTP '%s': disable_ssl=true so evita SSL implicito (porta 465). O STARTTLS "
            "continua ligado e o certificado e verificado. Para SMTP sem TLS o Extra "
            'precisa de "disable_ssl": true E "disable_tls": true.',
            conn_id,
        )
    return settings


def _peer_der(host: str, port: int, timeout: float, *, verify_chain: bool) -> bytes:
    import smtplib
    import ssl

    context = ssl.create_default_context()
    context.check_hostname = False
    if not verify_chain:
        context.verify_mode = ssl.CERT_NONE

    smtp = smtplib.SMTP(host=host, port=port, timeout=timeout)
    try:
        smtp.ehlo()
        smtp.starttls(context=context)
        sock = smtp.sock
        if sock is None:
            raise RuntimeError("socket SMTP ausente apos STARTTLS")
        der = sock.getpeercert(binary_form=True)
        if not der:
            raise RuntimeError("servidor SMTP nao apresentou certificado")
        return der
    finally:
        try:
            smtp.close()
        except Exception:
            pass


def _decode_certificate(der: bytes) -> tuple[list[str], str | None]:
    from cryptography import x509
    from cryptography.x509.oid import ExtensionOID, NameOID

    parsed = x509.load_der_x509_certificate(der)
    names = [
        str(attr.value)
        for attr in parsed.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
        if attr.value
    ]
    try:
        san = parsed.extensions.get_extension_for_oid(ExtensionOID.SUBJECT_ALTERNATIVE_NAME).value
        names.extend(str(dns) for dns in san.get_values_for_type(x509.DNSName))
        names.extend(str(ip) for ip in san.get_values_for_type(x509.IPAddress))
    except x509.ExtensionNotFound:
        pass

    not_after = getattr(parsed, "not_valid_after_utc", None) or parsed.not_valid_after
    seen: set[str] = set()
    unique = [n for n in names if n and not (n in seen or seen.add(n))]
    return unique, str(not_after)


def diagnose_tls(conn_id: str = SMTP_CONN_ID) -> None:
    """Le CN/SAN e validade do certificado sem autenticar nem enviar nada."""
    try:
        conn = _get_base_hook().get_connection(conn_id)
    except Exception as exc:
        logger.warning("Connection '%s' ilegivel: %s", conn_id, type(exc).__name__)
        return

    host = conn.host
    if not host:
        logger.warning("Connection '%s' sem Host; nao ha certificado para conferir.", conn_id)
        return

    settings = conn_settings(conn_id)
    verify_error: str | None = None
    try:
        der = _peer_der(str(host), settings["port"], settings["timeout"], verify_chain=True)
    except Exception as exc:
        verify_error = f"{type(exc).__name__}: {exc}"
        try:
            der = _peer_der(str(host), settings["port"], settings["timeout"], verify_chain=False)
        except Exception as peek_exc:
            logger.warning(
                "Certificado SMTP ilegivel (%s:%s): %s / %s",
                host,
                settings["port"],
                verify_error,
                peek_exc,
            )
            return

    try:
        names, not_after = _decode_certificate(der)
    except Exception as exc:
        logger.warning("Certificado obtido mas nao decodificado: %s", exc)
        names, not_after = [], None

    if verify_error and "expired" in verify_error.lower():
        logger.warning(
            "STARTTLS: certificado do servidor SMTP EXPIRADO (validade ate %s). Isso nao se "
            "corrige na connection — a infra precisa renovar. Nao use ssl_context=none em "
            "producao.",
            not_after or "data nao lida",
        )
    elif verify_error:
        logger.warning("STARTTLS: cadeia/validade do certificado falhou: %s", verify_error)

    if names:
        logger.warning(
            "STARTTLS: Host da connection '%s' e '%s'; o certificado cobre: %s.",
            conn_id,
            host,
            ", ".join(names),
        )


def send(
    *,
    recipients: list[str],
    subject: str,
    html_body: str,
    from_email: str | None = None,
    conn_id: str = SMTP_CONN_ID,
) -> None:
    """Envia pelo SmtpHook da connection configurada e fecha o cliente ao final."""
    try:
        from airflow.providers.smtp.hooks.smtp import SmtpHook
    except ImportError:
        from airflow.utils.email import send_email

        logger.warning("provider SMTP ausente; fallback airflow.utils.email via %s", conn_id)
        send_email(to=recipients, subject=subject, html_content=html_body, conn_id=conn_id)
        return

    _log_conn_settings(conn_id)
    hook = SmtpHook(smtp_conn_id=conn_id)
    try:
        hook.get_conn()
        hook.send_email_smtp(
            to=recipients,
            subject=subject,
            html_content=html_body,
            from_email=from_email,
        )
    finally:
        client = getattr(hook, "_smtp_client", None)
        if client is not None:
            try:
                client.close()
            except Exception:
                logger.debug("Encerramento SMTP ignorado apos o envio.")
