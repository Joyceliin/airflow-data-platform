"""TLS / CA para MinIO — storage_options e resolve_* (sem rede)."""

from __future__ import annotations

import dataops_core.delta.io as delta_io
from dataops_core.delta import (
    resolve_s3_ca_bundle,
    resolve_s3_ssl_verify,
    storage_options,
)


def _clear_ssl_env(monkeypatch) -> None:
    for key in (
        "DATAOPS_S3_CA_BUNDLE",
        "AWS_CA_BUNDLE",
        "REQUESTS_CA_BUNDLE",
        "DATAOPS_S3_ALLOW_INVALID_CERTS",
        "AWS_ALLOW_INVALID_CERTIFICATES",
    ):
        monkeypatch.delenv(key, raising=False)
    delta_io._ssl_diag_emitted = False


def test_resolve_s3_ca_bundle_priority(monkeypatch):
    _clear_ssl_env(monkeypatch)
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", "/requests/ca.pem")
    monkeypatch.setenv("AWS_CA_BUNDLE", "/aws/ca.pem")
    monkeypatch.setenv("DATAOPS_S3_CA_BUNDLE", "/opt/platform/minio-ca.pem")
    assert resolve_s3_ca_bundle() == "/opt/platform/minio-ca.pem"


def test_resolve_s3_ssl_verify_defaults_true(monkeypatch):
    _clear_ssl_env(monkeypatch)
    assert resolve_s3_ssl_verify() is True


def test_resolve_s3_ssl_verify_uses_ca_path(monkeypatch):
    _clear_ssl_env(monkeypatch)
    monkeypatch.setenv("AWS_CA_BUNDLE", "/certs/minio-ca.pem")
    assert resolve_s3_ssl_verify() == "/certs/minio-ca.pem"


def test_resolve_s3_ssl_verify_allow_invalid(monkeypatch):
    _clear_ssl_env(monkeypatch)
    monkeypatch.setenv("AWS_CA_BUNDLE", "/certs/minio-ca.pem")
    monkeypatch.setenv("DATAOPS_S3_ALLOW_INVALID_CERTS", "true")
    assert resolve_s3_ssl_verify() is False


def test_storage_options_certificate_path(monkeypatch):
    _clear_ssl_env(monkeypatch)
    monkeypatch.setenv("DATAOPS_S3_CA_BUNDLE", "/certs/minio-ca.pem")
    opts = storage_options()
    assert opts["certificate_path"] == "/certs/minio-ca.pem"
    assert "allow_invalid_certificates" not in opts


def test_storage_options_allow_invalid_certificates(monkeypatch):
    _clear_ssl_env(monkeypatch)
    monkeypatch.setenv("AWS_ALLOW_INVALID_CERTIFICATES", "1")
    opts = storage_options()
    assert opts["allow_invalid_certificates"] == "true"
