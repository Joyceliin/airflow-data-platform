"""Testes unitários de dataops_core.delta.health (sem MinIO/Delta reais)."""

from __future__ import annotations

from dataops_core.delta.health import (
    DEFAULT_MEDALLION_BUCKETS,
    DEFAULT_THRESHOLDS,
    delta_uri_from_object_key,
    evaluate_health,
    growth_pct,
    is_ephemeral_delta_uri,
    is_temp_object_key,
    resolve_targets,
)


def test_growth_pct_basic():
    assert growth_pct(150, 100) == 50.0
    assert growth_pct(100, 100) == 0.0
    assert growth_pct(None, 100) is None
    assert growth_pct(100, None) is None
    assert growth_pct(50, 0) == 100.0
    assert growth_pct(0, 0) is None


def test_delta_uri_from_object_key():
    assert (
        delta_uri_from_object_key("bronze", "projeto_a/tb_a/_delta_log/000.json")
        == "s3a://bronze/projeto_a/tb_a"
    )
    assert (
        delta_uri_from_object_key("prata", "proj/int_x/_delta_log/")
        == "s3a://prata/proj/int_x"
    )
    assert delta_uri_from_object_key("bronze", "projeto_a/tb_a/part-000.parquet") is None
    assert delta_uri_from_object_key("ouro", "_delta_log/000.json") == "s3a://ouro"


def test_resolve_targets_defaults_and_extra_uris():
    def fake_resolver(project: str):
        if project == "projeto_a":
            return [("tb_a", "s3a://bronze/projeto_a/tb_a")]
        return []

    out = resolve_targets(
        {
            "projects": ["projeto_a"],
            "buckets": ["bronze", "prata"],
            "extra_uris": ["s3a://prata/x", "s3a://bronze/projeto_a/tb_a"],
            "thresholds": {"max_num_files": 100},
            "vacuum": {"retention_hours": 168, "dry_run": True},
        },
        table_resolver=fake_resolver,
    )
    assert out["buckets"] == ["bronze", "prata"]
    assert out["table_uris"] == ["s3a://prata/x", "s3a://bronze/projeto_a/tb_a"]
    assert out["thresholds"]["max_num_files"] == 100
    assert out["thresholds"]["bucket_growth_pct"] == DEFAULT_THRESHOLDS["bucket_growth_pct"]
    assert out["vacuum"] == {"retention_hours": 168, "dry_run": True}


def test_resolve_targets_empty_config_uses_medallion_buckets():
    out = resolve_targets(None, table_resolver=lambda _p: [])
    assert out["buckets"] == list(DEFAULT_MEDALLION_BUCKETS)
    assert out["buckets"] == ["bronze", "prata", "ouro"]
    assert out["table_uris"] == []
    assert out["vacuum"]["retention_hours"] == 168
    assert out["vacuum"]["dry_run"] is False


def test_resolve_targets_empty_buckets_list_uses_medallion():
    out = resolve_targets({"buckets": []}, table_resolver=lambda _p: [])
    assert out["buckets"] == ["bronze", "prata", "ouro"]


def test_evaluate_health_alerts():
    metrics = {
        "buckets": [
            {
                "bucket": "bronze",
                "prefix": "",
                "total_bytes": 200,
                "object_count": 10,
                "growth_pct": 55.0,
            }
        ],
        "tables": [
            {
                "path": "s3a://bronze/projeto_a/tb",
                "is_delta": True,
                "num_files": 6000,
                "history_len": 500,
                "size_bytes": 120_000_000,
                "avg_file_bytes": 20_000,
                "growth_pct": 80.0,
            },
            {
                "path": "s3a://missing",
                "is_delta": False,
            },
        ],
    }
    alerts = evaluate_health(metrics)
    codes = {a["code"] for a in alerts}
    assert "bucket_growth" in codes
    assert "table_growth" in codes
    assert "too_many_files" in codes
    assert "stale_log" in codes
    assert "small_files" in codes
    assert "not_delta" in codes
    assert "long_history" not in codes


def test_evaluate_health_no_alerts_under_threshold():
    metrics = {
        "buckets": [
            {
                "bucket": "bronze",
                "prefix": "",
                "total_bytes": 100,
                "object_count": 1,
                "growth_pct": 5.0,
                "loose_bytes": 0,
                "loose_pct": 0.0,
                "loose_objects": 0,
                "ephemeral_delta_uris": [],
                "ephemeral_bytes": 0,
                "temp_objects": 0,
                "temp_bytes": 0,
            }
        ],
        "tables": [
            {
                "path": "s3a://bronze/projeto_a/tb",
                "is_delta": True,
                "num_files": 10,
                "history_len": 5,
                "size_bytes": 20_000_000,
                "avg_file_bytes": 2_000_000,
                "growth_pct": 5.0,
            }
        ],
    }
    assert evaluate_health(metrics) == []


def test_small_files_skips_tiny_partitioned_table():
    """2 arquivos pequenos em tabela pequena — não alerta."""
    metrics = {
        "buckets": [],
        "tables": [
            {
                "path": "s3a://bronze/projeto_b/tb_pequena",
                "is_delta": True,
                "num_files": 2,
                "history_len": 10,
                "size_bytes": 500_000,
                "avg_file_bytes": 250_000,
                "growth_pct": 0.0,
            }
        ],
    }
    codes = {a["code"] for a in evaluate_health(metrics)}
    assert "small_files" not in codes


def test_small_files_alerts_real_fragmentation():
    metrics = {
        "buckets": [],
        "tables": [
            {
                "path": "s3a://bronze/projeto_a/tb_amostra",
                "is_delta": True,
                "num_files": 100,
                "history_len": 50,
                "size_bytes": 20_000_000,  # abaixo de 80 MiB → sem small_files
                "avg_file_bytes": 200_000,
                "growth_pct": 0.0,
            },
            {
                "path": "s3a://bronze/projeto_a/tb_big",
                "is_delta": True,
                "num_files": 100,
                "history_len": 50,
                "size_bytes": 100_000_000,  # >= 80 MiB
                "avg_file_bytes": 200_000,
                "growth_pct": 0.0,
            },
        ],
    }
    alerts = evaluate_health(metrics)
    small = [a for a in alerts if a["code"] == "small_files"]
    assert len(small) == 1
    assert small[0]["target"] == "s3a://bronze/projeto_a/tb_big"


def test_stale_log_at_history_cap():
    metrics = {
        "buckets": [],
        "tables": [
            {
                "path": "s3a://ouro/projeto_c/origem_x/tb_x",
                "is_delta": True,
                "num_files": 10,
                "history_len": 500,
                "size_bytes": 10_000_000,
                "avg_file_bytes": 1_000_000,
                "growth_pct": 0.0,
            }
        ],
    }
    alerts = evaluate_health(metrics)
    by_code = {a["code"]: a for a in alerts}
    assert "stale_log" in by_code
    assert by_code["stale_log"]["severity"] == "warn"
    assert "ops_delta_log_cleanup" in by_code["stale_log"]["message"]
    assert "long_history" not in by_code
    assert "high_commit_rate" not in by_code


def test_high_commit_rate_below_cap():
    # 425 versões / 7d ≈ 61/dia (>= 60): dispara high_commit_rate pela taxa diária.
    metrics = {
        "buckets": [],
        "tables": [
            {
                "path": "s3a://ouro/projeto_c/origem_x/tb_y",
                "is_delta": True,
                "num_files": 10,
                "history_len": 425,
                "size_bytes": 10_000_000,
                "avg_file_bytes": 1_000_000,
                "growth_pct": 0.0,
            }
        ],
    }
    alerts = evaluate_health(metrics)
    by_code = {a["code"]: a for a in alerts}
    assert "high_commit_rate" in by_code
    assert by_code["high_commit_rate"]["severity"] == "info"
    assert "stale_log" not in by_code
    assert "long_history" not in by_code


def test_is_ephemeral_delta_uri():
    assert is_ephemeral_delta_uri(
        "s3a://bronze/datasus/_meta/column_inventory/system=SIA/run_id=manual__1"
    )
    assert not is_ephemeral_delta_uri("s3a://bronze/projeto_a/tb_paciente")


def test_is_temp_object_key():
    assert is_temp_object_key("proj/_tmp/out.parquet")
    assert not is_temp_object_key("proj/tb/part-000.parquet")


def test_evaluate_health_flags_ephemeral_and_loose_load():
    metrics = {
        "buckets": [
            {
                "bucket": "bronze",
                "prefix": "",
                "total_bytes": 10_000,
                "object_count": 100,
                "growth_pct": 0.0,
                "loose_bytes": 3_000,
                "loose_pct": 30.0,
                "loose_objects": 40,
                "loose_samples": ["dump/a.csv", "dump/b.csv"],
                "ephemeral_delta_uris": [
                    "s3a://bronze/datasus/_meta/x/system=SIA/run_id=r1",
                    "s3a://bronze/datasus/_meta/x/system=SIH/run_id=r2",
                ],
                "ephemeral_bytes": 80_000,
                "temp_objects": 2,
                "temp_bytes": 500,
            }
        ],
        "tables": [],
    }
    alerts = evaluate_health(metrics)
    codes = {a["code"] for a in alerts}
    assert "loose_load_volume" in codes
    assert "ephemeral_delta_load" in codes
    assert "ephemeral_delta_table" in codes
    assert "temp_load_volume" in codes
    assert sum(1 for a in alerts if a["code"] == "ephemeral_delta_table") == 2
