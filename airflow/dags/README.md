# DAGs do orquestrador da plataforma

Volume `/opt/airflow/dags` do Airflow da plataforma.

## DAGs de manutenção

DAGs de **manutenção de plataforma** em `ops/`:

| DAG | Função | Schedule |
|---|---|---|
| `ops_delta_health` | Inventário de buckets/tabelas, crescimento e fragmentação; alerta via e-mail | diário 06:00 |
| `ops_airflow_failures_report` | DagRuns failed classificadas em manual × agendado | diário 18:00 (fim do dia) |
| `ops_minio_growth_report` | Crescimento MinIO a partir do snapshot health | seg e sex 14:00 |
| `ops_delta_vacuum` | `vacuum` (delta-rs) nas URIs inventariadas | domingo 03:00 |
| `ops_delta_optimize` | Compacta small files (OPTIMIZE via delta-rs); vacuum posterior libera o storage | **manual** |
| `ops_delta_log_cleanup` | Checkpoint + limpeza de commits expirados no `_delta_log` | **manual** |
| `ops_minio_lifecycle` | Garante lifecycle MinIO (expira versões noncurrent + delete markers) | **manual** |

Configuração (Airflow Variable, sem secrets): `delta_ops`. Sem Variable / sem `buckets`, o padrão é o medalhão **`bronze`**, **`prata`**, **`ouro`**. Snapshot da health: `delta_health_snapshot`.

**Relatórios de ops**
- `ops_airflow_failures_report` (18:00): API REST v2 (`/api/v2/dags/~/dagRuns`), janela padrão 24h, gatilho **manual** / **agendado** / outro.
- `ops_minio_growth_report` (seg/sex 14:00): lê o snapshot gerado por `ops_delta_health` (06:00).

**Connection para falhas:** `airflow_api` (HTTP). A DAG obtém um JWT com o
login/senha da Connection e lista runs `state=failed`.

Na Variable (opcional):
```json
{
  "report": {
    "lookback_hours": 24,
    "exclude_dag_ids": ["ops_airflow_failures_report", "ops_minio_growth_report"],
    "max_failures": 40,
    "growth_warn_pct": 40,
    "api_conn_id": "airflow_api"
  }
}
```
No Trigger DAG, `conf` sobrescreve — ex.: `{"lookback_hours": 48}`.

**Vacuum:** retenção padrão **7 dias** (`retention_hours: 168`). Na Variable:
`{"vacuum": {"retention_hours": 168, "dry_run": false}}`.

**Lifecycle MinIO** (libera disco com versionamento ligado): default **10 dias** noncurrent. Variable:
`{"lifecycle": {"noncurrent_days": 10, "dry_run": false}}`.
No Trigger DAG, conf sobrescreve — ex.: `{"noncurrent_days": 14}` ou `{"noncurrent_days": 7, "buckets": ["bronze"]}`.

Object store por camada: uma Connection Airflow para bronze e outra para
prata/ouro. Credenciais e TLS vêm da Connection / env do host.

Tabelas Delta: descobertas ao percorrer objetos dos buckets (pasta `_delta_log`), somadas a `projects` / `extra_uris` quando houver.

**Qualidade de carga (volume):** a health alerta objetos fora de Delta (`loose_load_volume`), deltas efêmeros com `run_id=` / `_meta` aninhado (`ephemeral_delta_*`) e paths temporários (`temp_load_volume`).

## DAGs de domínio

DAGs de domínio (por fonte / pipeline) ficam no mesmo volume, em pastas próprias.
A imagem do worker instala as libs Python que esses jobs importam
(`requirements.txt` + `requirements-jobs.txt`).
