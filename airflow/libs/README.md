# dataops_core — biblioteca compartilhada do orquestrador

Biblioteca Python disponível para todas as DAGs da plataforma. Fica montada em
`/opt/airflow/libs` (read-only) e entra no `PYTHONPATH` do worker, junto com
`/opt/airflow/config`.

A lib cobre infraestrutura comum (e-mail, limpeza, PII, watermark, Delta). Regra
de negócio de cada pipeline fica no repositório do próprio projeto.

## Como a DAG usa

```python
import dataops_core
from dataops_core.notify import on_failure_callback
from dataops_core.clean import normalize_text, validate_cns
from dataops_core.security import hash_series

dataops_core.require("1.0.0")   # versão mínima exigida por esta DAG

default_args = {
    "email_on_failure": False,
    "on_failure_callback": on_failure_callback,
}
```

`require()` faz a DAG falhar no parse se o worker tiver uma versão da lib
anterior à declarada.

## Módulos (1.0.0)

| Módulo | Conteúdo |
|---|---|
| `dataops_core.notify` | E-mail operacional: template único, callbacks, entrega SMTP |
| `dataops_core.clean` | Texto, datas (`as_sp`/`sp_to_utc`/`sp_to_lake_ntz`), CNS/CPF/CNES/CID-10, faixa etária |
| `dataops_core.security` | SHA-256 com salt sobre identificador direto |
| `dataops_core.watermark` | Control plane: open/close run, max_ts, get_last_watermark |
| `dataops_core.delta` | URI s3a:// (qualquer camada): merge / overwrite / append / vacuum; health (`table_stats` / `bucket_usage` / `evaluate_health`); aliases `*_bronze` |

**Tempo:** datas de origem e watermark são aware `America/Sao_Paulo`; `etl_log`
grava em UTC via `sp_to_utc`; NTZ só na escrita Delta.

## E-mail

Template único em `dataops_core/notify/templates/shell.html`.

```python
from dataops_core.notify import (
    ExecucaoMetric, on_failure_callback, send_quality_email, send_execucao_email,
)

send_quality_email(context, rows=divergencias)          # [(tabela, descrição), ...]

send_execucao_email(
    context,
    intro="Carga concluída sem divergências.",
    metrics=[ExecucaoMetric("Linhas", "1.204"), ExecucaoMetric("Tabelas", "7")],
)
```

Quatro modos sobre o mesmo shell (barra de 4 cores, logo, cabeçalho e rodapé),
mudando cor de acento, título e rodapé:

| Modo | Uso | Acento |
|---|---|---|
| `error` | Falha de task, via `on_failure_callback` | vermelho |
| `quality` | Divergência warn-only | laranja |
| `vigilancia` | Indicadores em atenção, com destaque por tendência | verde |
| `execucao` | Resumo de execução bem-sucedida | azul |

`monitor` e `success` são aceitos como apelidos de `quality` e `execucao`.

A mensagem de erro vai resumida (primeiras linhas úteis, até 320 caracteres); o
stack completo fica no log do Airflow.

`send_quality_email` recebe `rows` como pares `(tabela, descrição)`.

Configuração por variável de ambiente:

| Variável | Default | Para que serve |
|---|---|---|
| `DATAOPS_PIPELINE_LABEL` | `dag_id` | Nome do pipeline no subtítulo |
| `DATAOPS_EMAIL_VAR` | `emails_alerta` | Variable/env com os destinatários |
| `DATAOPS_SMTP_CONN_ID` | `smtp_default` | Connection SMTP |
| `DATAOPS_EMAIL_LOGO_PATH` | asset da lib | Logo embutido em base64 |
| `DATAOPS_BRAND_*` | — | Nome, organização, unidade e rodapé |
| `DATAOPS_HASH_SALT` | — | Salt do hash de PII |

O rótulo do pipeline segue esta precedência: argumento `label=` da chamada,
depois `DATAOPS_PIPELINE_LABEL`, depois o `dag_id`. Para fixar o rótulo nas
DAGs:

```python
from dataops_core.notify import failure_callback

default_args = {"email_on_failure": False, "on_failure_callback": failure_callback(label="Vendas")}
```

`on_failure_callback` sem rótulo usa o `dag_id`.

O logo (`logo.svg`) é embutido em base64 no cabeçalho. Sem o arquivo, o
cabeçalho mostra `DATAOPS_BRAND_NAME` em texto.

Destinatários vêm de uma Airflow Variable (ou variável de ambiente de mesmo
nome) e aceitam JSON, lista ou string separada por vírgula. Falha de envio não
derruba a task: é logada. Erro de certificado dispara um diagnóstico de STARTTLS
que registra CN/SAN e validade do certificado do servidor.

## Versão

SemVer em `dataops_core.__version__`:

- **patch** — correção que não muda assinatura nem resultado esperado;
- **minor** — função ou parâmetro opcional novo;
- **major** — remoção ou mudança de comportamento.

## Testes

`tests/test_api_contract.py` verifica a superfície pública (nomes de função e de
parâmetro). Os demais testes cobrem limpeza, faixa etária, hash, Delta e
renderização do template, e rodam sem Airflow e sem SMTP.

```bash
pip install pytest unidecode
pytest airflow/libs/tests -q
```

## Deploy

A lib sobe junto com este repositório, no volume `./airflow/libs`. Alterações na
pasta do host valem para todos os workers sem rebuild de imagem.
