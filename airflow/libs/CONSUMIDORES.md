# Usando a dataops_core em um projeto

## Configuração da DAG

1. Declarar a versão mínima no topo do módulo de DAG:

   ```python
   import dataops_core
   dataops_core.require("1.0.0")
   ```

2. Alerta de falha, com o rótulo do projeto:

   ```python
   from dataops_core.notify import failure_callback

   default_args = {
       "email_on_failure": False,
       "on_failure_callback": failure_callback(label="Nome do projeto"),
   }
   ```

3. Limpeza e PII: `dataops_core.clean` e `dataops_core.security`. O que a lib
   cobre está no `README.md`.

## Consumidores

| Projeto | Versão mínima | Módulos |
|---|---|---|
| ops (plataforma) | 1.0.0 | delta.health, delta.vacuum, notify, watermark |

"Versão mínima" é a declarada em `dataops_core.require(...)`.

## Tempo

Datas de origem e watermark são aware `America/Sao_Paulo`; `etl_log` em UTC via
`sp_to_utc`; NTZ só na escrita Delta.
