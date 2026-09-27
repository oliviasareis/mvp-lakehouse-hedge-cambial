-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 · Setup do ambiente
-- MAGIC Cria a estrutura do Lakehouse em camadas (medalhão) dentro do catálogo `workspace`.
-- MAGIC Optei por **um schema por camada** em vez de um catálogo por camada porque, no Free Edition,
-- MAGIC o catálogo `workspace` já vem pronto e isso simplifica permissões.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.bronze
COMMENT 'Camada Bronze: dados como vieram das fontes (BCB, FRED, Yahoo Finance), sem tratamento, com metadados de ingestão.';

CREATE SCHEMA IF NOT EXISTS workspace.silver
COMMENT 'Camada Silver: séries tipadas, deduplicadas e padronizadas (data, valor numérico).';

CREATE SCHEMA IF NOT EXISTS workspace.gold
COMMENT 'Camada Gold: modelo dimensional (constelação) para análise de câmbio e janelas de proteção.';

-- COMMAND ----------

-- Volume onde ficam os arquivos brutos (JSON/CSV) exatamente como baixados
CREATE VOLUME IF NOT EXISTS workspace.bronze.raw
COMMENT 'Arquivos brutos das coletas, organizados por fonte/data_ingestao.';

-- COMMAND ----------

SHOW SCHEMAS IN workspace;