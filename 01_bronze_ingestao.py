# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 · Camada Bronze
# MAGIC
# MAGIC **Fontes (todas oficiais e abertas)** — período de 01/01/2017 até a data da coleta
# MAGIC | Série | Fonte | Código | Arquivo bruto |
# MAGIC |---|---|---|---|
# MAGIC | PTAX venda USD/BRL | Banco Central (SGS) | 1 | `bcb_ptax_venda.json` |
# MAGIC | CDI anualizado (base 252) | Banco Central (SGS) | 4389 | `bcb_cdi_anual.json` |
# MAGIC | Fed Funds efetivo | FRED (St. Louis Fed) | DFF | `DFF.csv` |
# MAGIC | Índice amplo do dólar (Fed) | FRED | DTWEXBGS | `DTWEXBGS.csv` |
# MAGIC | VIX (volatilidade S&P 500) | FRED / Cboe | VIXCLS | `VIXCLS.csv` |
# MAGIC | Petróleo Brent spot | FRED / EIA | DCOILBRENTEU | `DCOILBRENTEU.csv` |
# MAGIC
# MAGIC **Como a coleta foi feita:** minha primeira versão buscava os dados direto daqui do notebook
# MAGIC via API, mas o Databricks Free Edition bloqueia acesso a sites externos
# MAGIC (`NameResolutionError` ao chamar `api.bcb.gov.br`). Como a alternativa seria instalar Python e
# MAGIC bibliotecas numa máquina corporativa, optei pelo caminho mais simples e rastreável: baixei cada
# MAGIC série diretamente pelas URLs públicas das fontes (listadas no README) e fiz upload dos arquivos,
# MAGIC sem nenhuma alteração, para o Volume `workspace.bronze.raw`.
# MAGIC
# MAGIC **Este notebook:** lê os arquivos do Volume **como vieram** (tudo como texto) e grava tabelas Delta
# MAGIC na Bronze, acrescentando apenas metadados de controle: arquivo de origem e momento da ingestão.
# MAGIC Nenhuma limpeza acontece aqui — isso fica para a Silver.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = "workspace"
VOLUME = "/Volumes/workspace/bronze/raw"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Conferência dos arquivos no Volume

# COMMAND ----------

esperados = ["bcb_ptax_venda.json", "bcb_cdi_anual.json",
             "DFF.csv", "DTWEXBGS.csv", "VIXCLS.csv", "DCOILBRENTEU.csv"]

no_volume = {f.name: f.size for f in dbutils.fs.ls(VOLUME)}
for arq in esperados:
    status = f"OK ({no_volume[arq]:,} bytes)" if arq in no_volume else "FALTANDO"
    print(f"{arq:<22} {status}")

assert all(a in no_volume for a in esperados), "Há arquivos faltando no Volume"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Carga das tabelas Bronze (Delta)
# MAGIC Uma tabela por série, porque cada arquivo tem um layout próprio
# MAGIC (o JSON do BCB usa `data`/`valor`; os CSVs da FRED usam a data + uma coluna com o código da série).

# COMMAND ----------

def carregar_bronze(arquivo, formato, tabela, **opcoes):
    df = (spark.read.format(formato).options(**opcoes).load(f"{VOLUME}/{arquivo}")
          .withColumn("_arquivo_origem", F.col("_metadata.file_path"))
          .withColumn("_data_ingestao", F.current_timestamp()))
    (df.write.mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOGO}.bronze.{tabela}"))
    print(f"bronze.{tabela}: {spark.table(f'{CATALOGO}.bronze.{tabela}').count()} linhas")

carregar_bronze("bcb_ptax_venda.json", "json", "bcb_ptax", multiLine="true")
carregar_bronze("bcb_cdi_anual.json", "json", "bcb_cdi", multiLine="true")
carregar_bronze("DFF.csv", "csv", "fred_fed_funds", header="true")
carregar_bronze("DTWEXBGS.csv", "csv", "fred_dolar_amplo", header="true")
carregar_bronze("VIXCLS.csv", "csv", "fred_vix", header="true")
carregar_bronze("DCOILBRENTEU.csv", "csv", "fred_brent", header="true")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Amostra de como o dado chegou (tudo texto, sem tratamento)
# MAGIC SELECT * FROM workspace.bronze.bcb_ptax LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.bronze.fred_vix LIMIT 5;