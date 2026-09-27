# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 · Camada Silver — limpeza e padronização
# MAGIC
# MAGIC Na Bronze cada série chegou com um formato diferente:
# MAGIC - Banco Central: data em texto `dd/MM/yyyy` e valor em texto (`data`, `valor`)
# MAGIC - FRED: data em texto `yyyy-MM-dd` e uma coluna com o próprio código da série (`observation_date`, `VIXCLS`...)
# MAGIC - Históricos longos: a FRED manda VIX desde 1990, Brent desde 1987 e Fed Funds desde 1954
# MAGIC
# MAGIC **O que faço aqui, série a série:**
# MAGIC 1. Converto a data para `DATE` — linhas com data inválida são descartadas e contadas
# MAGIC 2. Mantenho só o período do estudo (a partir de 01/01/2017)
# MAGIC 3. Converto o valor para `DECIMAL(18,6)` — dias sem valor (feriados nos EUA, dias sem cotação) são descartados e contados
# MAGIC 4. Descarto valores ≤ 0 (nenhum desses indicadores pode ser zero ou negativo)
# MAGIC 5. Removo datas duplicadas
# MAGIC 6. Empilho tudo numa única tabela no formato longo: **uma linha por data × indicador**
# MAGIC
# MAGIC Cada descarte fica registrado na tabela `silver.controle_qualidade`, para que nada "desapareça em silêncio".

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F, DataFrame

DATA_CORTE = "2017-01-01"

# (tabela bronze, indicador, coluna de data, formato da data, coluna de valor, fonte, unidade)
SERIES = [
    ("bcb_ptax",         "PTAX_VENDA",  "data",             "dd/MM/yyyy", "valor",        "Banco Central - SGS 1",    "BRL por USD"),
    ("bcb_cdi",          "CDI",         "data",             "dd/MM/yyyy", "valor",        "Banco Central - SGS 4389", "% ao ano"),
    ("fred_fed_funds",   "FED_FUNDS",   "observation_date", "yyyy-MM-dd", "DFF",          "FRED - DFF",               "% ao ano"),
    ("fred_dolar_amplo", "DOLAR_AMPLO", "observation_date", "yyyy-MM-dd", "DTWEXBGS",     "FRED - DTWEXBGS",          "índice (jan/2006 = 100)"),
    ("fred_vix",         "VIX",         "observation_date", "yyyy-MM-dd", "VIXCLS",       "FRED - VIXCLS",            "pontos"),
    ("fred_brent",       "BRENT",       "observation_date", "yyyy-MM-dd", "DCOILBRENTEU", "FRED - DCOILBRENTEU",      "USD por barril"),
]

# COMMAND ----------

partes, controle = [], []

for tabela, indicador, col_data, fmt, col_valor, fonte, unidade in SERIES:
    bronze = spark.table(f"workspace.bronze.{tabela}")
    n_bronze = bronze.count()

    df = bronze.select(
        F.expr(f"try_to_date(`{col_data}`, '{fmt}')").alias("data"),
        F.expr(f"try_cast(trim(`{col_valor}`) AS DECIMAL(18,6))").alias("valor"),
        "_data_ingestao",
    )

    n_data_invalida = df.filter("data IS NULL").count()
    df = df.filter("data IS NOT NULL")

    n_fora_periodo = df.filter(F.col("data") < DATA_CORTE).count()
    df = df.filter(F.col("data") >= DATA_CORTE)

    n_sem_valor = df.filter("valor IS NULL").count()
    df = df.filter("valor IS NOT NULL")

    n_valor_invalido = df.filter("valor <= 0").count()
    df = df.filter("valor > 0")

    antes = df.count()
    df = df.dropDuplicates(["data"])
    n_duplicadas = antes - df.count()

    df = df.select(
        "data",
        F.lit(indicador).alias("indicador"),
        "valor",
        F.lit(fonte).alias("fonte"),
        F.lit(unidade).alias("unidade"),
        "_data_ingestao",
    )
    n_silver = df.count()
    partes.append(df)

    controle.append((indicador, n_bronze, n_data_invalida, n_fora_periodo,
                     n_sem_valor, n_valor_invalido, n_duplicadas, n_silver))

# COMMAND ----------

silver = reduce(DataFrame.unionByName, partes)

(silver.write.mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable("workspace.silver.serie_diaria"))

colunas_controle = ["indicador", "linhas_bronze", "data_invalida", "fora_periodo",
                    "sem_valor", "valor_invalido", "duplicadas", "linhas_silver"]
(spark.createDataFrame(controle, colunas_controle)
      .withColumn("data_execucao", F.current_timestamp())
      .write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable("workspace.silver.controle_qualidade"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Documentação no catálogo (Unity Catalog)

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE workspace.silver.serie_diaria IS
# MAGIC 'Séries diárias de câmbio, juros e indicadores globais a partir de 2017, tipadas e deduplicadas. Grão: uma linha por data x indicador. Origem: tabelas bronze.bcb_* e bronze.fred_*.';
# MAGIC
# MAGIC ALTER TABLE workspace.silver.serie_diaria ALTER COLUMN data COMMENT 'Data da observação (DATE). Convertida de dd/MM/yyyy (BCB) ou yyyy-MM-dd (FRED). Domínio: 2017-01-01 até a data da coleta.';
# MAGIC ALTER TABLE workspace.silver.serie_diaria ALTER COLUMN indicador COMMENT 'Código do indicador. Domínio: PTAX_VENDA, CDI, FED_FUNDS, DOLAR_AMPLO, VIX, BRENT.';
# MAGIC ALTER TABLE workspace.silver.serie_diaria ALTER COLUMN valor COMMENT 'Valor observado na unidade da coluna unidade. DECIMAL(18,6), sempre > 0.';
# MAGIC ALTER TABLE workspace.silver.serie_diaria ALTER COLUMN fonte COMMENT 'Fonte oficial e código da série (ex.: Banco Central - SGS 1).';
# MAGIC ALTER TABLE workspace.silver.serie_diaria ALTER COLUMN unidade COMMENT 'Unidade de medida do valor (BRL por USD, % ao ano, pontos, índice, USD por barril).';
# MAGIC ALTER TABLE workspace.silver.serie_diaria ALTER COLUMN _data_ingestao COMMENT 'Momento em que o dado entrou na Bronze (linhagem).';
# MAGIC
# MAGIC COMMENT ON TABLE workspace.silver.controle_qualidade IS
# MAGIC 'Registro de quantas linhas de cada série foram descartadas na passagem Bronze -> Silver, e por qual motivo.';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT indicador, linhas_bronze, data_invalida, fora_periodo, sem_valor,
# MAGIC        valor_invalido, duplicadas, linhas_silver
# MAGIC FROM workspace.silver.controle_qualidade
# MAGIC ORDER BY indicador;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT indicador, unidade, MIN(data) AS primeira_data, MAX(data) AS ultima_data,
# MAGIC        COUNT(*) AS dias, MIN(valor) AS minimo, MAX(valor) AS maximo
# MAGIC FROM workspace.silver.serie_diaria
# MAGIC GROUP BY indicador, unidade
# MAGIC ORDER BY indicador;