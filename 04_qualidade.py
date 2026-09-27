# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04 · Qualidade de dados
# MAGIC
# MAGIC Verificações feitas sobre a Silver e a Gold, seguindo os critérios pedidos no enunciado:
# MAGIC **reconciliação, completude, unicidade, consistência (integridade referencial), acurácia (domínio) e outliers**.
# MAGIC Os descartes da Bronze → Silver já foram registrados em `silver.controle_qualidade` (notebook 02);
# MAGIC aqui eu confiro se tudo fecha e investigo o que sobrou.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Reconciliação Bronze → Silver → Gold
# MAGIC Toda linha da Bronze precisa estar na Silver **ou** ter um motivo de descarte. A diferença deve ser zero.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.indicador,
# MAGIC        c.linhas_bronze,
# MAGIC        c.data_invalida + c.fora_periodo + c.sem_valor + c.valor_invalido + c.duplicadas AS descartadas,
# MAGIC        c.linhas_silver,
# MAGIC        c.linhas_bronze - (c.data_invalida + c.fora_periodo + c.sem_valor + c.valor_invalido + c.duplicadas) - c.linhas_silver AS diferenca_bronze_silver,
# MAGIC        g.linhas_gold,
# MAGIC        c.linhas_silver - g.linhas_gold AS diferenca_silver_gold
# MAGIC FROM workspace.silver.controle_qualidade c
# MAGIC JOIN (SELECT indicador, COUNT(*) AS linhas_gold
# MAGIC       FROM workspace.gold.fato_indicador_diario GROUP BY indicador) g
# MAGIC   ON g.indicador = c.indicador
# MAGIC ORDER BY c.indicador;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- A fato_janela_cambial deve ter exatamente um registro por dia de PTAX da Silver
# MAGIC SELECT (SELECT COUNT(*) FROM workspace.silver.serie_diaria WHERE indicador = 'PTAX_VENDA') AS dias_ptax_silver,
# MAGIC        (SELECT COUNT(*) FROM workspace.gold.fato_janela_cambial) AS linhas_fato_janela;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Completude (nulos por coluna na fato principal)
# MAGIC Alguns nulos são **esperados por construção**: o primeiro dia não tem retorno, a volatilidade precisa de 21 dias
# MAGIC de histórico e as colunas "futuras" (30/60/90 dias) ficam nulas no fim da série, porque o futuro ainda não aconteceu.

# COMMAND ----------

fato = spark.table("workspace.gold.fato_janela_cambial")
total = fato.count()
nulos = fato.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in fato.columns]).first().asDict()
linhas = [(c, int(n), round(100 * n / total, 2)) for c, n in nulos.items()]
display(spark.createDataFrame(linhas, ["coluna", "nulos", "pct_nulos"]).orderBy(F.desc("nulos"), "coluna"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Unicidade (grão das tabelas)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'silver.serie_diaria (data x indicador)' AS tabela, COUNT(*) - COUNT(DISTINCT data, indicador) AS duplicadas FROM workspace.silver.serie_diaria
# MAGIC UNION ALL
# MAGIC SELECT 'gold.fato_indicador_diario (data x indicador)', COUNT(*) - COUNT(DISTINCT data, indicador) FROM workspace.gold.fato_indicador_diario
# MAGIC UNION ALL
# MAGIC SELECT 'gold.fato_janela_cambial (data)', COUNT(*) - COUNT(DISTINCT data) FROM workspace.gold.fato_janela_cambial
# MAGIC UNION ALL
# MAGIC SELECT 'gold.dim_tempo (data)', COUNT(*) - COUNT(DISTINCT data) FROM workspace.gold.dim_tempo
# MAGIC UNION ALL
# MAGIC SELECT 'gold.dim_indicador (indicador)', COUNT(*) - COUNT(DISTINCT indicador) FROM workspace.gold.dim_indicador;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Consistência — integridade referencial entre fatos e dimensões
# MAGIC Como tabelas Delta no Free Edition não impõem chave estrangeira, confiro com anti-join: o resultado esperado é zero órfãos.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'fato_janela_cambial -> dim_tempo' AS relacao, COUNT(*) AS orfaos
# MAGIC FROM workspace.gold.fato_janela_cambial f LEFT ANTI JOIN workspace.gold.dim_tempo d ON f.data = d.data
# MAGIC UNION ALL
# MAGIC SELECT 'fato_indicador_diario -> dim_tempo', COUNT(*)
# MAGIC FROM workspace.gold.fato_indicador_diario f LEFT ANTI JOIN workspace.gold.dim_tempo d ON f.data = d.data
# MAGIC UNION ALL
# MAGIC SELECT 'fato_indicador_diario -> dim_indicador', COUNT(*)
# MAGIC FROM workspace.gold.fato_indicador_diario f LEFT ANTI JOIN workspace.gold.dim_indicador d ON f.indicador = d.indicador;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Acurácia — valores dentro de faixas plausíveis
# MAGIC Faixas definidas por mim com base no conhecimento do mercado (bem largas, para pegar erro grosseiro e não evento real).

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH limites AS (
# MAGIC   SELECT * FROM VALUES
# MAGIC     ('PTAX_VENDA', 2.0, 8.0), ('CDI', 0.0, 20.0), ('FED_FUNDS', 0.0, 10.0),
# MAGIC     ('DOLAR_AMPLO', 80.0, 160.0), ('VIX', 5.0, 100.0), ('BRENT', 5.0, 200.0)
# MAGIC   AS t(indicador, minimo_esperado, maximo_esperado)
# MAGIC )
# MAGIC SELECT l.indicador, l.minimo_esperado, l.maximo_esperado,
# MAGIC        MIN(s.valor) AS minimo_obs, MAX(s.valor) AS maximo_obs,
# MAGIC        SUM(CASE WHEN s.valor NOT BETWEEN l.minimo_esperado AND l.maximo_esperado THEN 1 ELSE 0 END) AS fora_da_faixa
# MAGIC FROM workspace.silver.serie_diaria s JOIN limites l ON s.indicador = l.indicador
# MAGIC GROUP BY l.indicador, l.minimo_esperado, l.maximo_esperado
# MAGIC ORDER BY l.indicador;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Lacunas nas séries
# MAGIC Intervalo entre observações consecutivas. Fins de semana geram intervalos de 3 dias; feriados, 4.
# MAGIC Intervalos maiores merecem investigação.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH g AS (
# MAGIC   SELECT indicador, data,
# MAGIC          datediff(data, LAG(data) OVER (PARTITION BY indicador ORDER BY data)) AS intervalo
# MAGIC   FROM workspace.silver.serie_diaria
# MAGIC )
# MAGIC SELECT indicador, MAX(intervalo) AS maior_intervalo_dias,
# MAGIC        SUM(CASE WHEN intervalo > 4 THEN 1 ELSE 0 END) AS intervalos_acima_4_dias
# MAGIC FROM g GROUP BY indicador ORDER BY indicador;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Defasagem de publicação: quantos dias o último valor de cada indicador está atrás da última PTAX
# MAGIC SELECT indicador, MAX(data) AS ultima_data,
# MAGIC        datediff((SELECT MAX(data) FROM workspace.silver.serie_diaria WHERE indicador = 'PTAX_VENDA'), MAX(data)) AS dias_atras_da_ptax
# MAGIC FROM workspace.silver.serie_diaria GROUP BY indicador ORDER BY dias_atras_da_ptax DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Outliers — maiores variações diárias (z-score)
# MAGIC Uso o z-score da variação diária de cada indicador de mercado. Um |z| alto não significa erro:
# MAGIC preciso verificar se corresponde a um evento real antes de decidir tratar ou manter.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH estat AS (
# MAGIC   SELECT indicador, AVG(variacao_pct) AS media, STDDEV(variacao_pct) AS desvio
# MAGIC   FROM workspace.gold.fato_indicador_diario
# MAGIC   WHERE indicador IN ('PTAX_VENDA', 'DOLAR_AMPLO', 'VIX', 'BRENT')
# MAGIC   GROUP BY indicador
# MAGIC )
# MAGIC SELECT f.indicador, f.data, ROUND(f.valor, 4) AS valor, ROUND(f.variacao_pct, 2) AS variacao_pct,
# MAGIC        ROUND((f.variacao_pct - e.media) / e.desvio, 1) AS z_score
# MAGIC FROM workspace.gold.fato_indicador_diario f JOIN estat e ON f.indicador = e.indicador
# MAGIC WHERE ABS((f.variacao_pct - e.media) / e.desvio) > 5
# MAGIC ORDER BY ABS((f.variacao_pct - e.media) / e.desvio) DESC
# MAGIC LIMIT 20;