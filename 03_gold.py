# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 · Camada Gold — modelo dimensional
# MAGIC
# MAGIC Modelo em **constelação de fatos** (Aula 1 de DW): dois fatos com grãos diferentes que
# MAGIC compartilham a dimensão Tempo.
# MAGIC
# MAGIC | Tabela | Tipo | Grão (uma linha representa...) |
# MAGIC |---|---|---|
# MAGIC | `dim_tempo` | dimensão | um dia corrido do período |
# MAGIC | `dim_indicador` | dimensão | um indicador (PTAX, CDI, VIX...) |
# MAGIC | `fato_indicador_diario` | fato | um valor observado de um indicador em uma data |
# MAGIC | `fato_janela_cambial` | fato | um dia útil com PTAX publicada, com métricas de risco e o que aconteceu nos 30/60/90 dias seguintes |
# MAGIC
# MAGIC **Decisões importantes**
# MAGIC - O calendário de referência é o da **PTAX** (dias úteis no Brasil), porque é a taxa que liquida a exposição.
# MAGIC - Indicadores com calendário diferente (feriados nos EUA, Fed Funds publicado todo dia, índice do dólar
# MAGIC   com atraso) são alinhados pelo **último valor conhecido** até aquela data. Assim não uso informação do futuro.
# MAGIC - Horizontes de 30/60/90 dias corridos ≈ **21/42/63 dias úteis** de PTAX.
# MAGIC - O "forward teórico" é uma **aproximação** pela paridade de juros (CDI x Fed Funds). Não é a cotação real
# MAGIC   de uma NDF, que inclui cupom cambial, spread do banco e custos. Serve para comparar ordens de grandeza.

# COMMAND ----------

import math
from pyspark.sql import functions as F, Window

silver = spark.table("workspace.silver.serie_diaria")
lim = silver.agg(F.min("data").alias("ini"), F.max("data").alias("fim")).first()
datas = spark.sql(f"SELECT explode(sequence(DATE'{lim.ini}', DATE'{lim.fim}', INTERVAL 1 DAY)) AS data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_tempo

# COMMAND ----------

dias_ptax = (silver.filter("indicador = 'PTAX_VENDA'")
                   .select("data", F.lit(True).alias("tem_ptax")))
nomes_dia = F.array(*[F.lit(d) for d in ["domingo", "segunda", "terça", "quarta", "quinta", "sexta", "sábado"]])

dim_tempo = (datas.join(dias_ptax, "data", "left")
    .select(
        "data",
        F.year("data").alias("ano"),
        F.quarter("data").alias("trimestre"),
        F.month("data").alias("mes"),
        F.date_format("data", "yyyy-MM").alias("ano_mes"),
        F.weekofyear("data").alias("semana_ano"),
        F.element_at(nomes_dia, F.dayofweek("data")).alias("dia_semana"),
        F.coalesce("tem_ptax", F.lit(False)).alias("eh_dia_util_ptax"),
    ))

dim_tempo.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.dim_tempo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_indicador

# COMMAND ----------

indicadores = [
    ("PTAX_VENDA",  "PTAX venda USD/BRL",          "Câmbio",               "BRL por USD",             "Banco Central - SGS 1",    "diária (dias úteis BR)"),
    ("CDI",         "CDI anualizado base 252",     "Juros Brasil",         "% ao ano",                "Banco Central - SGS 4389", "diária (dias úteis BR)"),
    ("FED_FUNDS",   "Fed Funds efetivo",           "Juros EUA",            "% ao ano",                "FRED - DFF",               "diária (dias corridos)"),
    ("DOLAR_AMPLO", "Índice amplo do dólar (Fed)", "Força global do dólar", "índice (jan/2006 = 100)", "FRED - DTWEXBGS",          "diária, publicada semanalmente"),
    ("VIX",         "Índice de volatilidade VIX",  "Aversão a risco",      "pontos",                  "FRED - VIXCLS",            "diária (dias úteis EUA)"),
    ("BRENT",       "Petróleo Brent spot",         "Commodity",            "USD por barril",          "FRED - DCOILBRENTEU",      "diária (dias úteis)"),
]
(spark.createDataFrame(indicadores, ["indicador", "nome", "categoria", "unidade", "fonte", "periodicidade"])
      .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.dim_indicador"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## fato_indicador_diario

# COMMAND ----------

w_ind = Window.partitionBy("indicador").orderBy("data")

fato_ind = (silver
    .withColumn("valor", F.col("valor").cast("double"))
    .withColumn("valor_anterior", F.lag("valor").over(w_ind))
    .select(
        "data", "indicador", "valor",
        (F.col("valor") - F.col("valor_anterior")).alias("variacao_abs"),
        ((F.col("valor") / F.col("valor_anterior") - 1) * 100).alias("variacao_pct"),
    ))

fato_ind.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.fato_indicador_diario")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fato_janela_cambial

# COMMAND ----------

codigos = {"PTAX_VENDA": "ptax", "CDI": "cdi", "FED_FUNDS": "fed_funds",
           "DOLAR_AMPLO": "dolar_amplo", "VIX": "vix", "BRENT": "brent"}

pivot = (silver.groupBy("data").pivot("indicador", list(codigos)).agg(F.first("valor")))
for original, novo in codigos.items():
    pivot = pivot.withColumnRenamed(original, novo).withColumn(novo, F.col(novo).cast("double"))

# alinhamento: último valor conhecido até a data (nunca olha para frente)
base = datas.join(pivot, "data", "left")
w_ff = Window.orderBy("data").rowsBetween(Window.unboundedPreceding, 0)
for c in ["cdi", "fed_funds", "dolar_amplo", "vix", "brent"]:
    base = base.withColumn(c, F.last(c, ignorenulls=True).over(w_ff))
base = base.filter("ptax IS NOT NULL")   # só dias com PTAX publicada

w = Window.orderBy("data")
def janela(ini, fim):
    return Window.orderBy("data").rowsBetween(ini, fim)

base = (base
    .withColumn("ret_ptax", F.log(F.col("ptax") / F.lag("ptax").over(w)))
    .withColumn("ret_dolar_amplo", F.log(F.col("dolar_amplo") / F.lag("dolar_amplo").over(w)))
    .withColumn("ret_vix", F.log(F.col("vix") / F.lag("vix").over(w)))
    .withColumn("ret_brent", F.log(F.col("brent") / F.lag("brent").over(w)))
    .withColumn("var_ptax_5d_pct", (F.col("ptax") / F.lag("ptax", 5).over(w) - 1) * 100)
    .withColumn("vol_21d_anual_pct", F.stddev("ret_ptax").over(janela(-20, 0)) * math.sqrt(252) * 100)
    .withColumn("mm_63d", F.avg("ptax").over(janela(-62, 0)))
    .withColumn("desvio_mm63_pct", (F.col("ptax") / F.col("mm_63d") - 1) * 100)
    .withColumn("min_252d", F.min("ptax").over(janela(-251, 0)))
    .withColumn("max_252d", F.max("ptax").over(janela(-251, 0)))
    .withColumn("posicao_faixa_252d",
                (F.col("ptax") - F.col("min_252d")) / F.nullif(F.col("max_252d") - F.col("min_252d"), F.lit(0)))
    .withColumn("diferencial_juros_pp", F.col("cdi") - F.col("fed_funds"))
)

# regime de volatilidade: terços da distribuição histórica da vol de 21 dias
q1, q2 = base.approxQuantile("vol_21d_anual_pct", [1/3, 2/3], 0.001)
base = (base
    .withColumn("regime_vol", F.when(F.col("vol_21d_anual_pct").isNull(), None)
                              .when(F.col("vol_21d_anual_pct") <= q1, "1-Baixa")
                              .when(F.col("vol_21d_anual_pct") <= q2, "2-Média")
                              .otherwise("3-Alta"))
    .withColumn("faixa_252d", F.when(F.col("posicao_faixa_252d").isNull(), None)
                              .when(F.col("posicao_faixa_252d") <= 0.25, "1-Parte baixa (≤25%)")
                              .when(F.col("posicao_faixa_252d") >= 0.75, "3-Parte alta (≥75%)")
                              .otherwise("2-Intermediária"))
)
print(f"Limites de regime de vol: baixa ≤ {q1:.2f}% | média ≤ {q2:.2f}% | alta > {q2:.2f}%")

# o que aconteceu depois: 30/60/90 dias corridos ≈ 21/42/63 dias úteis
for du, dc in [(21, 30), (42, 60), (63, 90)]:
    base = (base
        .withColumn(f"ptax_em_{dc}d", F.lead("ptax", du).over(w))
        .withColumn(f"var_futura_{dc}d_pct", (F.col(f"ptax_em_{dc}d") / F.col("ptax") - 1) * 100)
        .withColumn(f"maior_alta_{dc}d_pct",
                    (F.max("ptax").over(janela(1, du)) / F.col("ptax") - 1) * 100)
        .withColumn(f"fwd_teorico_{dc}d",
                    F.col("ptax") * F.pow(1 + F.col("cdi") / 100, du / 252)
                    / (1 + F.col("fed_funds") / 100 * dc / 360))
        .withColumn(f"dif_fwd_vs_spot_{dc}d_pct",
                    (F.col(f"fwd_teorico_{dc}d") / F.col(f"ptax_em_{dc}d") - 1) * 100)
    )
    # maior_alta só é válida quando existe a janela futura completa
    base = base.withColumn(f"maior_alta_{dc}d_pct",
                           F.when(F.col(f"ptax_em_{dc}d").isNotNull(), F.col(f"maior_alta_{dc}d_pct")))

base.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.gold.fato_janela_cambial")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Documentação no catálogo

# COMMAND ----------

tabelas = {
    "dim_tempo": "Dimensão Tempo. Grão: um dia corrido entre a primeira e a última data da Silver. Origem: gerada a partir de sequence() + marcação dos dias com PTAX publicada.",
    "dim_indicador": "Dimensão dos indicadores do estudo, com fonte, unidade e periodicidade de publicação. Origem: cadastro manual baseado na documentação das fontes (BCB SGS e FRED).",
    "fato_indicador_diario": "Fato com o valor observado de cada indicador por data e sua variação em relação à observação anterior. Grão: data x indicador. Origem: silver.serie_diaria.",
    "fato_janela_cambial": "Fato analítico do câmbio. Grão: um dia útil com PTAX publicada. Contém indicadores alinhados pelo último valor conhecido, métricas de risco (vol, média móvel, faixa de 252 dias) e o que ocorreu com a PTAX nos 30/60/90 dias seguintes. Origem: silver.serie_diaria.",
}
for t, desc in tabelas.items():
    spark.sql(f"COMMENT ON TABLE workspace.gold.{t} IS '{desc}'")

colunas = {
    "dim_tempo": {
        "data": "Data (chave). Domínio: dias corridos do período do estudo.",
        "ano": "Ano da data.", "trimestre": "Trimestre (1 a 4).", "mes": "Mês (1 a 12).",
        "ano_mes": "Ano e mês no formato yyyy-MM.", "semana_ano": "Semana ISO do ano (1 a 53).",
        "dia_semana": "Nome do dia da semana em português.",
        "eh_dia_util_ptax": "TRUE quando o Banco Central publicou PTAX nesse dia (proxy de dia útil no Brasil).",
    },
    "fato_indicador_diario": {
        "data": "Data da observação. FK para dim_tempo.data.",
        "indicador": "FK para dim_indicador.indicador.",
        "valor": "Valor observado, na unidade descrita em dim_indicador.",
        "variacao_abs": "Diferença para a observação anterior do mesmo indicador (na unidade original).",
        "variacao_pct": "Variação percentual para a observação anterior do mesmo indicador.",
    },
    "fato_janela_cambial": {
        "data": "Dia útil com PTAX publicada. FK para dim_tempo.data.",
        "ptax": "PTAX venda do dia (BRL por USD). Domínio observado: ~3,05 a ~6,21.",
        "cdi": "CDI % a.a. (último valor conhecido até a data).",
        "fed_funds": "Fed Funds efetivo % a.a. (último valor conhecido até a data).",
        "dolar_amplo": "Índice amplo do dólar do Fed (último valor conhecido até a data).",
        "vix": "VIX em pontos (último valor conhecido até a data).",
        "brent": "Brent spot USD/barril (último valor conhecido até a data).",
        "ret_ptax": "Retorno logarítmico diário da PTAX.",
        "ret_dolar_amplo": "Retorno logarítmico do índice amplo do dólar entre dias de PTAX.",
        "ret_vix": "Retorno logarítmico do VIX entre dias de PTAX.",
        "ret_brent": "Retorno logarítmico do Brent entre dias de PTAX.",
        "var_ptax_5d_pct": "Variação % da PTAX em 5 dias úteis (aprox. uma semana).",
        "vol_21d_anual_pct": "Volatilidade realizada da PTAX em 21 dias úteis, anualizada (%).",
        "mm_63d": "Média móvel da PTAX em 63 dias úteis (aprox. 3 meses).",
        "desvio_mm63_pct": "Distância % da PTAX para a média móvel de 63 dias.",
        "min_252d": "Menor PTAX dos últimos 252 dias úteis.",
        "max_252d": "Maior PTAX dos últimos 252 dias úteis.",
        "posicao_faixa_252d": "Posição da PTAX na faixa do último ano: 0 = mínimo, 1 = máximo.",
        "diferencial_juros_pp": "CDI menos Fed Funds, em pontos percentuais (proxy do custo de carrego da proteção).",
        "regime_vol": "Regime de volatilidade pelos terços históricos da vol de 21d: 1-Baixa, 2-Média, 3-Alta.",
        "faixa_252d": "Classificação da posição na faixa de 252d: 1-Parte baixa (≤25%), 2-Intermediária, 3-Parte alta (≥75%).",
    },
}
for dc in (30, 60, 90):
    colunas["fato_janela_cambial"].update({
        f"ptax_em_{dc}d": f"PTAX observada {dc} dias corridos depois (aprox. em dias úteis). Nulo no fim da série.",
        f"var_futura_{dc}d_pct": f"Variação % da PTAX entre a data e {dc} dias depois.",
        f"maior_alta_{dc}d_pct": f"Maior alta % da PTAX em relação à data dentro dos {dc} dias seguintes (pior cenário para quem tem pagamento em USD sem proteção).",
        f"fwd_teorico_{dc}d": f"Forward teórico de {dc} dias pela paridade de juros CDI x Fed Funds (aproximação; não é cotação real de NDF).",
        f"dif_fwd_vs_spot_{dc}d_pct": f"Diferença % entre o forward teórico e a PTAX efetivamente observada em {dc} dias.",
    })

for t, cols in colunas.items():
    for c, desc in cols.items():
        spark.sql(f"ALTER TABLE workspace.gold.{t} ALTER COLUMN {c} COMMENT '{desc}'")
print("Comentários gravados no Unity Catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'dim_tempo' AS tabela, COUNT(*) AS linhas FROM workspace.gold.dim_tempo
# MAGIC UNION ALL SELECT 'dim_indicador', COUNT(*) FROM workspace.gold.dim_indicador
# MAGIC UNION ALL SELECT 'fato_indicador_diario', COUNT(*) FROM workspace.gold.fato_indicador_diario
# MAGIC UNION ALL SELECT 'fato_janela_cambial', COUNT(*) FROM workspace.gold.fato_janela_cambial;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT data, ptax, cdi, fed_funds, vix, vol_21d_anual_pct, regime_vol, faixa_252d,
# MAGIC        var_futura_90d_pct, maior_alta_90d_pct, fwd_teorico_90d
# MAGIC FROM workspace.gold.fato_janela_cambial
# MAGIC WHERE data >= '2024-12-01'
# MAGIC ORDER BY data
# MAGIC LIMIT 10;