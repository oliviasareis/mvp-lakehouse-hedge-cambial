# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 05 · Análise — respondendo às perguntas de negócio
# MAGIC
# MAGIC Todas as consultas usam a camada Gold (`workspace.gold.fato_janela_cambial`).
# MAGIC
# MAGIC **Cuidados de interpretação (valem para todo o notebook)**
# MAGIC - É uma análise **descritiva do histórico 2017–2026**, não uma previsão.
# MAGIC - As janelas de 30/60/90 dias se sobrepõem (cada dia útil abre uma janela), então os dias não são
# MAGIC   observações independentes. Os percentuais mostram frequências históricas, não probabilidades.
# MAGIC - Hedge aqui é tratado como **instrumento de previsibilidade**: a pergunta não é "quem acertou o dólar",
# MAGIC   e sim "quanta incerteza existia e quanto custava eliminá-la".

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 1 — Como o USD/BRL se comportou desde 2017?
# MAGIC Visão por ano: nível, variação dentro do ano, extremos, volatilidade e maior movimento diário.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year(data) AS ano,
# MAGIC        COUNT(*) AS dias_uteis,
# MAGIC        ROUND(min_by(ptax, data), 4) AS ptax_inicio,
# MAGIC        ROUND(max_by(ptax, data), 4) AS ptax_fim,
# MAGIC        ROUND((max_by(ptax, data) / min_by(ptax, data) - 1) * 100, 2) AS var_no_ano_pct,
# MAGIC        ROUND(MIN(ptax), 4) AS minima,
# MAGIC        ROUND(MAX(ptax), 4) AS maxima,
# MAGIC        ROUND(AVG(vol_21d_anual_pct), 2) AS vol_media_pct,
# MAGIC        ROUND(100 * AVG(CASE WHEN regime_vol = '3-Alta' THEN 1 ELSE 0 END), 1) AS pct_dias_vol_alta,
# MAGIC        ROUND(MAX(ABS(ret_ptax)) * 100, 2) AS maior_mov_diario_pct
# MAGIC FROM workspace.gold.fato_janela_cambial
# MAGIC GROUP BY year(data)
# MAGIC ORDER BY ano;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Série para gráfico: PTAX, média móvel de 63 dias e volatilidade
# MAGIC SELECT data, ptax, ROUND(mm_63d, 4) AS media_movel_63d, ROUND(vol_21d_anual_pct, 2) AS vol_21d_pct
# MAGIC FROM workspace.gold.fato_janela_cambial
# MAGIC ORDER BY data;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quanto o dólar costuma andar numa semana
# MAGIC SELECT ROUND(AVG(ABS(var_ptax_5d_pct)), 2) AS variacao_semanal_media_abs_pct,
# MAGIC        ROUND(percentile_approx(var_ptax_5d_pct, 0.05), 2) AS p5_semanal_pct,
# MAGIC        ROUND(percentile_approx(var_ptax_5d_pct, 0.95), 2) AS p95_semanal_pct,
# MAGIC        ROUND(MIN(var_ptax_5d_pct), 2) AS pior_queda_semanal_pct,
# MAGIC        ROUND(MAX(var_ptax_5d_pct), 2) AS maior_alta_semanal_pct
# MAGIC FROM workspace.gold.fato_janela_cambial;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 2 — Quais indicadores se movem junto com o dólar? A relação é estável?
# MAGIC Correlação entre a variação **semanal** (5 dias úteis) da PTAX e a dos demais indicadores.
# MAGIC Uso variação semanal, e não diária, para reduzir o ruído de calendários diferentes (feriados nos EUA,
# MAGIC publicação atrasada do índice do dólar).

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH r AS (
# MAGIC   SELECT data, year(data) AS ano,
# MAGIC          ln(ptax / LAG(ptax, 5) OVER w)               AS r_ptax,
# MAGIC          ln(dolar_amplo / LAG(dolar_amplo, 5) OVER w) AS r_dolar_amplo,
# MAGIC          ln(vix / LAG(vix, 5) OVER w)                 AS r_vix,
# MAGIC          ln(brent / LAG(brent, 5) OVER w)             AS r_brent,
# MAGIC          diferencial_juros_pp - LAG(diferencial_juros_pp, 5) OVER w AS d_diferencial
# MAGIC   FROM workspace.gold.fato_janela_cambial
# MAGIC   WINDOW w AS (ORDER BY data)
# MAGIC )
# MAGIC SELECT periodo, corr_dolar_global, corr_vix, corr_brent, corr_diferencial_juros FROM (
# MAGIC   SELECT 'Todo o período' AS periodo, 0 AS ordem,
# MAGIC          ROUND(corr(r_ptax, r_dolar_amplo), 2) AS corr_dolar_global,
# MAGIC          ROUND(corr(r_ptax, r_vix), 2) AS corr_vix,
# MAGIC          ROUND(corr(r_ptax, r_brent), 2) AS corr_brent,
# MAGIC          ROUND(corr(r_ptax, d_diferencial), 2) AS corr_diferencial_juros
# MAGIC   FROM r
# MAGIC   UNION ALL
# MAGIC   SELECT CAST(ano AS STRING), ano,
# MAGIC          ROUND(corr(r_ptax, r_dolar_amplo), 2), ROUND(corr(r_ptax, r_vix), 2),
# MAGIC          ROUND(corr(r_ptax, r_brent), 2), ROUND(corr(r_ptax, d_diferencial), 2)
# MAGIC   FROM r GROUP BY ano
# MAGIC )
# MAGIC ORDER BY ordem;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 3 — Quanta imprevisibilidade uma exposição sem proteção carregou em cada regime?
# MAGIC Para cada dia, a **maior alta** da PTAX nos 30/60/90 dias seguintes é o pior cenário para quem tinha um
# MAGIC pagamento em dólar sem proteção. Agrupo pelo regime de volatilidade daquele dia.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT regime_vol,
# MAGIC        COUNT(*) AS dias,
# MAGIC        ROUND(AVG(maior_alta_30d_pct), 2) AS media_maior_alta_30d,
# MAGIC        ROUND(AVG(maior_alta_60d_pct), 2) AS media_maior_alta_60d,
# MAGIC        ROUND(AVG(maior_alta_90d_pct), 2) AS media_maior_alta_90d,
# MAGIC        ROUND(percentile_approx(maior_alta_90d_pct, 0.95), 2) AS p95_maior_alta_90d,
# MAGIC        ROUND(MAX(maior_alta_90d_pct), 2) AS pior_caso_90d,
# MAGIC        ROUND(100 * AVG(CASE WHEN maior_alta_90d_pct > 5 THEN 1 ELSE 0 END), 1) AS pct_dias_alta_acima_5pct
# MAGIC FROM workspace.gold.fato_janela_cambial
# MAGIC WHERE regime_vol IS NOT NULL AND maior_alta_90d_pct IS NOT NULL
# MAGIC GROUP BY regime_vol
# MAGIC ORDER BY regime_vol;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 4 — Janela de proteção: a posição do dólar na faixa do último ano diz algo sobre os 90 dias seguintes?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT faixa_252d,
# MAGIC        COUNT(*) AS dias,
# MAGIC        ROUND(AVG(var_futura_90d_pct), 2) AS media_var_90d,
# MAGIC        ROUND(100 * AVG(CASE WHEN var_futura_90d_pct > 0 THEN 1 ELSE 0 END), 1) AS pct_dolar_mais_alto_em_90d,
# MAGIC        ROUND(AVG(maior_alta_90d_pct), 2) AS media_maior_alta_90d,
# MAGIC        ROUND(percentile_approx(maior_alta_90d_pct, 0.95), 2) AS p95_maior_alta_90d,
# MAGIC        ROUND(100 * AVG(CASE WHEN maior_alta_90d_pct > 5 THEN 1 ELSE 0 END), 1) AS pct_dias_alta_acima_5pct
# MAGIC FROM workspace.gold.fato_janela_cambial
# MAGIC WHERE faixa_252d IS NOT NULL AND var_futura_90d_pct IS NOT NULL
# MAGIC GROUP BY faixa_252d
# MAGIC ORDER BY faixa_252d;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cruzamento faixa x regime: média da maior alta em 90 dias (e quantidade de dias entre parênteses)
# MAGIC SELECT faixa_252d,
# MAGIC        CONCAT(ROUND(AVG(CASE WHEN regime_vol = '1-Baixa' THEN maior_alta_90d_pct END), 2), '%  (',
# MAGIC               SUM(CASE WHEN regime_vol = '1-Baixa' THEN 1 ELSE 0 END), ')') AS vol_baixa,
# MAGIC        CONCAT(ROUND(AVG(CASE WHEN regime_vol = '2-Média' THEN maior_alta_90d_pct END), 2), '%  (',
# MAGIC               SUM(CASE WHEN regime_vol = '2-Média' THEN 1 ELSE 0 END), ')') AS vol_media,
# MAGIC        CONCAT(ROUND(AVG(CASE WHEN regime_vol = '3-Alta' THEN maior_alta_90d_pct END), 2), '%  (',
# MAGIC               SUM(CASE WHEN regime_vol = '3-Alta' THEN 1 ELSE 0 END), ')') AS vol_alta
# MAGIC FROM workspace.gold.fato_janela_cambial
# MAGIC WHERE faixa_252d IS NOT NULL AND regime_vol IS NOT NULL AND maior_alta_90d_pct IS NOT NULL
# MAGIC GROUP BY faixa_252d
# MAGIC ORDER BY faixa_252d;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Onde estamos na última data disponível (retrato, não recomendação)
# MAGIC SELECT data, ptax, ROUND(posicao_faixa_252d, 2) AS posicao_faixa_252d, faixa_252d,
# MAGIC        ROUND(vol_21d_anual_pct, 2) AS vol_21d_pct, regime_vol,
# MAGIC        ROUND(desvio_mm63_pct, 2) AS desvio_mm63_pct, ROUND(diferencial_juros_pp, 2) AS diferencial_juros_pp
# MAGIC FROM workspace.gold.fato_janela_cambial
# MAGIC ORDER BY data DESC
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 5 — Quanto custava a previsibilidade (carrego) e como isso se compara ao que o dólar fez?
# MAGIC - **Custo do forward 90d**: quanto o forward teórico ficava acima da PTAX do dia (efeito do diferencial de juros).
# MAGIC - **Diferença forward x spot realizado**: positiva quando a taxa travada ficou acima da PTAX observada 90 dias depois
# MAGIC   (o custo pago pela previsibilidade); negativa quando o dólar subiu mais do que o forward embutia
# MAGIC   (a proteção absorveu essa alta).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year(data) AS ano,
# MAGIC        ROUND(AVG(diferencial_juros_pp), 2) AS diferencial_juros_medio_pp,
# MAGIC        ROUND(AVG((fwd_teorico_90d / ptax - 1) * 100), 2) AS custo_fwd_90d_pct,
# MAGIC        ROUND(AVG(var_futura_90d_pct), 2) AS var_media_ptax_90d_pct,
# MAGIC        ROUND(AVG(dif_fwd_vs_spot_90d_pct), 2) AS dif_media_fwd_vs_spot_pct,
# MAGIC        ROUND(100 * AVG(CASE WHEN dif_fwd_vs_spot_90d_pct < 0 THEN 1 ELSE 0 END), 1) AS pct_dias_dolar_acima_do_fwd
# MAGIC FROM workspace.gold.fato_janela_cambial
# MAGIC WHERE ptax_em_90d IS NOT NULL
# MAGIC GROUP BY year(data)
# MAGIC ORDER BY ano;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 6 — NDF ou call? (resposta parcial, com estimativa)
# MAGIC Não há dado público de volatilidade implícita nem de prêmio de opção de dólar. Por isso estimo o prêmio
# MAGIC teórico de uma **call de 90 dias com strike no forward** pela fórmula de **Garman-Kohlhagen**
# MAGIC (Black-Scholes para moedas), usando a **volatilidade realizada de 21 dias como proxy da implícita**.
# MAGIC
# MAGIC **Premissas e limitações**
# MAGIC - A vol realizada costuma ser menor que a implícita → o prêmio estimado tende a ficar **subestimado**.
# MAGIC - Sem smile de volatilidade, sem spread bancário, sem IOF/custos operacionais.
# MAGIC - NDF com cap depende de termos negociados com o banco e ficou fora do escopo.
# MAGIC
# MAGIC **O que comparo por regime de volatilidade:** o custo da NDF (forward acima do spot) contra o prêmio da call,
# MAGIC e com que frequência a call teria sido acionada (PTAX em 90 dias acima do strike).

# COMMAND ----------

import math
from pyspark.sql import functions as F

T = 63 / 252   # 90 dias corridos ≈ 63 dias úteis

def N(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))

pdf = (spark.table("workspace.gold.fato_janela_cambial")
       .select("data", "ptax", "cdi", "vol_21d_anual_pct", "regime_vol", "fwd_teorico_90d", "ptax_em_90d")
       .dropna()
       .toPandas())

def premio_call(linha):
    # Garman-Kohlhagen com strike = forward: C = e^(-rd.T) . F . [N(d1) - N(d2)], d1 = σ√T/2, d2 = -σ√T/2
    sigma_t = (linha.vol_21d_anual_pct / 100) * math.sqrt(T)
    rd = math.log(1 + linha.cdi / 100)
    return math.exp(-rd * T) * linha.fwd_teorico_90d * (N(sigma_t / 2) - N(-sigma_t / 2))

pdf["custo_ndf_pct"] = (pdf.fwd_teorico_90d / pdf.ptax - 1) * 100
pdf["premio_call_pct"] = pdf.apply(premio_call, axis=1) / pdf.ptax * 100
pdf["call_acionada"] = (pdf.ptax_em_90d > pdf.fwd_teorico_90d).astype(int)
pdf["cobertura_call_pct"] = ((pdf.ptax_em_90d - pdf.fwd_teorico_90d).clip(lower=0)) / pdf.ptax * 100

call = spark.createDataFrame(pdf[["data", "regime_vol", "custo_ndf_pct", "premio_call_pct",
                                  "call_acionada", "cobertura_call_pct"]])
display(call.groupBy("regime_vol").agg(
            F.count("*").alias("dias"),
            F.round(F.avg("custo_ndf_pct"), 2).alias("custo_medio_ndf_pct"),
            F.round(F.avg("premio_call_pct"), 2).alias("premio_medio_call_pct"),
            F.round(100 * F.avg("call_acionada"), 1).alias("pct_call_acionada"),
            F.round(F.avg(F.when(F.col("call_acionada") == 1, F.col("cobertura_call_pct"))), 2)
             .alias("cobertura_media_quando_acionada_pct"))
        .orderBy("regime_vol"))