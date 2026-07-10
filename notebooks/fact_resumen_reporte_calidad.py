# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # FACT_RESUMEN_REPORTE_CALIDAD — Métricas de calidad por ámbito
# MAGIC **Tabla fuente:** `uc_axa_cli.gold.fact_resumen_reporte_calidad`
# MAGIC
# MAGIC **Ámbitos disponibles:** Fuente · Cliente · Grupo Regla · Atributo · Regla
# MAGIC
# MAGIC **% calidad** = `cant_validos / cant_evaluados * 100`
# MAGIC
# MAGIC **Semáforo:** 🟢 VERDE ≥ 95% · 🟡 AMARILLO ≥ 80% · 🔴 ROJO < 80%

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Resumen ejecutivo — una fila por ámbito

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC     ambito,
# MAGIC     COUNT(DISTINCT descripcion_ambito)         AS total_elementos,
# MAGIC     SUM(cant_evaluados)                        AS total_evaluados,
# MAGIC     SUM(cant_validos)                          AS total_validos,
# MAGIC     SUM(cant_invalidos)                        AS total_invalidos,
# MAGIC     SUM(cant_remediados)                       AS total_remediados,
# MAGIC     ROUND(
# MAGIC         SUM(cant_validos) * 100.0
# MAGIC         / NULLIF(SUM(cant_evaluados), 0)
# MAGIC     , 2)                                       AS porcentaje_calidad
# MAGIC FROM  uc_axa_cli.gold.fact_resumen_reporte_calidad
# MAGIC GROUP BY ambito
# MAGIC ORDER BY
# MAGIC     CASE ambito
# MAGIC         WHEN 'Fuente'      THEN 1
# MAGIC         WHEN 'Cliente'     THEN 2
# MAGIC         WHEN 'Grupo Regla' THEN 3
# MAGIC         WHEN 'Atributo'    THEN 4
# MAGIC         WHEN 'Regla'       THEN 5
# MAGIC         ELSE                    6
# MAGIC     END

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Detalle por ámbito — FUENTE (satélite)

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC     'Fuente'            AS ambito,
# MAGIC     descripcion_ambito  AS fuente,
# MAGIC     SUM(cant_evaluados) AS evaluados,
# MAGIC     SUM(cant_validos)   AS validos,
# MAGIC     SUM(cant_invalidos) AS invalidos,
# MAGIC     SUM(cant_remediados)AS remediados,
# MAGIC     ROUND(
# MAGIC         SUM(cant_validos) * 100.0
# MAGIC         / NULLIF(SUM(cant_evaluados), 0)
# MAGIC     , 2)                AS porcentaje_calidad
# MAGIC FROM  uc_axa_cli.gold.fact_resumen_reporte_calidad
# MAGIC WHERE ambito = 'Fuente'
# MAGIC GROUP BY descripcion_ambito
# MAGIC ORDER BY porcentaje_calidad ASC

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Detalle por ámbito — CLIENTE

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC     'Cliente'           AS ambito,
# MAGIC     descripcion_ambito  AS cliente,
# MAGIC     SUM(cant_evaluados) AS evaluados,
# MAGIC     SUM(cant_validos)   AS validos,
# MAGIC     SUM(cant_invalidos) AS invalidos,
# MAGIC     SUM(cant_remediados)AS remediados,
# MAGIC     ROUND(
# MAGIC         SUM(cant_validos) * 100.0
# MAGIC         / NULLIF(SUM(cant_evaluados), 0)
# MAGIC     , 2)                AS porcentaje_calidad
# MAGIC FROM  uc_axa_cli.gold.fact_resumen_reporte_calidad
# MAGIC WHERE ambito = 'Cliente'
# MAGIC GROUP BY descripcion_ambito
# MAGIC ORDER BY porcentaje_calidad ASC

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Detalle por ámbito — GRUPO REGLA

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC     'Grupo Regla'       AS ambito,
# MAGIC     descripcion_ambito  AS grupo_regla,
# MAGIC     SUM(cant_evaluados) AS evaluados,
# MAGIC     SUM(cant_validos)   AS validos,
# MAGIC     SUM(cant_invalidos) AS invalidos,
# MAGIC     SUM(cant_remediados)AS remediados,
# MAGIC     ROUND(
# MAGIC         SUM(cant_validos) * 100.0
# MAGIC         / NULLIF(SUM(cant_evaluados), 0)
# MAGIC     , 2)                AS porcentaje_calidad
# MAGIC FROM  uc_axa_cli.gold.fact_resumen_reporte_calidad
# MAGIC WHERE ambito = 'Grupo Regla'
# MAGIC GROUP BY descripcion_ambito
# MAGIC ORDER BY porcentaje_calidad ASC

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Detalle por ámbito — ATRIBUTO

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC     'Atributo'          AS ambito,
# MAGIC     descripcion_ambito  AS atributo,
# MAGIC     SUM(cant_evaluados) AS evaluados,
# MAGIC     SUM(cant_validos)   AS validos,
# MAGIC     SUM(cant_invalidos) AS invalidos,
# MAGIC     SUM(cant_remediados)AS remediados,
# MAGIC     ROUND(
# MAGIC         SUM(cant_validos) * 100.0
# MAGIC         / NULLIF(SUM(cant_evaluados), 0)
# MAGIC     , 2)                AS porcentaje_calidad
# MAGIC FROM  uc_axa_cli.gold.fact_resumen_reporte_calidad
# MAGIC WHERE ambito = 'Atributo'
# MAGIC GROUP BY descripcion_ambito
# MAGIC ORDER BY porcentaje_calidad ASC

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Detalle por ámbito — REGLA

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC     'Regla'             AS ambito,
# MAGIC     descripcion_ambito  AS regla,
# MAGIC     SUM(cant_evaluados) AS evaluados,
# MAGIC     SUM(cant_validos)   AS validos,
# MAGIC     SUM(cant_invalidos) AS invalidos,
# MAGIC     SUM(cant_remediados)AS remediados,
# MAGIC     ROUND(
# MAGIC         SUM(cant_validos) * 100.0
# MAGIC         / NULLIF(SUM(cant_evaluados), 0)
# MAGIC     , 2)                AS porcentaje_calidad
# MAGIC FROM  uc_axa_cli.gold.fact_resumen_reporte_calidad
# MAGIC WHERE ambito = 'Regla'
# MAGIC GROUP BY descripcion_ambito
# MAGIC ORDER BY porcentaje_calidad ASC

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Vista completa con semáforo — todos los ámbitos

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT
# MAGIC     ambito,
# MAGIC     descripcion_ambito,
# MAGIC     cant_evaluados,
# MAGIC     cant_validos,
# MAGIC     cant_invalidos,
# MAGIC     cant_remediados,
# MAGIC     ROUND(
# MAGIC         cant_validos * 100.0
# MAGIC         / NULLIF(cant_evaluados, 0)
# MAGIC     , 2)                AS porcentaje_calidad,
# MAGIC     CASE
# MAGIC         WHEN ROUND(cant_validos * 100.0 / NULLIF(cant_evaluados, 0), 2) >= 95 THEN 'VERDE'
# MAGIC         WHEN ROUND(cant_validos * 100.0 / NULLIF(cant_evaluados, 0), 2) >= 80 THEN 'AMARILLO'
# MAGIC         ELSE 'ROJO'
# MAGIC     END                 AS semaforo_calidad
# MAGIC FROM  uc_axa_cli.gold.fact_resumen_reporte_calidad
# MAGIC ORDER BY
# MAGIC     CASE ambito
# MAGIC         WHEN 'Fuente'      THEN 1
# MAGIC         WHEN 'Cliente'     THEN 2
# MAGIC         WHEN 'Grupo Regla' THEN 3
# MAGIC         WHEN 'Atributo'    THEN 4
# MAGIC         WHEN 'Regla'       THEN 5
# MAGIC         ELSE                    6
# MAGIC     END,
# MAGIC     porcentaje_calidad ASC
