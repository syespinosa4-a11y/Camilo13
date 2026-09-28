# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Tarea de control de calidad (Módulo 6)
# MAGIC Pensada para ejecutarse como **segunda tarea de un Job**, después del notebook 03.
# MAGIC Comprueba las tablas, publica *task values* y **falla** si algo va mal (el job se marcará como fallido y te avisará).

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

dbutils.widgets.text("max_pct_cuarentena", "20", "% máximo en cuarentena")
max_pct = float(dbutils.widgets.get("max_pct_cuarentena"))

bronze = spark.table("pedidos_bronze").count()
silver = spark.table("pedidos_silver").count()
cuarentena = spark.table("pedidos_cuarentena").count()
pct = round(100 * cuarentena / bronze, 1) if bronze else 0.0

print(f"bronze={bronze} silver={silver} cuarentena={cuarentena} ({pct}%)")

# Solo funciona dentro de un Job; en ejecución interactiva lo ignoramos
try:
    dbutils.jobs.taskValues.set(key="filas_silver", value=silver)
    dbutils.jobs.taskValues.set(key="pct_cuarentena", value=pct)
except Exception as e:
    print("taskValues no disponible fuera de un job:", e)

# COMMAND ----------

errores = []
if silver == 0:
    errores.append("La tabla silver está vacía")
if pct > max_pct:
    errores.append(f"Demasiadas filas en cuarentena: {pct}% > {max_pct}%")

if errores:
    raise Exception("Control de calidad FALLIDO: " + "; ".join(errores))

dbutils.notebook.exit(f"OK: {silver} filas en silver, {pct}% en cuarentena")
