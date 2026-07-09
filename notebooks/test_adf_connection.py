# Databricks notebook source
# MAGIC %md
# MAGIC # Test ADF → Databricks
# MAGIC Notebook de prueba para verificar que Azure Data Factory puede ejecutar notebooks correctamente.

# COMMAND ----------

print("=" * 50)
print("✅ Conexión ADF → Databricks: OK")
print("=" * 50)

# COMMAND ----------

# Captura parámetros que ADF puede enviar (widgets)
# Si ADF no envía nada, usa el valor por defecto
dbutils.widgets.text("env", "dev", "Ambiente")
dbutils.widgets.text("pipeline_run_id", "manual", "ADF Pipeline Run ID")

env            = dbutils.widgets.get("env")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")

print(f"Ambiente       : {env}")
print(f"ADF Run ID     : {pipeline_run_id}")

# COMMAND ----------

# Prueba mínima: un select en Spark
result = spark.sql("SELECT 'Hola mundo desde Databricks' AS mensaje, current_timestamp() AS ejecutado_en")
result.show(truncate=False)

# COMMAND ----------

# Valor de retorno para ADF (opcional)
# ADF puede leer este valor con la actividad "Get Notebook Output"
dbutils.notebook.exit("OK")
