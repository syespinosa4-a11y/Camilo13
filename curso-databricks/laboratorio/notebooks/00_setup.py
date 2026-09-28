# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup y primer contacto (Módulos 1 y 4)
# MAGIC **Objetivo:** comprobar que tu workspace funciona, crear el esquema y el volumen del curso, y explorar Unity Catalog.
# MAGIC
# MAGIC 1. Arriba a la derecha, conecta el notebook a **Serverless**.
# MAGIC 2. Ejecuta celda por celda con `Shift+Enter`.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md ## ¿Dónde estoy?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user() AS usuario, current_catalog() AS catalogo, current_schema() AS esquema, current_version() AS version

# COMMAND ----------

# MAGIC %md ## Explorar Unity Catalog con SQL
# MAGIC Jerarquía: **catálogo → esquema → tablas / volúmenes / funciones / modelos**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- El catálogo `samples` trae datos de ejemplo
# MAGIC SHOW TABLES IN samples.nyctaxi

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED samples.nyctaxi.trips

# COMMAND ----------

# MAGIC %md ## Volúmenes: archivos dentro de Unity Catalog

# COMMAND ----------

# Con Python normal se puede escribir en un volumen
with open(f"{volumen}/hola.txt", "w") as f:
    f.write("Hola Databricks\n")

display(dbutils.fs.ls(volumen))
print(open(f"{volumen}/hola.txt").read())

# COMMAND ----------

# MAGIC %md ## information_schema: metadatos consultables con SQL

# COMMAND ----------

display(spark.sql(f"""
  SELECT table_catalog, table_schema, table_name, table_type
  FROM {catalogo}.information_schema.tables
  WHERE table_schema = '{esquema}'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Retos
# MAGIC 1. Abre **Catalog** en la barra lateral y encuentra tu esquema `curso_db` y el volumen `raw`. Sube un CSV cualquiera al volumen desde la interfaz.
# MAGIC 2. Léelo con `SELECT * FROM read_files('/Volumes/workspace/curso_db/raw/tu_archivo.csv', format => 'csv', header => true)`.
# MAGIC 3. Ejecuta `SHOW GRANTS ON SCHEMA curso_db`. ¿Qué permisos tienes?
# MAGIC 4. Pídele al **asistente de IA** (icono en el notebook): "explica qué hace la celda de _config".
