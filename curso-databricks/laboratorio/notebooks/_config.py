# Databricks notebook source
# MAGIC %md
# MAGIC # Configuración compartida
# MAGIC Los demás notebooks lo ejecutan con `%run ./_config`. Crea los widgets `catalogo` y `esquema` y deja variables listas.
# MAGIC En **Databricks Free Edition** el catálogo por defecto se llama `workspace`.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
dbutils.widgets.text("esquema", "curso_db", "Esquema")

catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")
volumen = f"/Volumes/{catalogo}/{esquema}/raw"

spark.sql(f"USE CATALOG {catalogo}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {esquema}")
spark.sql(f"USE SCHEMA {esquema}")
spark.sql("CREATE VOLUME IF NOT EXISTS raw")

print(f"Usando {catalogo}.{esquema} · volumen: {volumen}")
