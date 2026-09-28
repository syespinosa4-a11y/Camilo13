# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Auto Loader + arquitectura medallón (Módulo 5)
# MAGIC Simulamos un sistema que deja **archivos JSON de pedidos** en un volumen. Construimos:
# MAGIC - **Bronze**: ingesta incremental con Auto Loader (cada archivo se procesa una sola vez).
# MAGIC - **Silver**: limpieza, tipos, calidad y deduplicación con `MERGE`.
# MAGIC - **Gold**: ventas agregadas para BI.
# MAGIC
# MAGIC 👉 Ejecuta el notebook completo **varias veces**: cada ejecución genera un lote nuevo y verás cómo solo se procesa lo nuevo.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

dbutils.widgets.dropdown("generar_lote", "si", ["si", "no"], "Generar lote nuevo")

origen = f"{volumen}/pedidos"
checkpoint_bronze = f"{volumen}/_checkpoints/pedidos_bronze"
dbutils.fs.mkdirs(origen)

# COMMAND ----------

# MAGIC %md ## 1 · Simular la llegada de un archivo (con datos "sucios" a propósito)

# COMMAND ----------

import json, random, uuid
from datetime import datetime, timedelta

def generar_lote(n=200):
    ciudades = ["Bogotá", "Medellín", "Cali", "Barranquilla", "Cartagena", None]
    productos = {"Café": 12.5, "Taza": 6.0, "Filtro": 3.2, "Molinillo": 45.0, "Termo": 22.0}
    ahora = datetime.now()
    pedidos = []
    for _ in range(n):
        prod = random.choice(list(productos))
        pedidos.append({
            "pedido_id": str(uuid.uuid4())[:8],
            "cliente": f"cliente_{random.randint(1, 50)}",
            "ciudad": random.choices(ciudades, weights=[30, 25, 20, 10, 10, 5])[0],   # ~5% sin ciudad
            "producto": prod,
            "cantidad": random.choice([1, 1, 2, 3, 5, -1]),                          # algunos negativos
            "precio_unitario": productos[prod],
            "fecha": (ahora - timedelta(days=random.randint(0, 30), minutes=random.randint(0, 1440))).isoformat(),
        })
    pedidos += random.sample(pedidos, 10)   # duplicados
    nombre = f"{origen}/pedidos_{ahora:%Y%m%d_%H%M%S}.json"
    with open(nombre, "w") as f:
        f.write("\n".join(json.dumps(p, ensure_ascii=False) for p in pedidos))
    return nombre

if dbutils.widgets.get("generar_lote") == "si":
    print("Archivo creado:", generar_lote())
display(dbutils.fs.ls(origen))

# COMMAND ----------

# MAGIC %md ## 2 · Bronze con Auto Loader

# COMMAND ----------

from pyspark.sql import functions as F

filas_antes = spark.table("pedidos_bronze").count() if spark.catalog.tableExists("pedidos_bronze") else 0

consulta = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", checkpoint_bronze)
    .option("cloudFiles.inferColumnTypes", "true")
    .load(origen)
    .select("*",
            F.col("_metadata.file_path").alias("archivo_origen"),
            F.current_timestamp().alias("ingestado_en"))
    .writeStream
    .option("checkpointLocation", checkpoint_bronze)
    .trigger(availableNow=True)
    .toTable("pedidos_bronze"))

consulta.awaitTermination()
print("Filas nuevas en bronze en esta ejecución:", spark.table("pedidos_bronze").count() - filas_antes)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT archivo_origen, count(*) AS filas, max(ingestado_en) AS ingestado
# MAGIC FROM pedidos_bronze GROUP BY archivo_origen ORDER BY ingestado

# COMMAND ----------

# MAGIC %md ## 3 · Silver: limpiar, validar, deduplicar y hacer MERGE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS pedidos_silver (
# MAGIC   pedido_id STRING, cliente STRING, ciudad STRING, producto STRING,
# MAGIC   cantidad INT, precio_unitario DECIMAL(10,2), total DECIMAL(12,2), fecha TIMESTAMP
# MAGIC ) CLUSTER BY (fecha);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS pedidos_cuarentena AS SELECT * FROM pedidos_bronze WHERE 1 = 0;

# COMMAND ----------

bronze = spark.table("pedidos_bronze")
es_valido = (F.col("cantidad") > 0) & F.col("ciudad").isNotNull()

# Filas inválidas → cuarentena (no se pierden: se pueden revisar)
invalidos = bronze.filter(~es_valido)
invalidos.join(spark.table("pedidos_cuarentena"), ["pedido_id", "archivo_origen"], "left_anti") \
         .write.mode("append").saveAsTable("pedidos_cuarentena")

silver_nuevo = (bronze.filter(es_valido)
    .dropDuplicates(["pedido_id"])
    .select("pedido_id", "cliente", "ciudad", "producto",
            F.col("cantidad").cast("int"),
            F.col("precio_unitario").cast("decimal(10,2)"),
            (F.col("cantidad") * F.col("precio_unitario")).cast("decimal(12,2)").alias("total"),
            F.to_timestamp("fecha").alias("fecha")))
silver_nuevo.createOrReplaceTempView("silver_nuevo")

spark.sql("""
  MERGE INTO pedidos_silver t USING silver_nuevo s ON t.pedido_id = s.pedido_id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""")

display(spark.sql("""
  SELECT (SELECT count(*) FROM pedidos_bronze)     AS bronze,
         (SELECT count(*) FROM pedidos_silver)     AS silver,
         (SELECT count(*) FROM pedidos_cuarentena) AS cuarentena
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 Aquí leemos **toda** la tabla bronze en cada ejecución (fácil de entender y correcto gracias al MERGE).
# MAGIC > En producción la harías incremental: leyendo bronze como stream con `foreachBatch`, con Change Data Feed o, lo más sencillo, con un **pipeline declarativo** (notebook 04).

# COMMAND ----------

# MAGIC %md ## 4 · Gold: tablas listas para negocio

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ventas_diarias_gold AS
# MAGIC SELECT date(fecha) AS dia, ciudad,
# MAGIC        count(*) AS pedidos, sum(cantidad) AS unidades, sum(total) AS ventas
# MAGIC FROM pedidos_silver GROUP BY ALL;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE top_productos_gold AS
# MAGIC SELECT producto, sum(cantidad) AS unidades, sum(total) AS ventas,
# MAGIC        rank() OVER (ORDER BY sum(total) DESC) AS ranking
# MAGIC FROM pedidos_silver GROUP BY producto;
# MAGIC
# MAGIC SELECT * FROM top_productos_gold ORDER BY ranking;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Retos
# MAGIC 1. Vuelve a ejecutar el notebook con `generar_lote = no`. ¿Cuántas filas procesa Auto Loader? (Debería ser 0).
# MAGIC 2. Genera un archivo con una **columna nueva** (p. ej. `"canal": "web"`) y observa qué hace Auto Loader con el esquema (pista: `cloudFiles.schemaEvolutionMode` y la columna `_rescued_data`).
# MAGIC 3. Haz Silver incremental: lee bronze con `spark.readStream.table("pedidos_bronze")` y usa `.foreachBatch(funcion_que_hace_merge)`.
# MAGIC 4. Abre **Catalog → ventas_diarias_gold → Lineage** y comprueba el linaje bronze → silver → gold.
