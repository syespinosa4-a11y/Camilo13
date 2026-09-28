# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Spark básico (Módulo 2)
# MAGIC Usaremos `samples.nyctaxi.trips`: viajes de taxi en Nueva York.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

viajes = spark.read.table("samples.nyctaxi.trips")
viajes.printSchema()
print("Filas:", viajes.count())      # count() es una ACCIÓN: aquí sí se ejecuta un job

# COMMAND ----------

display(viajes.limit(10))

# COMMAND ----------

# MAGIC %md ## Transformaciones (perezosas) → acción
# MAGIC Nada se calcula hasta el `display`. Mira `explain()` para ver el plan que genera Catalyst.

# COMMAND ----------

limpio = (viajes
    .filter((F.col("trip_distance") > 0) & (F.col("fare_amount") > 0))
    .withColumn("duracion_min",
                (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60)
    .withColumn("hora", F.hour("tpep_pickup_datetime"))
    .withColumn("dia_semana", F.date_format("tpep_pickup_datetime", "E"))
    .withColumn("tarifa_por_milla", F.round(F.col("fare_amount") / F.col("trip_distance"), 2))
    .filter(F.col("duracion_min").between(1, 180)))

limpio.explain()

# COMMAND ----------

por_hora = (limpio.groupBy("hora")
            .agg(F.count("*").alias("viajes"),
                 F.round(F.avg("fare_amount"), 2).alias("tarifa_media"),
                 F.round(F.avg("duracion_min"), 1).alias("duracion_media"))
            .orderBy("hora"))
display(por_hora)   # 👉 en el resultado pulsa "+" → Visualization → gráfico de barras por hora

# COMMAND ----------

# MAGIC %md ## Funciones de ventana: top 3 viajes más caros por código postal de origen

# COMMAND ----------

w = Window.partitionBy("pickup_zip").orderBy(F.desc("fare_amount"))
top3 = (limpio.withColumn("rn", F.row_number().over(w))
        .filter("rn <= 3")
        .select("pickup_zip", "rn", "fare_amount", "trip_distance"))
display(top3.orderBy("pickup_zip", "rn"))

# COMMAND ----------

# MAGIC %md ## Lo mismo en SQL (vista temporal)

# COMMAND ----------

limpio.createOrReplaceTempView("viajes_limpios")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dia_semana, count(*) AS viajes, round(avg(tarifa_por_milla), 2) AS tarifa_milla
# MAGIC FROM viajes_limpios
# MAGIC GROUP BY dia_semana
# MAGIC ORDER BY viajes DESC

# COMMAND ----------

# MAGIC %md ## Guardar el resultado como tabla Delta gestionada

# COMMAND ----------

por_hora.write.mode("overwrite").saveAsTable("taxi_por_hora")
display(spark.table("taxi_por_hora"))

# COMMAND ----------

# MAGIC %md ## Joins: enriquecer con una tabla pequeña (broadcast)

# COMMAND ----------

franjas = spark.createDataFrame(
    [(h, "madrugada" if h < 6 else "mañana" if h < 12 else "tarde" if h < 19 else "noche") for h in range(24)],
    ["hora", "franja"])

por_franja = (limpio.join(F.broadcast(franjas), "hora")
              .groupBy("franja").agg(F.count("*").alias("viajes")))
display(por_franja)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Retos
# MAGIC 1. ¿Qué par (`pickup_zip`, `dropoff_zip`) tiene más viajes? Resuélvelo en **PySpark** y en **SQL**.
# MAGIC 2. Calcula la velocidad media (millas/hora) por franja horaria. ¿Cuándo hay más tráfico?
# MAGIC 3. Añade una columna `categoria` ("corto" < 2 millas, "medio" < 10, "largo") con `F.when(...).otherwise(...)` y cuenta viajes por categoría.
# MAGIC 4. Guarda el reto 3 como tabla `taxi_categorias` y búscala en **Catalog**. Mira su pestaña *Lineage*.
