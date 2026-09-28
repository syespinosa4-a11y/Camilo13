# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Delta Lake (Módulo 3)
# MAGIC Una tabla de productos que evoluciona: inserts, updates, MERGE, time travel, restore y optimización.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Empezamos desde cero para que el historial de versiones sea predecible
# MAGIC DROP TABLE IF EXISTS productos;
# MAGIC CREATE TABLE productos (
# MAGIC   id INT,
# MAGIC   nombre STRING,
# MAGIC   precio DECIMAL(10,2),
# MAGIC   stock INT,
# MAGIC   actualizado TIMESTAMP
# MAGIC ) CLUSTER BY (id)
# MAGIC COMMENT 'Productos de la tienda del curso';
# MAGIC
# MAGIC INSERT INTO productos VALUES
# MAGIC   (1, 'Café',  12.50, 100, current_timestamp()),
# MAGIC   (2, 'Taza',   6.00,  40, current_timestamp()),
# MAGIC   (3, 'Filtro', 3.20, 300, current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE productos SET precio = precio * 1.10, actualizado = current_timestamp() WHERE id = 1;
# MAGIC DELETE FROM productos WHERE id = 3;
# MAGIC SELECT * FROM productos ORDER BY id;

# COMMAND ----------

# MAGIC %md ## MERGE (upsert): llega un lote de cambios

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW cambios AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (2, 'Taza',       5.50, 35, current_timestamp()),   -- existe: se actualiza
# MAGIC   (4, 'Molinillo', 45.00, 10, current_timestamp()),   -- nuevo: se inserta
# MAGIC   (5, 'Termo',     22.00,  0, current_timestamp())    -- nuevo pero sin stock: lo ignoramos
# MAGIC AS t(id, nombre, precio, stock, actualizado);
# MAGIC
# MAGIC MERGE INTO productos AS t
# MAGIC USING cambios AS s
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED AND s.stock > 0 THEN INSERT *;
# MAGIC
# MAGIC SELECT * FROM productos ORDER BY id;

# COMMAND ----------

# MAGIC %md ## Historial y time travel

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY productos

# COMMAND ----------

# MAGIC %sql
# MAGIC -- La versión 1 es la tabla tras el primer INSERT
# MAGIC SELECT * FROM productos VERSION AS OF 1 ORDER BY id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ¿Qué cambió entre la versión 1 y la actual?
# MAGIC SELECT * FROM productos EXCEPT SELECT * FROM productos VERSION AS OF 1

# COMMAND ----------

# MAGIC %md ## ¡Ups! Un borrado accidental... y RESTORE

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM productos;
# MAGIC SELECT count(*) AS filas_tras_el_error FROM productos;

# COMMAND ----------

# Busca la última versión anterior al DELETE y restaura
ultima_buena = spark.sql("DESCRIBE HISTORY productos").filter("operation != 'DELETE'").agg({"version": "max"}).first()[0]
spark.sql(f"RESTORE TABLE productos TO VERSION AS OF {ultima_buena}")
display(spark.table("productos"))

# COMMAND ----------

# MAGIC %md ## Schema enforcement vs. schema evolution

# COMMAND ----------

from pyspark.sql import functions as F

nuevos = spark.createDataFrame([(6, "Tetera", 30.0, 5, "cerámica")],
                               "id INT, nombre STRING, precio DOUBLE, stock INT, material STRING") \
              .withColumn("actualizado", F.current_timestamp()) \
              .withColumn("precio", F.col("precio").cast("decimal(10,2)"))

try:
    nuevos.write.mode("append").saveAsTable("productos")      # falla: la columna 'material' no existe
except Exception as e:
    print("Rechazado por schema enforcement ✅\n", str(e)[:300])

nuevos.write.mode("append").option("mergeSchema", "true").saveAsTable("productos")  # evoluciona el esquema
display(spark.table("productos"))

# COMMAND ----------

# MAGIC %md ## Calidad y mantenimiento

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE productos ADD CONSTRAINT precio_positivo CHECK (precio >= 0);
# MAGIC -- Prueba: esto debe fallar
# MAGIC -- INSERT INTO productos (id, nombre, precio, stock) VALUES (9, 'Error', -1, 1);
# MAGIC
# MAGIC OPTIMIZE productos;
# MAGIC DESCRIBE DETAIL productos;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Copia instantánea para experimentar sin miedo
# MAGIC CREATE OR REPLACE TABLE productos_pruebas SHALLOW CLONE productos;
# MAGIC SELECT * FROM productos_pruebas;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Retos
# MAGIC 1. Activa Change Data Feed: `ALTER TABLE productos SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`, haz 2 updates y consulta `SELECT * FROM table_changes('productos', <version>)`.
# MAGIC 2. Escribe un `MERGE` que además **borre** los productos con `stock = 0` que vengan en los cambios (`WHEN MATCHED AND s.stock = 0 THEN DELETE`).
# MAGIC 3. Descomenta el INSERT con precio negativo y comprueba que la constraint lo bloquea.
# MAGIC 4. Crea una tabla con `id BIGINT GENERATED ALWAYS AS IDENTITY` e inserta filas sin indicar el id.
