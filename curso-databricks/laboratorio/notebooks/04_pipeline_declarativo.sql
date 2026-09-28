-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · Lakeflow Declarative Pipeline (Módulo 5)
-- MAGIC El **mismo** flujo medallón del notebook 03, pero declarativo: tú dices *qué* tablas quieres; el pipeline gestiona el orden, la incrementalidad, los checkpoints y la calidad.
-- MAGIC
-- MAGIC ⚠️ **Este archivo NO se ejecuta celda a celda.** Se usa como código fuente de un pipeline:
-- MAGIC 1. **Jobs & Pipelines → Create → ETL pipeline**.
-- MAGIC 2. Catálogo `workspace`, esquema `curso_db`, compute **Serverless**.
-- MAGIC 3. Añade este archivo como código fuente (source code) y pulsa **Start**.
-- MAGIC 4. Observa el **grafo**, las filas por tabla y las métricas de calidad (*expectations*).
-- MAGIC
-- MAGIC Requisito: haber ejecutado antes el notebook 03 al menos una vez (para que existan archivos en el volumen).
-- MAGIC Si tu catálogo o esquema son otros, cambia la ruta de `read_files`.

-- COMMAND ----------

-- BRONZE: streaming table = ingesta incremental (usa Auto Loader por debajo)
CREATE OR REFRESH STREAMING TABLE lf_pedidos_bronze
COMMENT 'Pedidos crudos desde archivos JSON'
AS SELECT *,
          _metadata.file_path AS archivo_origen,
          current_timestamp() AS ingestado_en
FROM STREAM read_files('/Volumes/workspace/curso_db/raw/pedidos/', format => 'json');

-- COMMAND ----------

-- SILVER: calidad declarativa con expectations
CREATE OR REFRESH STREAMING TABLE lf_pedidos_silver (
  CONSTRAINT cantidad_valida  EXPECT (cantidad > 0)         ON VIOLATION DROP ROW,
  CONSTRAINT ciudad_presente  EXPECT (ciudad IS NOT NULL)   ON VIOLATION DROP ROW,
  CONSTRAINT precio_razonable EXPECT (precio_unitario < 1000)   -- solo registra la métrica, no descarta
)
COMMENT 'Pedidos válidos y tipados'
AS SELECT pedido_id, cliente, ciudad, producto,
          CAST(cantidad AS INT)                             AS cantidad,
          CAST(precio_unitario AS DECIMAL(10,2))            AS precio_unitario,
          CAST(cantidad * precio_unitario AS DECIMAL(12,2)) AS total,
          to_timestamp(fecha)                               AS fecha
FROM STREAM(lf_pedidos_bronze);

-- COMMAND ----------

-- GOLD: materialized views (deduplicamos por pedido_id antes de agregar)
CREATE OR REFRESH MATERIALIZED VIEW lf_ventas_diarias_gold
COMMENT 'Ventas por día y ciudad'
AS SELECT date(fecha) AS dia, ciudad, count(*) AS pedidos, sum(total) AS ventas
FROM (SELECT * FROM lf_pedidos_silver QUALIFY row_number() OVER (PARTITION BY pedido_id ORDER BY fecha) = 1)
GROUP BY ALL;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW lf_top_productos_gold
AS SELECT producto, sum(cantidad) AS unidades, sum(total) AS ventas
FROM (SELECT * FROM lf_pedidos_silver QUALIFY row_number() OVER (PARTITION BY pedido_id ORDER BY fecha) = 1)
GROUP BY producto;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ✅ Retos
-- MAGIC 1. Ejecuta el notebook 03 con un lote nuevo y luego lanza otra actualización del pipeline. ¿Cuántas filas nuevas procesa bronze?
-- MAGIC 2. Cambia `precio_razonable` a `ON VIOLATION FAIL UPDATE` y a un umbral bajo (`< 10`). ¿Qué ocurre?
-- MAGIC 3. Sustituye la deduplicación de gold por una tabla silver creada con `AUTO CDC INTO` (en versiones anteriores, `APPLY CHANGES INTO`), usando `pedido_id` como clave.
-- MAGIC 4. Reescribe la capa bronze en **Python** (`import dlt` y `@dlt.table`) en otro archivo y añádelo al mismo pipeline.
-- MAGIC 5. Consulta el *event log* del pipeline para ver las métricas de calidad: `SELECT * FROM event_log(TABLE(lf_pedidos_silver))`.
