-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 · SQL analítico y BI (Módulo 7)
-- MAGIC Consultas sobre las tablas de los notebooks 03 y 04. Ejecútalas aquí y **también en el SQL Editor** (con un SQL warehouse), y crea un dashboard.

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA curso_db;

-- COMMAND ----------

-- KPIs generales
SELECT count(*) AS pedidos, sum(total) AS ventas, round(avg(total), 2) AS ticket_medio,
       count(DISTINCT cliente) AS clientes
FROM pedidos_silver;

-- COMMAND ----------

-- Serie temporal con media móvil de 7 días
SELECT dia, sum(ventas) AS ventas,
       round(avg(sum(ventas)) OVER (ORDER BY dia ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS media_movil_7d
FROM ventas_diarias_gold
GROUP BY dia ORDER BY dia;

-- COMMAND ----------

-- Participación de cada ciudad (%)
SELECT ciudad, sum(ventas) AS ventas,
       round(100 * sum(ventas) / sum(sum(ventas)) OVER (), 1) AS pct
FROM ventas_diarias_gold GROUP BY ciudad ORDER BY ventas DESC;

-- COMMAND ----------

-- Top 5 clientes y su producto favorito (CTE + ventanas + QUALIFY)
WITH por_cliente_producto AS (
  SELECT cliente, producto, sum(cantidad) AS unidades
  FROM pedidos_silver GROUP BY ALL
),
favorito AS (
  SELECT cliente, producto AS producto_favorito
  FROM por_cliente_producto
  QUALIFY row_number() OVER (PARTITION BY cliente ORDER BY unidades DESC) = 1
)
SELECT s.cliente, sum(s.total) AS gastado, f.producto_favorito
FROM pedidos_silver s JOIN favorito f USING (cliente)
GROUP BY s.cliente, f.producto_favorito
ORDER BY gastado DESC LIMIT 5;

-- COMMAND ----------

-- Pivot: unidades por ciudad y producto
SELECT * FROM (SELECT ciudad, producto, cantidad FROM pedidos_silver)
PIVOT (sum(cantidad) FOR producto IN ('Café', 'Taza', 'Filtro', 'Molinillo', 'Termo'));

-- COMMAND ----------

-- Una vista para los analistas y una función SQL reutilizable
CREATE OR REPLACE VIEW v_pedidos_con_iva AS
SELECT *, total * 1.19 AS total_con_iva FROM pedidos_silver;

CREATE OR REPLACE FUNCTION segmento(gasto DECIMAL(12,2))
RETURNS STRING
RETURN CASE WHEN gasto >= 500 THEN 'VIP' WHEN gasto >= 150 THEN 'Frecuente' ELSE 'Ocasional' END;

-- COMMAND ----------

SELECT segmento, count(*) AS clientes FROM (
  SELECT cliente, segmento(sum(total)) AS segmento FROM pedidos_silver GROUP BY cliente
) GROUP BY segmento;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## (Opcional) Funciones de IA en SQL
-- MAGIC Pueden no estar disponibles en todas las regiones o ediciones. Si fallan, sáltalas.

-- COMMAND ----------

SELECT comentario,
       ai_analyze_sentiment(comentario) AS sentimiento,
       ai_classify(comentario, array('envío', 'calidad', 'precio')) AS tema
FROM VALUES ('El café llegó tarde y frío'), ('Excelente calidad, repetiré'), ('Muy caro para lo que es')
  AS t(comentario);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ✅ Retos (en la interfaz)
-- MAGIC 1. **SQL Editor**: pega la consulta de participación por ciudad, añade un parámetro `:ciudad` en un `WHERE` y guárdala.
-- MAGIC 2. **Dashboard** (*New → Dashboard*): usa como datasets `ventas_diarias_gold` y `top_productos_gold`. Crea un contador de ventas totales, una línea de ventas por día, barras por ciudad y un filtro por ciudad. Publícalo.
-- MAGIC 3. **Genie** (*New → Genie space*): añade las tablas gold y silver y pregunta "¿cuál es la ciudad con mayor ticket medio?". Revisa el SQL que genera.
-- MAGIC 4. **Alerta**: crea una consulta `SELECT sum(ventas) FROM ventas_diarias_gold WHERE dia = current_date()` y una alerta que avise si es menor que 100.
-- MAGIC 5. Abre **Query History**, elige una consulta y examina su **Query Profile**.
