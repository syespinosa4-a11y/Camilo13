# Módulo 7 — Databricks SQL y BI

## 7.1 SQL warehouses
Compute optimizado para SQL: Photon, caché de resultados y escalado automático.
**Serverless** arranca en segundos. Tipos: serverless, pro y classic. Tallas: 2X-Small … 4X-Large.
Se apagan solos tras un tiempo sin uso (*auto stop*); ajústalo para ahorrar.

## 7.2 SQL Editor
- Autocompletado, varias pestañas, parámetros `:nombre`, historial y el asistente de IA ("genera una consulta que…").
- Guarda consultas, prográmalas y compártelas.
- **Query History** y **Query Profile**: aquí se diagnostica una consulta lenta.

## 7.3 SQL de Databricks que conviene conocer
```sql
SELECT * FROM read_files('/Volumes/.../archivo.csv', format => 'csv', header => true);  -- leer archivos directamente
SELECT pedido:cliente.nombre FROM bronze;              -- navegar dentro de JSON guardado como texto
SELECT explode(items) FROM pedidos;                    -- arrays → filas
SELECT * FROM t QUALIFY row_number() OVER (PARTITION BY id ORDER BY fecha DESC) = 1;  -- deduplicar
CREATE VIEW v AS ...;  CREATE MATERIALIZED VIEW mv AS ...;
CREATE FUNCTION iva(x DOUBLE) RETURNS DOUBLE RETURN x * 0.19;
SELECT ai_query('databricks-meta-llama-3-3-70b-instruct', 'Resume: ' || comentario) FROM resenas;  -- LLM desde SQL
SELECT ai_analyze_sentiment(comentario), ai_classify(texto, array('queja','elogio')) FROM resenas;
```
Tipos útiles: `STRUCT`, `ARRAY`, `MAP` y `VARIANT` (JSON semiestructurado eficiente).

## 7.4 Visualización
- **AI/BI Dashboards**: tableros con filtros, programación y envío por email; se publican y comparten.
- **Genie** (AI/BI Genie): un espacio donde los usuarios de negocio preguntan en lenguaje natural ("¿qué ciudad vendió más en marzo?") sobre las tablas que tú eliges. Añade instrucciones y consultas de ejemplo para mejorar su precisión.
- **Alertas**: aviso cuando una consulta cumple una condición (p. ej. ventas del día < X).
- Herramientas externas (Power BI, Tableau): se conectan al warehouse por JDBC/ODBC o con *Partner Connect*.

➡️ Lab: `05_sql_analitica`.
