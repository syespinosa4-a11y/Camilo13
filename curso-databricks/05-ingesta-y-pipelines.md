# Módulo 5 — Ingesta y pipelines

## 5.1 Arquitectura medallón
| Capa | Contenido | Ejemplo |
|---|---|---|
| **Bronze** | Datos crudos tal como llegan, más metadatos (archivo, fecha de carga) | JSON de pedidos |
| **Silver** | Limpios, tipados, deduplicados, con joins | Pedidos válidos con cliente |
| **Gold** | Agregados listos para negocio/BI/ML | Ventas por día y ciudad |

## 5.2 Formas de ingerir
| Método | Cuándo |
|---|---|
| `COPY INTO` (SQL) | Carga incremental e idempotente de archivos, sencilla |
| **Auto Loader** (`cloudFiles`) | Archivos que llegan continuamente; escala a millones y detecta el esquema |
| **Lakeflow Connect** | Conectores gestionados (Salesforce, SQL Server, Google Analytics, etc.) |
| Streaming (Kafka, Kinesis, Event Hubs) | Eventos en tiempo real |
| Subir archivo desde la UI | Pruebas rápidas: *New → Add or upload data* |

Auto Loader:
```python
(spark.readStream.format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", checkpoint)      # guarda el esquema inferido
   .load("/Volumes/workspace/curso_db/raw/pedidos/")
 .writeStream
   .option("checkpointLocation", checkpoint)             # recuerda qué archivos ya procesó
   .trigger(availableNow=True)                           # procesa lo pendiente y se detiene
   .toTable("workspace.curso_db.pedidos_bronze"))
```
El **checkpoint** hace que cada archivo se procese **exactamente una vez**, aunque vuelvas a ejecutar.

## 5.3 Structured Streaming en 4 ideas
1. Un stream es una tabla que crece sin fin; se programa igual que un DataFrame.
2. Triggers: `availableNow=True` (incremental por lotes, lo más común) o `processingTime="1 minute"` (continuo, requiere clúster clásico).
3. Modos de salida: `append`, `complete`, `update`.
4. Para agregaciones con tiempo, usa `withWatermark` para limitar el estado.

## 5.4 Lakeflow Declarative Pipelines (antes Delta Live Tables)
Declaras **qué** tablas quieres y el sistema resuelve el **cómo**: orden, dependencias, reintentos, calidad e infraestructura.
```sql
CREATE OR REFRESH STREAMING TABLE pedidos_bronze
AS SELECT * FROM STREAM read_files('/Volumes/workspace/curso_db/raw/pedidos/', format => 'json');

CREATE OR REFRESH MATERIALIZED VIEW ventas_gold
AS SELECT ciudad, sum(total) AS ventas FROM pedidos_silver GROUP BY ciudad;
```
- **Streaming table**: procesa incrementalmente (ideal para bronze y silver).
- **Materialized view**: resultado recalculado de forma incremental cuando es posible (ideal para gold).
- **Expectations** (calidad de datos): `CONSTRAINT total_positivo EXPECT (total > 0) ON VIOLATION DROP ROW` (o `FAIL UPDATE`, o solo registrar).
- **AUTO CDC** / `APPLY CHANGES`: aplica flujos de cambios (CDC) y gestiona SCD tipo 1 y 2.
- En Python: `import dlt` con `@dlt.table` y `@dlt.expect_or_drop` (la API nueva es `from pyspark import pipelines as dp`).
- Se crean en **Jobs & Pipelines → Create → ETL pipeline**, y el grafo de dependencias se ve en la interfaz.

➡️ Labs: `03_autoloader_medallon` y `04_pipeline_declarativo`.
