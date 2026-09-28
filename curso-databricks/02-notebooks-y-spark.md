# Módulo 2 — Notebooks y Apache Spark

## 2.1 Notebooks
- Cada notebook tiene un lenguaje por defecto; las **magics** cambian el de una celda: `%python`, `%sql`, `%scala`, `%r`, `%md` (Markdown), `%sh` (shell), `%pip` (instalar librerías), `%run ./otro_notebook` (ejecutar otro notebook en el mismo contexto).
- `display(df)` muestra tablas interactivas y permite crear gráficos sin código.
- `spark` (la SparkSession) ya viene creada.
- `dbutils`: utilidades → `dbutils.fs` (archivos), `dbutils.widgets` (parámetros), `dbutils.secrets` (secretos), `dbutils.notebook` (ejecutar/salir).
- **Widgets** = parámetros del notebook; los jobs los rellenan.
- Atajos: `Shift+Enter` ejecuta la celda; `Ctrl/Cmd+Shift+P` abre la paleta de comandos. Puedes comentar celdas, ver el historial de versiones y colaborar en tiempo real.

## 2.2 Cómo funciona Spark (lo imprescindible)
- **Driver** (coordina) + **executors** (procesan en paralelo particiones de los datos).
- **DataFrame**: una tabla distribuida e inmutable.
- **Transformaciones** (perezosas): `select`, `filter`, `withColumn`, `groupBy`, `join`… no se ejecutan hasta que llega una
- **Acción**: `count`, `show`, `display`, `collect`, `write`… que dispara un **job** → **stages** → **tasks**.
- **Shuffle**: mover datos entre nodos (`groupBy`, `join`, `orderBy`). Es lo caro; minimízalo.
- El optimizador **Catalyst** y **AQE** (Adaptive Query Execution) reescriben tu consulta. Usa `df.explain()` para ver el plan.

## 2.3 PySpark esencial
```python
from pyspark.sql import functions as F

df = spark.read.table("samples.nyctaxi.trips")       # leer tabla
df = (df.filter(F.col("trip_distance") > 0)
        .withColumn("tarifa_km", F.col("fare_amount") / F.col("trip_distance"))
        .select("pickup_zip", "trip_distance", "tarifa_km"))
resumen = df.groupBy("pickup_zip").agg(F.count("*").alias("viajes"),
                                       F.round(F.avg("tarifa_km"), 2).alias("media"))
display(resumen.orderBy(F.desc("viajes")))
resumen.write.mode("overwrite").saveAsTable("workspace.curso_db.resumen_zip")  # guardar
```
Leer archivos: `spark.read.format("csv").option("header", True).load("/Volumes/...")` (también `json`, `parquet`, `delta`).

## 2.4 Spark SQL: lo mismo en SQL
```sql
SELECT pickup_zip, count(*) AS viajes
FROM samples.nyctaxi.trips
GROUP BY pickup_zip ORDER BY viajes DESC LIMIT 10;
```
Pasar de uno a otro: `df.createOrReplaceTempView("v")` y luego `spark.sql("SELECT ... FROM v")`.
Muchos equipos usan **SQL para transformar** y **Python para orquestar, hacer ML o lógica compleja**. Domina ambos.

## 2.5 pandas en Spark
- `df.toPandas()` trae todo al driver: **solo con datos pequeños**.
- `import pyspark.pandas as ps` ofrece una API tipo pandas distribuida.
- Las **pandas UDFs** (`@F.pandas_udf`) aplican lógica de pandas por lotes, en paralelo.

➡️ Lab: `01_spark_basico`.
