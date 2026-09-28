# Chuleta de Databricks

## Notebook
| | |
|---|---|
| `%sql` `%python` `%md` `%sh` `%pip install x` | Magics |
| `%run ./notebook` | Ejecutar otro notebook en el mismo contexto |
| `display(df)` | Tabla y gráficos |
| `dbutils.widgets.text("p", "valor")` / `.get("p")` | Parámetros |
| `dbutils.fs.ls("/Volumes/...")` | Listar archivos |
| `dbutils.secrets.get("scope", "key")` | Secretos |
| `dbutils.notebook.exit("ok")` | Terminar y devolver un valor |

## PySpark
```python
from pyspark.sql import functions as F, Window
df = spark.read.table("cat.esq.tabla")
df.filter(F.col("x") > 0).select("a", "b").withColumn("c", F.col("a") * 2)
df.groupBy("a").agg(F.sum("b").alias("total"), F.countDistinct("c"))
df.join(otro, "id", "left")                 # inner, left, right, full, anti, semi
df.withColumn("rn", F.row_number().over(Window.partitionBy("id").orderBy(F.desc("fecha"))))
df.dropDuplicates(["id"]).na.fill(0)
df.write.mode("overwrite").saveAsTable("cat.esq.nueva")   # append | overwrite
df.explain()
```

## SQL / Delta
```sql
USE CATALOG workspace; USE SCHEMA curso_db;
CREATE TABLE t (id INT, v STRING) CLUSTER BY (id);
CREATE TABLE t2 AS SELECT ...;                       -- CTAS
MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *;
DESCRIBE HISTORY t;  SELECT * FROM t VERSION AS OF 3;  RESTORE TABLE t TO VERSION AS OF 3;
OPTIMIZE t;  VACUUM t;  DESCRIBE DETAIL t;  DESCRIBE EXTENDED t;
SELECT * FROM read_files('/Volumes/c/e/v/dir', format => 'json');
COPY INTO t FROM '/Volumes/c/e/v/dir' FILEFORMAT = CSV FORMAT_OPTIONS ('header' = 'true');
CREATE VOLUME raw;  SHOW TABLES;  SHOW GRANTS ON TABLE t;
GRANT SELECT ON TABLE t TO `grupo`;
```

## Auto Loader
```python
spark.readStream.format("cloudFiles").option("cloudFiles.format","json") \
  .option("cloudFiles.schemaLocation", chk).load(ruta) \
  .writeStream.option("checkpointLocation", chk).trigger(availableNow=True).toTable("t")
```

## Pipelines declarativos
```sql
CREATE OR REFRESH STREAMING TABLE b AS SELECT * FROM STREAM read_files('...', format => 'json');
CREATE OR REFRESH MATERIALIZED VIEW g (CONSTRAINT ok EXPECT (x > 0) ON VIOLATION DROP ROW) AS SELECT ...;
```

## MLflow
```python
mlflow.set_registry_uri("databricks-uc"); mlflow.autolog()
with mlflow.start_run(): ...
mlflow.pyfunc.load_model("models:/cat.esq.modelo@champion")
```

## CLI
```bash
databricks auth login --host <url>
databricks bundle init | validate | deploy -t dev | run -t dev <job>
```
