# Databricks notebook source
# NTT DATA
# MAGIC %md
# MAGIC # HUB_CLIENTE — Unificación de satélites silver
# MAGIC
# MAGIC Lee los 3 satélites ya migrados, los unifica y carga en `uc-axa-cli.silver.sv_hub_clientes`
# MAGIC mediante MERGE (upsert) sobre la llave de negocio `id_cliente`.
# MAGIC
# MAGIC | Satélite                                 | PK propia          |
# MAGIC |------------------------------------------|--------------------|
# MAGIC | `uc-axa-cli.silver.sv_sat_arl`           | `id_sat_arl`       |
# MAGIC | `uc-axa-cli.silver.sv_sat_beyond_health` | `id_sat_beyond_health` |
# MAGIC | `uc-axa-cli.silver.sv_sat_pyc`           | `id_sat_pyc`       |

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 1 — Configuración

import pyspark.sql.functions as F
from pyspark.sql import Window
from datetime import datetime, timezone

spark.conf.set("spark.sql.session.timeZone", "America/Bogota")

# Tablas fuente (backticks obligatorios por el guión en uc-axa-cli)
SAT_ARL   = "`uc-axa-cli`.`silver`.`sv_sat_arl`"
SAT_BH    = "`uc-axa-cli`.`silver`.`sv_sat_beyond_health`"
SAT_PYC   = "`uc-axa-cli`.`silver`.`sv_sat_pyc`"

# Tabla destino
HUB_TABLE = "`uc-axa-cli`.`silver`.`sv_hub_clientes`"

# Columna que actúa como llave de negocio del cliente en cada satélite.
# Ajusta este nombre si en tus tablas se llama distinto.
BK_COLUMN = "id_cliente"

LOAD_TS = datetime.now(timezone.utc).isoformat(timespec="seconds")

print(f"Inicio : {LOAD_TS}")
print(f"Destino: {HUB_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 2 — Lectura de satélites
# MAGIC
# MAGIC Se usa `spark.sql("SELECT * FROM ...")` con backticks para evitar el error
# MAGIC `INVALID_IDENTIFIER` que produce `spark.table()` con catálogos que contienen guión.

def read_sat(table_sql: str, sat_name: str, pk_col: str) -> "DataFrame":
    """
    Lee el satélite completo y agrega la columna de metadatos `satelite`.
    Si la columna BK_COLUMN no existe en el satélite, la crea como NULL.
    """
    df = spark.sql(f"SELECT * FROM {table_sql}")
    if BK_COLUMN not in df.columns:
        df = df.withColumn(BK_COLUMN, F.lit(None).cast("string"))
    return df.withColumn("satelite", F.lit(sat_name))

sat_arl_df = read_sat(SAT_ARL, "sat_arl",           "id_sat_arl")
sat_bh_df  = read_sat(SAT_BH,  "sat_beyond_health", "id_sat_beyond_health")
sat_pyc_df = read_sat(SAT_PYC, "sat_pyc",           "id_sat_pyc")

print(f"  ARL           : {sat_arl_df.count():>10,} filas")
print(f"  Beyond Health : {sat_bh_df.count():>10,} filas")
print(f"  PyC / SISE    : {sat_pyc_df.count():>10,} filas")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 3 — Unificación y deduplicación
# MAGIC
# MAGIC 1. `unionByName(allowMissingColumns=True)` alinea esquemas distintos rellenando con NULL.
# MAGIC 2. Se descartan filas sin `id_cliente`.
# MAGIC 3. Por cada `id_cliente` duplicado se conserva la fila con mayor prioridad:
# MAGIC    **ARL > Beyond Health > PyC**.
# MAGIC 4. Se genera `pk_hub_cliente` (BIGINT secuencial) como PK del HUB.

unified_raw = (
    sat_arl_df
    .unionByName(sat_bh_df,  allowMissingColumns=True)
    .unionByName(sat_pyc_df, allowMissingColumns=True)
)

# Descartar filas sin llave de negocio
unified_raw = unified_raw.filter(
    F.col(BK_COLUMN).isNotNull() & (F.trim(F.col(BK_COLUMN)) != "")
)

# Prioridad de sistema para romper empates
priority_expr = (
    F.when(F.col("satelite") == "sat_arl",           1)
     .when(F.col("satelite") == "sat_beyond_health", 2)
     .otherwise(3)
)
unified_raw = unified_raw.withColumn("_priority", priority_expr)

# Conservar solo la fila de mayor prioridad por id_cliente
w_dedup = Window.partitionBy(BK_COLUMN).orderBy("_priority")
unified_dedup = (
    unified_raw
    .withColumn("_rn", F.row_number().over(w_dedup))
    .filter(F.col("_rn") == 1)
    .drop("_priority", "_rn")
)

# PK secuencial del HUB
w_seq = Window.orderBy(BK_COLUMN)
unified = (
    unified_dedup
    .withColumn("pk_hub_cliente", F.row_number().over(w_seq).cast("long"))
    .withColumn("fecha_creacion", F.lit(LOAD_TS))
)

# pk_hub_cliente como primera columna
cols_ordered = ["pk_hub_cliente", BK_COLUMN, "satelite", "fecha_creacion"] + [
    c for c in unified.columns
    if c not in ("pk_hub_cliente", BK_COLUMN, "satelite", "fecha_creacion")
]
unified = unified.select(cols_ordered)

total_raw   = unified_raw.count()
total_dedup = unified.count()
print(f"Filas antes de deduplicar : {total_raw:>10,}")
print(f"Duplicados eliminados     : {total_raw - total_dedup:>10,}")
print(f"Filas a cargar en el HUB  : {total_dedup:>10,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 4 — Crear tabla destino si no existe

def table_exists(table_sql: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {table_sql}")
        return True
    except Exception:
        return False

if not table_exists(HUB_TABLE):
    print(f"Creando tabla {HUB_TABLE}...")
    unified.limit(0).createOrReplaceTempView("_hub_schema_ref")
    spark.sql(f"""
        CREATE TABLE {HUB_TABLE}
        USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        AS SELECT * FROM _hub_schema_ref
    """)
    spark.catalog.dropTempView("_hub_schema_ref")
    print("  Tabla creada.")
else:
    print(f"Tabla {HUB_TABLE} ya existe — se usará MERGE.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 5 — Carga con MERGE (upsert)
# MAGIC
# MAGIC - `id_cliente` nuevo → INSERT
# MAGIC - `id_cliente` existente → UPDATE de todos los atributos
# MAGIC
# MAGIC **Nota:** `pk_hub_cliente` no se actualiza en registros ya existentes
# MAGIC para preservar la estabilidad de la clave surrogate del HUB.

unified.createOrReplaceTempView("_hub_staging")

# Columnas a actualizar en MATCHED (todo excepto la PK surrogate y la BK)
update_cols = [
    c for c in unified.columns
    if c not in ("pk_hub_cliente", BK_COLUMN)
]
update_set    = ",\n        ".join([f"tgt.`{c}` = src.`{c}`" for c in update_cols])
insert_cols   = ", ".join([f"`{c}`" for c in unified.columns])
insert_values = ", ".join([f"src.`{c}`" for c in unified.columns])

merge_sql = f"""
MERGE INTO {HUB_TABLE} AS tgt
USING _hub_staging AS src
  ON tgt.`{BK_COLUMN}` = src.`{BK_COLUMN}`
WHEN MATCHED THEN
  UPDATE SET
        {update_set}
WHEN NOT MATCHED THEN
  INSERT ({insert_cols})
  VALUES ({insert_values})
"""

print("Ejecutando MERGE...\n")
spark.sql(merge_sql)
spark.catalog.dropTempView("_hub_staging")
print("  MERGE completado.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 6 — Validaciones post-carga

print("=" * 65)
print("  VALIDACIÓN POST-CARGA")
print("=" * 65)

hub_count = spark.sql(f"SELECT COUNT(*) AS n FROM {HUB_TABLE}").collect()[0]["n"]

null_bk = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {HUB_TABLE}
    WHERE `{BK_COLUMN}` IS NULL OR TRIM(`{BK_COLUMN}`) = ''
""").collect()[0]["n"]

dup_bk = spark.sql(f"""
    SELECT COUNT(*) AS n FROM (
        SELECT `{BK_COLUMN}` FROM {HUB_TABLE}
        GROUP BY `{BK_COLUMN}` HAVING COUNT(*) > 1
    )
""").collect()[0]["n"]

null_fecha = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {HUB_TABLE}
    WHERE fecha_creacion IS NULL
""").collect()[0]["n"]

print(f"\n  Filas staging (fuente)        : {total_dedup:>10,}")
print(f"  Filas en HUB tras MERGE       : {hub_count:>10,}")
print(f"  BK ({BK_COLUMN}) nulos        : {null_bk:>10,}   {'✓' if null_bk == 0 else '✗ REVISAR'}")
print(f"  BK duplicados                 : {dup_bk:>10,}   {'✓' if dup_bk == 0 else '✗ REVISAR'}")
print(f"  Nulos en fecha_creacion       : {null_fecha:>10,}   {'✓' if null_fecha == 0 else '✗ REVISAR'}")

print("\n  Distribución por satélite:")
spark.sql(f"""
    SELECT satelite, COUNT(*) AS filas
    FROM {HUB_TABLE}
    GROUP BY satelite
    ORDER BY filas DESC
""").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 7 — Resumen ejecutivo

import pandas as pd

status_bk    = "OK" if null_bk    == 0 else "ERROR"
status_dup   = "OK" if dup_bk     == 0 else "ERROR"
status_fecha = "OK" if null_fecha  == 0 else "ERROR"
overall      = "OK" if all(s == "OK" for s in [status_bk, status_dup, status_fecha]) else "CON ADVERTENCIAS"

summary = pd.DataFrame([
    {"Validación": "BK sin nulos",       "Resultado": status_bk,    "Detalle": f"{null_bk} nulos"},
    {"Validación": "BK sin duplicados",  "Resultado": status_dup,   "Detalle": f"{dup_bk} duplicados"},
    {"Validación": "fecha_creacion OK",  "Resultado": status_fecha, "Detalle": f"{null_fecha} nulos"},
    {"Validación": "Filas en HUB",       "Resultado": "INFO",       "Detalle": f"{hub_count:,} registros"},
    {"Validación": "Timestamp de carga", "Resultado": "INFO",       "Detalle": LOAD_TS},
])

print(f"\n  Estado general: {overall}\n")
display(spark.createDataFrame(summary))

print("\n  Muestra del HUB (10 filas):")
display(spark.sql(f"SELECT * FROM {HUB_TABLE} LIMIT 10"))
