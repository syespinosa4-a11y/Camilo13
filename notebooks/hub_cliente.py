# Databricks notebook source
# NTT DATA
# MAGIC %md
# MAGIC # HUB_CLIENTE — Carga desde satélites silver

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 1 — Configuración

import pyspark.sql.functions as F
from pyspark.sql import Window
from datetime import datetime, timezone

SAT_ARL   = "`uc-axa-cli`.`silver`.`sv_sat_arl`"
SAT_BH    = "`uc-axa-cli`.`silver`.`sv_sat_beyond_health`"
SAT_PYC   = "`uc-axa-cli`.`silver`.`sv_sat_pyc`"

HUB_TABLE = "`uc-axa-cli`.`silver`.`sv_hub_clientes`"

# Columnas que forman la llave de negocio id_cliente = tipo_doc + num_doc
# Si no existen en el satélite, id_cliente toma el valor de la PK propia
COL_TIPO_DOC = "tipo_doc"
COL_NUM_DOC  = "num_doc"

# Timestamp de carga en formato YYYYMMDDHHMMSS
LOAD_TS = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

print(f"Inicio : {LOAD_TS}")
print(f"Destino: {HUB_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 2 — Lectura y proyección de satélites
# MAGIC
# MAGIC Por cada satélite se extraen SOLO los 5 campos del HUB:
# MAGIC - `pk_hub_cliente` → se genera al final (Bloque 3)
# MAGIC - `id_satelite`    → PK propia del satélite (id_sat_arl, etc.)
# MAGIC - `id_cliente`     → tipo_doc + num_doc; si no existen usa la PK propia
# MAGIC - `satelite`       → nombre fijo del sistema origen
# MAGIC - `fecha_creacion` → timestamp de carga YYYYMMDDHHMMSS

def build_id_cliente(df, pk_col: str) -> "Column":
    """
    Construye id_cliente como tipo_doc-num_doc.
    Si alguna columna no existe, usa la PK propia del satélite.
    """
    cols = [c.lower() for c in df.columns]
    if COL_TIPO_DOC.lower() in cols and COL_NUM_DOC.lower() in cols:
        return F.concat_ws(
            "-",
            F.coalesce(F.col(f"`{COL_TIPO_DOC}`"), F.lit("")),
            F.coalesce(F.col(f"`{COL_NUM_DOC}`"),  F.lit(""))
        ).cast("string")
    else:
        print(f"  AVISO: '{COL_TIPO_DOC}'/'{COL_NUM_DOC}' no encontradas → id_cliente = {pk_col}")
        return F.col(f"`{pk_col}`").cast("string")


def read_sat(table_sql: str, sat_name: str, pk_col: str) -> "DataFrame":
    df = spark.sql(f"SELECT * FROM {table_sql}")
    return df.select(
        F.col(f"`{pk_col}`").cast("string").alias("id_satelite"),
        build_id_cliente(df, pk_col).alias("id_cliente"),
        F.lit(sat_name).alias("satelite"),
        F.lit(LOAD_TS).alias("fecha_creacion"),
    )


sat_arl_df = read_sat(SAT_ARL, "sat_arl",           "id_sat_arl")
sat_bh_df  = read_sat(SAT_BH,  "sat_beyond_health", "id_sat_beyond_health")
sat_pyc_df = read_sat(SAT_PYC, "sat_pyc",           "id_sat_pyc")

print(f"\n  ARL           : {sat_arl_df.count():>10,} filas")
print(f"  Beyond Health : {sat_bh_df.count():>10,} filas")
print(f"  PyC / SISE    : {sat_pyc_df.count():>10,} filas")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 3 — Unificación, deduplicación y PK secuencial

unified_raw = (
    sat_arl_df
    .unionByName(sat_bh_df)
    .unionByName(sat_pyc_df)
)

total_union = unified_raw.count()

# Descartar filas sin id_cliente
unified_clean = unified_raw.filter(
    F.col("id_cliente").isNotNull() & (F.trim(F.col("id_cliente")) != "")
)

# Deduplicar por id_cliente: prioridad ARL > BH > PYC
priority_expr = (
    F.when(F.col("satelite") == "sat_arl",           1)
     .when(F.col("satelite") == "sat_beyond_health", 2)
     .otherwise(3)
)
w_dedup = Window.partitionBy("id_cliente").orderBy(priority_expr)

unified_dedup = (
    unified_clean
    .withColumn("_rn", F.row_number().over(w_dedup))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# PK surrogate secuencial — pk_hub_cliente como primera columna
w_seq = Window.orderBy("id_cliente")
unified = (
    unified_dedup
    .withColumn("pk_hub_cliente", F.row_number().over(w_seq).cast("long"))
    .select("pk_hub_cliente", "id_satelite", "id_cliente", "satelite", "fecha_creacion")
)

total_dedup = unified.count()
print(f"Total filas unidas       : {total_union:>10,}")
print(f"Duplicados eliminados    : {total_union - total_dedup:>10,}")
print(f"Filas únicas a cargar    : {total_dedup:>10,}")

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
    unified.limit(0).createOrReplaceTempView("_hub_schema_ref")
    spark.sql(f"""
        CREATE TABLE {HUB_TABLE}
        USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        AS SELECT * FROM _hub_schema_ref
    """)
    spark.catalog.dropTempView("_hub_schema_ref")
    print("Tabla creada.")
else:
    print("Tabla ya existe — se usará MERGE.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 5 — Carga con MERGE (upsert)
# MAGIC
# MAGIC - id_cliente nuevo  → INSERT
# MAGIC - id_cliente existe → UPDATE (id_satelite, satelite, fecha_creacion)
# MAGIC - pk_hub_cliente    → NO se modifica en registros ya existentes

unified.createOrReplaceTempView("_hub_staging")

merge_sql = f"""
MERGE INTO {HUB_TABLE} AS tgt
USING _hub_staging AS src
  ON tgt.id_cliente = src.id_cliente
WHEN MATCHED THEN
  UPDATE SET
    tgt.id_satelite    = src.id_satelite,
    tgt.satelite       = src.satelite,
    tgt.fecha_creacion = src.fecha_creacion
WHEN NOT MATCHED THEN
  INSERT (pk_hub_cliente, id_satelite, id_cliente, satelite, fecha_creacion)
  VALUES (src.pk_hub_cliente, src.id_satelite, src.id_cliente, src.satelite, src.fecha_creacion)
"""

spark.sql(merge_sql)
spark.catalog.dropTempView("_hub_staging")
print("MERGE completado.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 6 — Validaciones post-carga

hub_count  = spark.sql(f"SELECT COUNT(*) AS n FROM {HUB_TABLE}").collect()[0]["n"]
null_bk    = spark.sql(f"SELECT COUNT(*) AS n FROM {HUB_TABLE} WHERE id_cliente IS NULL OR TRIM(id_cliente) = ''").collect()[0]["n"]
dup_bk     = spark.sql(f"SELECT COUNT(*) AS n FROM (SELECT id_cliente FROM {HUB_TABLE} GROUP BY id_cliente HAVING COUNT(*) > 1)").collect()[0]["n"]

print("=" * 65)
print("  VALIDACIÓN POST-CARGA")
print("=" * 65)
print(f"\n  Filas staging (fuente)        : {total_dedup:>10,}")
print(f"  Filas en HUB tras MERGE       : {hub_count:>10,}")
print(f"  id_cliente nulos              : {null_bk:>10,}   {'✓' if null_bk == 0 else '✗ REVISAR'}")
print(f"  id_cliente duplicados         : {dup_bk:>10,}   {'✓' if dup_bk == 0 else '✗ REVISAR'}")

print("\n  Distribución por satélite:")
spark.sql(f"""
    SELECT satelite, COUNT(*) AS filas
    FROM {HUB_TABLE}
    GROUP BY satelite ORDER BY filas DESC
""").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 7 — Resumen ejecutivo

import pandas as pd

summary = pd.DataFrame([
    {"Campo": "pk_hub_cliente", "Tipo": "BIGINT",      "Descripción": "PK surrogate secuencial del HUB"},
    {"Campo": "id_satelite",    "Tipo": "VARCHAR(50)", "Descripción": "PK del registro en el satélite origen"},
    {"Campo": "id_cliente",     "Tipo": "VARCHAR(50)", "Descripción": "BK: tipo_doc-num_doc (o PK satélite si no existen)"},
    {"Campo": "satelite",       "Tipo": "VARCHAR(50)", "Descripción": "Nombre del satélite fuente"},
    {"Campo": "fecha_creacion", "Tipo": "VARCHAR(14)", "Descripción": f"Timestamp carga YYYYMMDDHHMMSS — actual: {LOAD_TS}"},
])

display(spark.createDataFrame(summary))

ok = "OK" if null_bk == 0 and dup_bk == 0 else "CON ADVERTENCIAS"
print(f"\n  Estado general: {ok}")
print(f"  Registros en HUB: {hub_count:,}")

print("\n  Muestra del HUB (10 filas):")
display(spark.sql(f"SELECT * FROM {HUB_TABLE} LIMIT 10"))
