# Databricks notebook source
# NTT DATA
# MAGIC %md
# MAGIC # HUB_CLIENTE — Unificación de satélites silver
# MAGIC
# MAGIC Lee los 3 satélites ya migrados, los unifica y carga en `uc-axa-cli.silver.sv_hub_clientes`
# MAGIC mediante MERGE (upsert) sobre `pk_hub_cliente`.

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

# ── Columna de negocio que identifica al cliente en cada satélite ────────────
# Si en tus tablas el identificador se llama distinto, ajusta aquí.
# Dejar en None para que el script use la PK propia del satélite como BK.
BK_ARL = None   # ej: "num_documento"  — None → usa id_sat_arl
BK_BH  = None   # ej: "num_documento"  — None → usa id_sat_beyond_health
BK_PYC = None   # ej: "num_documento"  — None → usa id_sat_pyc

LOAD_TS = datetime.now(timezone.utc).isoformat(timespec="seconds")

print(f"Inicio : {LOAD_TS}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 2 — Lectura de satélites
# MAGIC
# MAGIC Cada satélite se lee completo. Se agrega `bk_cliente` (llave de negocio)
# MAGIC y `satelite` (trazabilidad). Si la columna BK no existe en el satélite
# MAGIC se usa su PK propia (`id_sat_*`).

def read_sat(table_sql: str, sat_name: str, pk_col: str, bk_col_override) -> "DataFrame":
    """
    Lee el satélite y normaliza la columna de negocio a `bk_cliente`.
    - bk_col_override: nombre real de la columna BK en ese satélite, o None.
    - pk_col: PK propia del satélite (id_sat_arl, etc.) usada como fallback.
    """
    df = spark.sql(f"SELECT * FROM {table_sql}")
    real_cols = [c.lower() for c in df.columns]

    # Determinar qué columna usar como llave de negocio
    if bk_col_override and bk_col_override.lower() in real_cols:
        bk_source = bk_col_override
    elif pk_col.lower() in real_cols:
        bk_source = pk_col
        print(f"  [{sat_name}] BK no configurada → usando '{pk_col}' como bk_cliente")
    else:
        bk_source = None
        print(f"  [{sat_name}] AVISO: no se encontró columna BK; bk_cliente será NULL")

    if bk_source:
        df = df.withColumn("bk_cliente", F.col(f"`{bk_source}`").cast("string"))
    else:
        df = df.withColumn("bk_cliente", F.lit(None).cast("string"))

    return df.withColumn("satelite", F.lit(sat_name))

sat_arl_df = read_sat(SAT_ARL, "sat_arl",           "id_sat_arl",           BK_ARL)
sat_bh_df  = read_sat(SAT_BH,  "sat_beyond_health", "id_sat_beyond_health", BK_BH)
sat_pyc_df = read_sat(SAT_PYC, "sat_pyc",           "id_sat_pyc",           BK_PYC)

cnt_arl = sat_arl_df.count()
cnt_bh  = sat_bh_df.count()
cnt_pyc = sat_pyc_df.count()
print(f"\n  ARL           : {cnt_arl:>10,} filas")
print(f"  Beyond Health : {cnt_bh:>10,} filas")
print(f"  PyC / SISE    : {cnt_pyc:>10,} filas")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 3 — Unificación y deduplicación
# MAGIC
# MAGIC 1. `unionByName(allowMissingColumns=True)` alinea esquemas distintos con NULL.
# MAGIC 2. Prioridad ARL > BH > PYC para desempatar duplicados por `bk_cliente`.
# MAGIC 3. Se genera `pk_hub_cliente` (BIGINT secuencial) como PK surrogate del HUB.
# MAGIC
# MAGIC **Nota:** el Window sin partición es esperado aquí (operación global de numeración).

unified_raw = (
    sat_arl_df
    .unionByName(sat_bh_df,  allowMissingColumns=True)
    .unionByName(sat_pyc_df, allowMissingColumns=True)
)

total_union = unified_raw.count()
print(f"Total filas unidas       : {total_union:>10,}")

# Descartar filas sin BK (solo si hay alguna con BK)
unified_with_bk = unified_raw.filter(
    F.col("bk_cliente").isNotNull() & (F.trim(F.col("bk_cliente")) != "")
)
total_with_bk = unified_with_bk.count()
total_null_bk = total_union - total_with_bk
print(f"Filas con bk_cliente     : {total_with_bk:>10,}")
print(f"Filas sin bk_cliente     : {total_null_bk:>10,}  (descartadas)")

# Deduplicar por bk_cliente conservando mayor prioridad de sistema
priority_expr = (
    F.when(F.col("satelite") == "sat_arl",           1)
     .when(F.col("satelite") == "sat_beyond_health", 2)
     .otherwise(3)
)
w_dedup = Window.partitionBy("bk_cliente").orderBy(priority_expr)

unified_dedup = (
    unified_with_bk
    .withColumn("_rn", F.row_number().over(w_dedup))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

total_dedup = unified_dedup.count()
print(f"Duplicados eliminados    : {total_with_bk - total_dedup:>10,}")
print(f"Filas únicas a cargar    : {total_dedup:>10,}")

# PK surrogate secuencial
w_seq = Window.orderBy("bk_cliente")
unified = (
    unified_dedup
    .withColumn("pk_hub_cliente", F.row_number().over(w_seq).cast("long"))
    .withColumn("fecha_cargue",   F.lit(LOAD_TS))
)

# Columnas de auditoría al frente
front_cols = ["pk_hub_cliente", "bk_cliente", "satelite", "fecha_cargue"]
rest_cols  = [c for c in unified.columns if c not in front_cols]
unified    = unified.select(front_cols + rest_cols)

print(f"\nColumnas en staging: {len(unified.columns)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 4 — Crear o recrear tabla destino
# MAGIC
# MAGIC Si la tabla existe pero su esquema es diferente al staging actual
# MAGIC (por ejemplo porque antes tenía menos columnas), se hace DROP + CREATE.
# MAGIC Los datos previos se pierden — el MERGE del bloque siguiente los recarga.

def table_exists(table_sql: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {table_sql}")
        return True
    except Exception:
        return False

def get_hub_columns(table_sql: str) -> set:
    try:
        return {r["col_name"].lower()
                for r in spark.sql(f"DESCRIBE TABLE {table_sql}")
                               .filter("col_name not like '#%'").collect()
                if r["col_name"].strip()}
    except Exception:
        return set()

staging_cols = {c.lower() for c in unified.columns}
hub_cols     = get_hub_columns(HUB_TABLE)
missing_in_hub = staging_cols - hub_cols

if not table_exists(HUB_TABLE):
    action = "CREATE"
elif missing_in_hub:
    print(f"  Columnas nuevas detectadas: {missing_in_hub}")
    print("  Recreando tabla con esquema actualizado...")
    spark.sql(f"DROP TABLE IF EXISTS {HUB_TABLE}")
    action = "CREATE"
else:
    action = "MERGE"

if action == "CREATE":
    unified.limit(0).createOrReplaceTempView("_hub_schema_ref")
    spark.sql(f"""
        CREATE TABLE {HUB_TABLE}
        USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        AS SELECT * FROM _hub_schema_ref
    """)
    spark.catalog.dropTempView("_hub_schema_ref")
    print("  Tabla creada.")
    # Refrescar columnas del hub para el MERGE
    hub_cols = get_hub_columns(HUB_TABLE)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 5 — Carga con MERGE (upsert)
# MAGIC
# MAGIC Solo se usan las columnas que existen en AMBOS lados (staging ∩ hub)
# MAGIC para evitar el error DELTA_MERGE_UNRESOLVED_EXPRESSION.
# MAGIC `pk_hub_cliente` no se actualiza en registros ya existentes.

# Intersección de columnas staging ↔ hub (en minúsculas para comparar)
common_cols = [c for c in unified.columns if c.lower() in hub_cols]

update_cols   = [c for c in common_cols if c not in ("pk_hub_cliente", "bk_cliente")]
update_set    = ",\n        ".join([f"tgt.`{c}` = src.`{c}`" for c in update_cols])
insert_cols   = ", ".join([f"`{c}`" for c in common_cols])
insert_values = ", ".join([f"src.`{c}`" for c in common_cols])

# Staging solo con columnas comunes
unified_common = unified.select(common_cols)
unified_common.createOrReplaceTempView("_hub_staging")

merge_sql = f"""
MERGE INTO {HUB_TABLE} AS tgt
USING _hub_staging AS src
  ON tgt.`bk_cliente` = src.`bk_cliente`
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

hub_count  = spark.sql(f"SELECT COUNT(*) AS n FROM {HUB_TABLE}").collect()[0]["n"]
null_bk    = spark.sql(f"SELECT COUNT(*) AS n FROM {HUB_TABLE} WHERE bk_cliente IS NULL OR TRIM(bk_cliente) = ''").collect()[0]["n"]
dup_bk     = spark.sql(f"SELECT COUNT(*) AS n FROM (SELECT bk_cliente FROM {HUB_TABLE} GROUP BY bk_cliente HAVING COUNT(*) > 1)").collect()[0]["n"]
null_fecha = spark.sql(f"SELECT COUNT(*) AS n FROM {HUB_TABLE} WHERE fecha_cargue IS NULL").collect()[0]["n"]

print("=" * 65)
print("  VALIDACIÓN POST-CARGA")
print("=" * 65)
print(f"\n  Filas staging (fuente)        : {total_dedup:>10,}")
print(f"  Filas en HUB tras MERGE       : {hub_count:>10,}")
print(f"  BK nulos                      : {null_bk:>10,}   {'✓' if null_bk == 0 else '✗ REVISAR'}")
print(f"  BK duplicados                 : {dup_bk:>10,}   {'✓' if dup_bk == 0 else '✗ REVISAR'}")
print(f"  Nulos en fecha_cargue         : {null_fecha:>10,}   {'✓' if null_fecha == 0 else '✗ REVISAR'}")

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

status_bk    = "OK" if null_bk    == 0 else "ERROR"
status_dup   = "OK" if dup_bk     == 0 else "ERROR"
status_fecha = "OK" if null_fecha  == 0 else "ERROR"
overall      = "OK" if all(s == "OK" for s in [status_bk, status_dup, status_fecha]) else "CON ADVERTENCIAS"

summary = pd.DataFrame([
    {"Validación": "BK sin nulos",       "Resultado": status_bk,    "Detalle": f"{null_bk} nulos"},
    {"Validación": "BK sin duplicados",  "Resultado": status_dup,   "Detalle": f"{dup_bk} duplicados"},
    {"Validación": "fecha_cargue OK",    "Resultado": status_fecha, "Detalle": f"{null_fecha} nulos"},
    {"Validación": "Filas en HUB",       "Resultado": "INFO",       "Detalle": f"{hub_count:,} registros"},
    {"Validación": "Timestamp de carga", "Resultado": "INFO",       "Detalle": LOAD_TS},
])

print(f"\n  Estado general: {overall}\n")
display(spark.createDataFrame(summary))

print("\n  Muestra del HUB (10 filas):")
display(spark.sql(f"SELECT pk_hub_cliente, bk_cliente, satelite, fecha_cargue FROM {HUB_TABLE} LIMIT 10"))
