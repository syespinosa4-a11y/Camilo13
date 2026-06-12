# Databricks notebook source
# MAGIC %md
# MAGIC # Carga HUB_CLIENTE
# MAGIC
# MAGIC Unifica datos de los 3 satélites silver y los carga en `uc-axa-cli.gold.hub_cliente`.
# MAGIC
# MAGIC | Satélite fuente                          | Sistema origen |
# MAGIC |------------------------------------------|----------------|
# MAGIC | `uc-axa-cli.silver.sv_sat_arl`           | AS400 / ARL    |
# MAGIC | `uc-axa-cli.silver.sv_sat_beyond_health` | Beyond Health  |
# MAGIC | `uc-axa-cli.silver.sv_sat_pyc`           | SISE / PyC     |
# MAGIC
# MAGIC **Estrategia de carga:** MERGE (upsert) sobre la llave de negocio `bk_cliente`.
# MAGIC Registros nuevos se insertan; registros existentes se actualizan solo si cambia
# MAGIC algún atributo relevante (`dv_hashdiif`).

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 1 — Configuración

# ── Tablas fuente ────────────────────────────────────────────────────────────
SAT_ARL           = "`uc-axa-cli`.`silver`.`sv_sat_arl`"
SAT_BEYOND_HEALTH = "`uc-axa-cli`.`silver`.`sv_sat_beyond_health`"
SAT_PYC           = "`uc-axa-cli`.`silver`.`sv_sat_pyc`"

# ── Tabla destino ────────────────────────────────────────────────────────────
HUB_CLIENTE = "`uc-axa-cli`.`gold`.`hub_cliente`"

# ── Catálogo / esquema destino (para CREATE TABLE IF NOT EXISTS) ─────────────
HUB_CATALOG = "uc-axa-cli"
HUB_SCHEMA  = "gold"
HUB_TABLE   = "hub_cliente"

# ── Columna de llave de negocio unificada ────────────────────────────────────
# Cambia este valor al nombre real de la columna que identifica al cliente
# en cada satélite (puede ser distinto por satélite; ver Bloque 2).
BK_COL_ARL  = "num_identificacion"   # columna BK en sv_sat_arl
BK_COL_BH   = "num_identificacion"   # columna BK en sv_sat_beyond_health
BK_COL_PYC  = "num_identificacion"   # columna BK en sv_sat_pyc

# ── Sistema origen (para trazabilidad) ──────────────────────────────────────
RECORD_SOURCE_ARL = "ARL-AS400"
RECORD_SOURCE_BH  = "BEYOND_HEALTH"
RECORD_SOURCE_PYC = "SISE-PYC"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 2 — Mapeo de columnas hacia HUB_CLIENTE
# MAGIC
# MAGIC Define qué columna de cada satélite mapea a cada campo del HUB.
# MAGIC Deja el valor en `None` si el satélite no tiene ese campo.
# MAGIC El notebook intentará rellenar con `NULL` los campos ausentes.
# MAGIC
# MAGIC Formato:
# MAGIC ```
# MAGIC COLUMN_MAP = {
# MAGIC     "<campo_hub>": {
# MAGIC         "arl": "<col_en_sat_arl>",
# MAGIC         "bh":  "<col_en_sat_bh>",
# MAGIC         "pyc": "<col_en_sat_pyc>",
# MAGIC     },
# MAGIC     ...
# MAGIC }
# MAGIC ```

COLUMN_MAP = {
    # Identificación principal del cliente
    "bk_cliente":           {"arl": BK_COL_ARL,          "bh": BK_COL_BH,          "pyc": BK_COL_PYC},
    "tipo_identificacion":  {"arl": "tipo_identificacion","bh": "tipo_identificacion","pyc": "tipo_identificacion"},

    # Datos personales
    "primer_nombre":        {"arl": "primer_nombre",      "bh": "primer_nombre",     "pyc": "primer_nombre"},
    "segundo_nombre":       {"arl": "segundo_nombre",     "bh": "segundo_nombre",    "pyc": "segundo_nombre"},
    "primer_apellido":      {"arl": "primer_apellido",    "bh": "primer_apellido",   "pyc": "primer_apellido"},
    "segundo_apellido":     {"arl": "segundo_apellido",   "bh": "segundo_apellido",  "pyc": "segundo_apellido"},
    "fecha_nacimiento":     {"arl": "fecha_nacimiento",   "bh": "fecha_nacimiento",  "pyc": "fecha_nacimiento"},
    "genero":               {"arl": "genero",             "bh": "genero",            "pyc": "genero"},

    # Contacto
    "telefono":             {"arl": "telefono",           "bh": "telefono",          "pyc": "telefono"},
    "email":                {"arl": "email",              "bh": "email",             "pyc": "email"},

    # Dirección
    "direccion":            {"arl": "direccion",          "bh": "direccion",         "pyc": "direccion"},
    "ciudad":               {"arl": "ciudad",             "bh": "ciudad",            "pyc": "ciudad"},
    "departamento":         {"arl": "departamento",       "bh": "departamento",      "pyc": "departamento"},
    "pais":                 {"arl": "pais",               "bh": "pais",              "pyc": "pais"},
}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 3 — Inicialización

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from datetime import datetime, timezone
import hashlib

spark = SparkSession.builder.getOrCreate()

LOAD_TIMESTAMP = datetime.now(timezone.utc).isoformat(timespec="seconds")

print(f"Inicio : {LOAD_TIMESTAMP}")
print(f"Destino: {HUB_CLIENTE}\n")


def table_exists(table_sql: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {table_sql}")
        return True
    except Exception:
        return False


def available_columns(table_sql: str) -> set:
    """Devuelve el conjunto de columnas reales de una tabla (minúsculas)."""
    try:
        return {r["col_name"].lower()
                for r in spark.sql(f"DESCRIBE TABLE {table_sql}")
                               .filter("col_name not like '#%'")
                               .collect()
                if r["col_name"].strip()}
    except Exception:
        return set()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 4 — Lectura y normalización de cada satélite
# MAGIC
# MAGIC Por cada satélite:
# MAGIC 1. Se leen solo las columnas presentes en `COLUMN_MAP`.
# MAGIC 2. Se renombran al nombre canónico del HUB.
# MAGIC 3. Se agrega `record_source` para trazabilidad.
# MAGIC 4. Las columnas ausentes se rellenan con `NULL`.


def read_satellite(table_sql: str, source_tag: str, col_alias: dict) -> "DataFrame":
    """
    Lee `table_sql` y proyecta las columnas según `col_alias`
    {hub_col: sat_col}. Columnas ausentes → NULL STRING.
    """
    real_cols = available_columns(table_sql)
    select_exprs = []

    for hub_col, sat_col in col_alias.items():
        if sat_col and sat_col.lower() in real_cols:
            select_exprs.append(
                F.col(f"`{sat_col}`").cast(StringType()).alias(hub_col)
            )
        else:
            select_exprs.append(F.lit(None).cast(StringType()).alias(hub_col))

    select_exprs.append(F.lit(source_tag).alias("record_source"))

    df = spark.sql(f"SELECT * FROM {table_sql}")
    return df.select(select_exprs)


# Mapeo hub_col → sat_col por satélite
alias_arl = {hub: info["arl"] for hub, info in COLUMN_MAP.items()}
alias_bh  = {hub: info["bh"]  for hub, info in COLUMN_MAP.items()}
alias_pyc = {hub: info["pyc"] for hub, info in COLUMN_MAP.items()}

print("Leyendo satélites...")

df_arl = read_satellite(SAT_ARL,           RECORD_SOURCE_ARL, alias_arl)
df_bh  = read_satellite(SAT_BEYOND_HEALTH, RECORD_SOURCE_BH,  alias_bh)
df_pyc = read_satellite(SAT_PYC,           RECORD_SOURCE_PYC, alias_pyc)

print(f"  ARL           : {df_arl.count():>10,} filas")
print(f"  Beyond Health : {df_bh.count():>10,} filas")
print(f"  PyC/SISE      : {df_pyc.count():>10,} filas")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 5 — Unificación y deduplicación
# MAGIC
# MAGIC 1. `UNION ALL` de los 3 DataFrames.
# MAGIC 2. Elimina filas sin `bk_cliente`.
# MAGIC 3. Por cada `bk_cliente` duplicado se conserva la fila con mayor
# MAGIC    prioridad de sistema: ARL > BH > PYC (configurable en `SOURCE_PRIORITY`).
# MAGIC 4. Se genera `dv_hashdiff` (MD5 de todos los atributos) para detectar
# MAGIC    cambios en el MERGE posterior.

SOURCE_PRIORITY = {RECORD_SOURCE_ARL: 1, RECORD_SOURCE_BH: 2, RECORD_SOURCE_PYC: 3}

df_union = df_arl.unionByName(df_bh).unionByName(df_pyc)

# Descartar filas sin llave de negocio
df_union = df_union.filter(F.col("bk_cliente").isNotNull() & (F.trim(F.col("bk_cliente")) != ""))

# Prioridad de sistema origen
priority_expr = (
    F.when(F.col("record_source") == RECORD_SOURCE_ARL, 1)
     .when(F.col("record_source") == RECORD_SOURCE_BH,  2)
     .otherwise(3)
)

df_union = df_union.withColumn("_priority", priority_expr)

# Columnas de atributos para el hashdiff (excluye metadatos)
attr_cols = [c for c in COLUMN_MAP.keys() if c != "bk_cliente"]

# Concatenar atributos para MD5
concat_expr = F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("")) for c in attr_cols])
df_union = df_union.withColumn("dv_hashdiff", F.md5(concat_expr))

# Deduplicar: conservar fila de mayor prioridad por bk_cliente
from pyspark.sql.window import Window

w = Window.partitionBy("bk_cliente").orderBy("_priority")
df_dedup = (
    df_union
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_priority", "_rn")
)

total_before = df_union.count()
total_after  = df_dedup.count()
duplicates   = total_before - total_after

print(f"Filas antes de deduplicar : {total_before:>10,}")
print(f"Duplicados eliminados     : {duplicates:>10,}")
print(f"Filas únicas              : {total_after:>10,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 6 — Preparación final del DataFrame
# MAGIC
# MAGIC Agrega columnas de auditoría requeridas por el patrón HUB de Data Vault:
# MAGIC - `load_date`      — timestamp de carga (UTC)
# MAGIC - `record_source`  — sistema origen ganador tras la dedup
# MAGIC - `dv_hashdiff`    — huella MD5 de atributos (detecta cambios)

df_hub = (
    df_dedup
    .withColumn("load_date", F.lit(LOAD_TIMESTAMP))
)

# Orden de columnas en la tabla destino
hub_columns = (
    ["bk_cliente"]
    + attr_cols
    + ["record_source", "dv_hashdiff", "load_date"]
)

# Asegurarse de que todas las columnas existen (algunas pueden ser NULL)
for col in hub_columns:
    if col not in df_hub.columns:
        df_hub = df_hub.withColumn(col, F.lit(None).cast(StringType()))

df_hub = df_hub.select(hub_columns)

print(f"Columnas en df_hub: {df_hub.columns}")
print(f"Filas a cargar    : {df_hub.count():>10,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 7 — Creación de la tabla destino (si no existe)
# MAGIC
# MAGIC La tabla se crea con el esquema inferido del DataFrame.
# MAGIC Si ya existe, este bloque no hace nada.

if not table_exists(HUB_CLIENTE):
    print(f"Creando tabla {HUB_CLIENTE}...")
    df_hub.limit(0).createOrReplaceTempView("_hub_schema_ref")
    spark.sql(f"""
        CREATE TABLE {HUB_CLIENTE}
        USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        AS SELECT * FROM _hub_schema_ref
    """)
    spark.catalog.dropTempView("_hub_schema_ref")
    print("  Tabla creada.")
else:
    print(f"Tabla {HUB_CLIENTE} ya existe — se usará MERGE.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 8 — Carga con MERGE (upsert)
# MAGIC
# MAGIC - Si `bk_cliente` no existe en el HUB → INSERT.
# MAGIC - Si existe y `dv_hashdiff` cambió → UPDATE de atributos y `load_date`.
# MAGIC - Si existe y `dv_hashdiff` es igual → no se toca (sin I/O innecesario).

df_hub.createOrReplaceTempView("_hub_staging")

update_set = ",\n        ".join(
    [f"t.`{c}` = s.`{c}`" for c in attr_cols + ["record_source", "dv_hashdiff", "load_date"]]
)

insert_cols   = ", ".join([f"`{c}`" for c in hub_columns])
insert_values = ", ".join([f"s.`{c}`" for c in hub_columns])

merge_sql = f"""
MERGE INTO {HUB_CLIENTE} AS t
USING _hub_staging AS s
  ON t.bk_cliente = s.bk_cliente
WHEN MATCHED AND t.dv_hashdiff <> s.dv_hashdiff THEN
  UPDATE SET
        {update_set}
WHEN NOT MATCHED THEN
  INSERT ({insert_cols})
  VALUES ({insert_values})
"""

print("Ejecutando MERGE...\n")
print(merge_sql)

merge_result = spark.sql(merge_sql)
spark.catalog.dropTempView("_hub_staging")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 9 — Validación post-carga

print("=" * 65)
print("  VALIDACIÓN POST-CARGA")
print("=" * 65)

hub_count = spark.sql(f"SELECT COUNT(*) AS n FROM {HUB_CLIENTE}").collect()[0]["n"]
src_count = df_hub.count()

null_bk = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {HUB_CLIENTE}
    WHERE bk_cliente IS NULL OR TRIM(bk_cliente) = ''
""").collect()[0]["n"]

dup_bk = spark.sql(f"""
    SELECT COUNT(*) AS n FROM (
        SELECT bk_cliente FROM {HUB_CLIENTE}
        GROUP BY bk_cliente HAVING COUNT(*) > 1
    )
""").collect()[0]["n"]

print(f"\n  Filas en staging (fuente)     : {src_count:>10,}")
print(f"  Filas en {HUB_TABLE:<22}: {hub_count:>10,}")
print(f"  BK nulos o vacíos             : {null_bk:>10,}  {'✓' if null_bk == 0 else '✗ REVISAR'}")
print(f"  BK duplicados                 : {dup_bk:>10,}   {'✓' if dup_bk == 0 else '✗ REVISAR'}")

# Cobertura por sistema origen
print("\n  Distribución por record_source:")
spark.sql(f"""
    SELECT record_source, COUNT(*) AS filas
    FROM {HUB_CLIENTE}
    GROUP BY record_source
    ORDER BY filas DESC
""").show(truncate=False)

# Muestra de registros cargados
print("  Muestra (10 filas):")
display(spark.sql(f"SELECT * FROM {HUB_CLIENTE} LIMIT 10"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 10 — Resumen ejecutivo

status_bk   = "OK" if null_bk == 0 else "ERROR"
status_dup  = "OK" if dup_bk  == 0 else "ERROR"
overall     = "OK" if status_bk == "OK" and status_dup == "OK" else "CON ADVERTENCIAS"

import pandas as pd

summary = pd.DataFrame([
    {"Validación": "BK sin nulos",        "Resultado": status_bk,  "Detalle": f"{null_bk} nulos encontrados"},
    {"Validación": "BK sin duplicados",   "Resultado": status_dup, "Detalle": f"{dup_bk} duplicados encontrados"},
    {"Validación": "Filas en HUB",        "Resultado": "INFO",     "Detalle": f"{hub_count:,} registros"},
    {"Validación": "Timestamp de carga",  "Resultado": "INFO",     "Detalle": LOAD_TIMESTAMP},
])

print(f"\n  Estado general de la carga: {overall}\n")
display(spark.createDataFrame(summary))
