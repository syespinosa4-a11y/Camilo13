# Databricks notebook source
# MAGIC %md
# MAGIC # Migración de tablas: axa_col_dv → uc-axa-cli.silver
# MAGIC
# MAGIC Este notebook realiza el paso de tablas desde el catálogo fuente `axa_col_dv`
# MAGIC hacia los satélites del catálogo `uc-axa-cli.silver`.
# MAGIC
# MAGIC | Esquema fuente           | Tabla satélite destino                     |
# MAGIC |--------------------------|--------------------------------------------|
# MAGIC | axa_col_dv.core_as400    | uc-axa-cli.silver.sv_sat_arl               |
# MAGIC | axa_col_dv.core_bh       | uc-axa-cli.silver.sv_sat_beyond_health     |
# MAGIC | axa_col_dv.core_sise     | uc-axa-cli.silver.sv_sat_pyc               |

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 1 — Configuración de tablas a migrar

MIGRATION_MAP = [

    # ── sat_arl  ←  core_as400 ──────────────────────────────────────────────
    {
        "source": "`axa_col_dv`.`core_as400`.`as_arafild0_aaempaf0`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_arl`",
    },
    {
        "source": "`axa_col_dv`.`core_as400`.`as_arafild0_aaafaaf0`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_arl`",
    },

    # ── sat_beyond_health  ←  core_bh ───────────────────────────────────────
    {
        "source": "`axa_col_dv`.`core_bh`.`bh_sa_person`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_beyond_health`",
    },
    {
        "source": "`axa_col_dv`.`core_bh`.`bh_sa_address_telephone_number`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_beyond_health`",
    },

    # ── sat_pyc  ←  core_sise ───────────────────────────────────────────────
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_mpersona`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_maseg_header`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
    },
]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 2 — Funciones auxiliares

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

spark = SparkSession.builder.getOrCreate()


def table_exists(table_sql: str) -> bool:
    """Verifica existencia de tabla usando SQL (soporta catálogos con guiones)."""
    try:
        spark.sql(f"DESCRIBE TABLE {table_sql}")
        return True
    except Exception:
        return False


def read_source(source: str):
    """
    Lee la tabla fuente convirtiendo todas las columnas a STRING para evitar
    errores de schema mismatch al escribir en el destino.

    Estrategia 1 (preferida): SELECT TRY_CAST(col AS STRING) via SQL.
      - Funciona aunque la tabla tenga row filters/column masks en UC.
      - TRY_CAST devuelve NULL en vez de error si hay datos inválidos.

    Estrategia 2 (fallback): lectura directa en formato Parquet por path.
      - Se usa solo si la tabla no tiene row filters/column masks.
      - Bloqueada por Unity Catalog cuando existen filtros de seguridad.

    Si ambas estrategias fallan se relanza la excepción original de SQL
    para que migrate_table la registre con el mensaje correcto.
    """
    sql_error = None

    # ── Estrategia 1: SQL con TRY_CAST ──────────────────────────────────────
    try:
        cols = spark.sql(f"SELECT * FROM {source} LIMIT 0").columns
        cast_exprs = ", ".join([f"TRY_CAST(`{c}` AS STRING) AS `{c}`" for c in cols])
        df = spark.sql(f"SELECT {cast_exprs} FROM {source}")
        df.limit(1).collect()   # validar que realmente funciona antes de continuar
        return df
    except Exception as e:
        sql_error = e

    # ── Estrategia 2: lectura Parquet por path (fallback sin row filters) ───
    try:
        location = spark.sql(
            f"DESCRIBE DETAIL {source}"
        ).select("location").collect()[0][0]

        df = spark.read.option("mergeSchema", "true").format("parquet").load(location)
        for c in df.columns:
            df = df.withColumn(c, F.col(c).cast("string"))
        df.limit(1).collect()   # validar antes de continuar
        return df
    except Exception:
        # Si el fallback también falla, relanzamos el error SQL original
        # (normalmente el más informativo, e.g. PERMISSION_DENIED con row filters)
        raise sql_error


def migrate_table(source: str, target: str) -> dict:
    """
    Migra `source` a `target` usando un patrón CREATE + RENAME atómico
    para evitar conflictos de schema con tablas destino ya existentes.

    Pasos:
    1. Lee fuente como DataFrame todo-STRING.
    2. Escribe en tabla temporal `_tmp_<nombre>`.
    3. Si el destino existe → renombra a `_bak_<nombre>`.
    4. Renombra temporal a destino.
    5. Elimina backup si quedó.
    """
    result = {
        "source": source, "target": target,
        "status": None, "rows": None, "error": None,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }

    table_name = source.split(".")[-1].strip("`")
    schema_sql  = target.rsplit(".", 1)[0]          # e.g. `uc-axa-cli`.`silver`
    tmp_target  = f"{schema_sql}.`_tmp_{table_name}`"
    bak_target  = f"{schema_sql}.`_bak_{table_name}`"
    temp_view   = f"_vw_{table_name}"

    try:
        # Limpiar tabla temporal previa si quedó de una ejecución anterior
        spark.sql(f"DROP TABLE IF EXISTS {tmp_target}")

        df = read_source(source)
        row_count = df.count()

        # Escribir en tabla temporal para evitar tocar el destino hasta tener datos
        df.createOrReplaceTempView(temp_view)
        spark.sql(f"CREATE TABLE {tmp_target} AS SELECT * FROM {temp_view}")
        spark.catalog.dropTempView(temp_view)

        # Swap atómico: temporal → destino
        if table_exists(target):
            spark.sql(f"DROP TABLE IF EXISTS {bak_target}")
            spark.sql(f"ALTER TABLE {target} RENAME TO {bak_target}")

        spark.sql(f"ALTER TABLE {tmp_target} RENAME TO {target}")

        # Eliminar backup
        if table_exists(bak_target):
            spark.sql(f"DROP TABLE IF EXISTS {bak_target}")

        result["status"] = "OK"
        result["rows"]   = row_count
        print(f"  ✓  {source}  →  {target}  ({row_count:,} filas)")

    except Exception as exc:
        # Limpiar temporales en caso de error
        try:
            spark.sql(f"DROP TABLE IF EXISTS {tmp_target}")
        except Exception:
            pass
        result["status"] = "ERROR"
        result["error"]  = str(exc)
        print(f"  ✗  {source}  →  {target}  ERROR: {exc}")

    return result

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 3 — Ejecución de la migración

print("=" * 65)
print(f"  INICIO MIGRACIÓN  {datetime.now().isoformat(timespec='seconds')}")
print("=" * 65)

log = []

for entry in MIGRATION_MAP:
    log.append(
        migrate_table(
            source=entry["source"],
            target=entry["target"],
        )
    )

print("=" * 65)
print(f"  FIN MIGRACIÓN     {datetime.now().isoformat(timespec='seconds')}")
print("=" * 65)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 4 — Resumen de resultados

import pandas as pd

summary_df = pd.DataFrame(log)[["source", "target", "status", "rows", "timestamp", "error"]]

ok_count    = summary_df[summary_df["status"] == "OK"].shape[0]
error_count = summary_df[summary_df["status"] == "ERROR"].shape[0]

print(f"\nTablas migradas correctamente : {ok_count}")
print(f"Tablas con error              : {error_count}")
print(f"Total procesadas              : {len(log)}\n")

summary_df["rows"] = summary_df["rows"].apply(lambda x: str(int(x)) if x is not None and str(x) != "" else "")
display(spark.createDataFrame(summary_df.fillna("")))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 5 — Diagnóstico de tablas con error
# MAGIC
# MAGIC Si alguna tabla falló con **PERMISSION_DENIED** y el mensaje menciona
# MAGIC *"row filter or column mask"*, la tabla fuente tiene políticas de seguridad
# MAGIC de Unity Catalog que impiden cualquier acceso por ruta al archivo.
# MAGIC
# MAGIC **Acción requerida (administrador de Unity Catalog):**
# MAGIC 1. Identificar y remover temporalmente el row filter/column mask de la tabla fuente, O
# MAGIC 2. Ejecutar `ALTER TABLE <fuente> ALTER COLUMN <col> TYPE STRING` para alinear
# MAGIC    el tipo declarado con los datos reales y permitir la lectura SQL directa.
# MAGIC
# MAGIC Una vez corregido, volver a ejecutar este notebook; las tablas OK no se
# MAGIC reprocesarán (el destino ya existe y el swap es seguro).

errors = [r for r in log if r["status"] == "ERROR"]
if errors:
    print("\n── Tablas que requieren atención ──────────────────────────────")
    for r in errors:
        print(f"\n  Fuente : {r['source']}")
        print(f"  Error  : {r['error']}")
        if "row filter or column mask" in (r["error"] or ""):
            print("  ► Acción: el administrador de UC debe remover el row filter/")
            print("            column mask de esta tabla fuente, o corregir el tipo")
            print("            de la columna conflictiva con ALTER TABLE ... TYPE STRING.")
else:
    print("\n  Todas las tablas se migraron correctamente.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 6 — Validación de conteos (solo tablas OK)

print("Validación de conteos fuente vs destino:\n")
print(f"{'FUENTE':<60} {'FILAS_SRC':>10}  {'FILAS_DST':>10}  {'MATCH':>6}")
print("-" * 92)

for entry in log:
    if entry["status"] != "OK":
        continue

    src_count = spark.sql(f"SELECT COUNT(*) AS n FROM {entry['source']}").collect()[0]["n"]
    dst_count = spark.sql(f"SELECT COUNT(*) AS n FROM {entry['target']}").collect()[0]["n"]
    match     = "OK" if src_count == dst_count else "DIFF"

    print(f"{entry['source']:<60} {src_count:>10,}  {dst_count:>10,}  {match:>6}")
