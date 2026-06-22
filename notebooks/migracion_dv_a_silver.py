# Databricks notebook source
# NTT DATA 2026
# Migración de tablas a los satélites — Data Vault
# MAGIC %md
# MAGIC # Migración de satélites: axa_col_dv → uc-axa-cli.silver
# MAGIC
# MAGIC Cada satélite acumula TODAS las tablas fuente de su sistema origen,
# MAGIC unidas horizontalmente (UNION ALL con mergeSchema).
# MAGIC Columnas ausentes en alguna fuente se rellenan con NULL.
# MAGIC
# MAGIC | Satélite destino                         | Fuentes | Sistema  |
# MAGIC |------------------------------------------|---------|----------|
# MAGIC | uc-axa-cli.silver.sv_sat_arl             | 6       | AS400    |
# MAGIC | uc-axa-cli.silver.sv_sat_beyond_health   | 7       | BH       |
# MAGIC | uc-axa-cli.silver.sv_sat_pyc             | 16      | SISE     |

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 1 — Configuración

# Cada entrada: fuente → satélite destino
MIGRATION_MAP = [

    # ── sv_sat_arl  ←  core_as400 : 6 tablas ────────────────────────────────
    {
        "source": "`axa_col_dv`.`core_as400`.`as_arafild0_aaempaf0`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_arl`",
    },
    {
        "source": "`axa_col_dv`.`core_as400`.`as_arafild0_aaafaaf0`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_arl`",
    },
    {
        "source": "`axa_col_dv`.`core_as400`.`as_arafild0_aaafnaf0`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_arl`",
    },
    {
        "source": "`axa_col_dv`.`core_as400`.`as_arafild0_aaciuaf0`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_arl`",
    },
    {
        "source": "`axa_col_dv`.`core_as400`.`as_arafild0_aaicoaf0`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_arl`",
    },
    {
        "source": "`axa_col_dv`.`core_as400`.`as_arafild0_aacenaf0`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_arl`",
    },

    # ── sv_sat_beyond_health  ←  core_bh : 7 tablas ─────────────────────────
    {
        "source": "`axa_col_dv`.`core_bh`.`bh_sa_person`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_beyond_health`",
    },
    {
        "source": "`axa_col_dv`.`core_bh`.`bh_sa_address`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_beyond_health`",
    },
    {
        "source": "`axa_col_dv`.`core_bh`.`bh_sa_institution`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_beyond_health`",
    },
    {
        "source": "`axa_col_dv`.`core_bh`.`bh_sa_country`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_beyond_health`",
    },
    {
        "source": "`axa_col_dv`.`core_bh`.`bh_sa_city`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_beyond_health`",
    },
    {
        "source": "`axa_col_dv`.`core_bh`.`bh_sa_affiliation_contract`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_beyond_health`",
    },
    {
        "source": "`axa_col_dv`.`core_bh`.`bh_sa_member`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_beyond_health`",
    },

    # ── sv_sat_pyc  ←  core_sise : 16 tablas ────────────────────────────────
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_mpersona`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_maseg_header`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_mpersona_dir`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_mpersona_telef`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_tmunicipio`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_sg_mpersona_aut_datos`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_tpais`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_tciuu`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_tdpto`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_magente`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_sv_pv_header`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_sv_tramo`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_sg_pv_header`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_sg_tramo`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_sv_di_header`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_sg_di_header`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_sg_di_benef`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
    },
]

# PK de cada satélite
TARGET_ID_MAP = {
    "sv_sat_arl":           "id_sat_arl",
    "sv_sat_beyond_health": "id_sat_beyond_health",
    "sv_sat_pyc":           "id_sat_pyc",
}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 2 — Funciones auxiliares

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timezone
from collections import defaultdict

spark = SparkSession.builder.getOrCreate()

LOAD_TS = datetime.now(timezone.utc).isoformat(timespec="seconds")


def _short_error(e: Exception) -> str:
    """
    Extrae solo la línea relevante del error, sin el stack trace completo
    de Java/Spark (que puede tener cientos de líneas).
    """
    text = str(e)
    first_line = text.strip().splitlines()[0] if text.strip() else text
    # Si el mensaje real viene después de un prefijo de excepción Java,
    # nos quedamos con la primera línea (suele contener el código de error,
    # p.ej. [UNAUTHORIZED_ACCESS] o [TABLE_OR_VIEW_NOT_FOUND]).
    return first_line[:400]


def _is_permission_error(e: Exception) -> bool:
    text = str(e).upper()
    keywords = [
        "UNAUTHORIZED_ACCESS",
        "PERMISSION_DENIED",
        "ACCESS_DENIED",
        "FORBIDDEN",
        "403",
        "AUTHORIZATIONFAILURE",
        "DOES NOT HAVE PERMISSION",
        "ROW FILTER OR COLUMN MASK",
    ]
    return any(k in text for k in keywords)


def table_exists(table_sql: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {table_sql}")
        return True
    except Exception:
        return False


def get_id_col(target: str) -> str:
    table_name = target.split(".")[-1].strip("`")
    return TARGET_ID_MAP.get(table_name)


def read_source(source: str):
    """
    Lee la tabla fuente convirtiendo todo a STRING.
    Estrategia 1: SQL + TRY_CAST.
    Estrategia 2: Parquet por path (fallback sin row filters).
    """
    sql_error = None
    try:
        cols = spark.sql(f"SELECT * FROM {source} LIMIT 0").columns
        cast_exprs = ", ".join([f"TRY_CAST(`{c}` AS STRING) AS `{c}`" for c in cols])
        df = spark.sql(f"SELECT {cast_exprs} FROM {source}")
        df.limit(1).collect()
        return df
    except Exception as e:
        sql_error = e

    try:
        location = spark.sql(
            f"DESCRIBE DETAIL {source}"
        ).select("location").collect()[0][0]
        df = spark.read.option("mergeSchema", "true").format("parquet").load(location)
        for c in df.columns:
            df = df.withColumn(c, F.col(c).cast("string"))
        df.limit(1).collect()
        return df
    except Exception:
        raise sql_error


def union_all_sources(sources: list) -> "DataFrame":
    """
    Lee todas las tablas fuente de un satélite y las une con UNION ALL.
    Cada tabla aporta sus propias columnas; las columnas ausentes se
    rellenan con NULL → el satélite queda con TODAS las columnas de
    TODAS las fuentes.
    Agrega 'dv_record_source' para trazabilidad (nombre de la tabla fuente).
    """
    dfs = []
    failed = []

    for source in sources:
        try:
            df = read_source(source)
            # Nombre corto de la tabla para trazabilidad
            src_name = source.split(".")[-1].strip("`")
            df = df.withColumn("dv_record_source", F.lit(src_name))
            dfs.append(df)
            print(f"    ✓ leída  {source}  ({df.count():,} filas)")
        except Exception as e:
            es_permiso = _is_permission_error(e)
            failed.append({
                "source": source,
                "error": _short_error(e),
                "permiso_denegado": es_permiso,
            })
            motivo = "SIN PERMISOS" if es_permiso else "ERROR"
            print(f"    ✗ {motivo}  {source}  → {_short_error(e)}")

    if not dfs:
        raise RuntimeError("Ninguna tabla fuente pudo leerse.")

    # UNION ALL con allowMissingColumns=True:
    # columnas que no existen en una fuente aparecen como NULL en el resultado
    combined = dfs[0]
    for df in dfs[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)

    return combined, failed


def migrate_satellite(target: str, sources: list) -> dict:
    """
    Migra TODAS las fuentes de un satélite en una sola operación:
    1. Lee cada fuente (TRY_CAST → Parquet fallback).
    2. UNION ALL con allowMissingColumns → un DataFrame con todas las columnas.
    3. Agrega columnas Data Vault: dv_load_date, dv_record_source, PK secuencial.
    4. Escribe en tabla temporal y hace RENAME atómico al destino.
    """
    result = {
        "target":    target,
        "sources":   len(sources),
        "status":    None,
        "rows":      None,
        "cols":      None,
        "errors":    [],
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }

    table_name = target.split(".")[-1].strip("`")
    schema_sql  = target.rsplit(".", 1)[0]
    tmp_target  = f"{schema_sql}.`_tmp_{table_name}`"
    bak_target  = f"{schema_sql}.`_bak_{table_name}`"
    temp_view   = f"_vw_{table_name}"
    id_col      = get_id_col(target)

    try:
        spark.sql(f"DROP TABLE IF EXISTS {tmp_target}")

        print(f"\n  Leyendo {len(sources)} fuentes para {target}...")
        combined, failed = union_all_sources(sources)
        result["errors"] = failed

        # Columnas Data Vault de auditoría
        combined = combined.withColumn("dv_load_date", F.lit(LOAD_TS))

        # PK secuencial (primera columna del satélite)
        if id_col:
            window = Window.orderBy(F.monotonically_increasing_id())
            combined = combined.withColumn(
                id_col,
                F.row_number().over(window).cast("long")
            )
            # id_col al frente, luego auditoría, luego el resto
            audit_cols = [id_col, "dv_load_date", "dv_record_source"]
            data_cols  = [c for c in combined.columns if c not in audit_cols]
            combined   = combined.select(audit_cols + data_cols)

        row_count = combined.count()
        col_count = len(combined.columns)

        # Escribir en temporal
        combined.createOrReplaceTempView(temp_view)
        spark.sql(f"CREATE TABLE {tmp_target} AS SELECT * FROM {temp_view}")
        spark.catalog.dropTempView(temp_view)

        # Swap atómico: temporal → destino
        if table_exists(target):
            spark.sql(f"DROP TABLE IF EXISTS {bak_target}")
            spark.sql(f"ALTER TABLE {target} RENAME TO {bak_target}")

        spark.sql(f"ALTER TABLE {tmp_target} RENAME TO {target}")

        if table_exists(bak_target):
            spark.sql(f"DROP TABLE IF EXISTS {bak_target}")

        result["status"] = "OK"
        result["rows"]   = row_count
        result["cols"]   = col_count
        print(f"\n  ✓ {target}")
        print(f"    Filas  : {row_count:,}")
        print(f"    Columnas: {col_count}")
        if failed:
            print(f"    Fuentes con error: {len(failed)}/{len(sources)}")

    except Exception as exc:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {tmp_target}")
        except Exception:
            pass
        result["status"] = "ERROR"
        result["error"]  = _short_error(exc)
        print(f"\n  ✗ {target}  ERROR: {_short_error(exc)}")

    return result

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 3 — Agrupar fuentes por satélite y ejecutar migración
# MAGIC
# MAGIC A diferencia del script anterior que procesaba tabla por tabla
# MAGIC (y cada una reemplazaba a la anterior), ahora se agrupan TODAS
# MAGIC las fuentes de cada satélite y se escriben de una sola vez.

# Agrupar entradas del MIGRATION_MAP por target
satellites = defaultdict(list)
for entry in MIGRATION_MAP:
    satellites[entry["target"]].append(entry["source"])

print("=" * 65)
print(f"  INICIO MIGRACIÓN  {datetime.now().isoformat(timespec='seconds')}")
print(f"  Satélites a procesar: {len(satellites)}")
print("=" * 65)

log = []
for target, sources in satellites.items():
    log.append(migrate_satellite(target=target, sources=sources))

print("\n" + "=" * 65)
print(f"  FIN MIGRACIÓN     {datetime.now().isoformat(timespec='seconds')}")
print("=" * 65)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 4 — Resumen de resultados

import pandas as pd

rows_summary = []
for r in log:
    errores = r.get("errors", [])
    sin_permiso = sum(1 for e in errores if e.get("permiso_denegado"))
    rows_summary.append({
        "target":   r["target"],
        "status":   r["status"],
        "filas":    str(r["rows"]) if r.get("rows") is not None else "",
        "columnas": str(r["cols"]) if r.get("cols") is not None else "",
        "fuentes_ok":         str(r["sources"] - len(errores)),
        "fuentes_error":      str(len(errores)),
        "fuentes_sin_permiso": str(sin_permiso),
        "timestamp": r["timestamp"],
    })

summary_df = pd.DataFrame(rows_summary)
ok_count    = sum(1 for r in log if r["status"] == "OK")
error_count = sum(1 for r in log if r["status"] == "ERROR")

print(f"\nSatélites migrados correctamente : {ok_count}")
print(f"Satélites con error              : {error_count}")
print(f"Total procesados                 : {len(log)}\n")

display(spark.createDataFrame(summary_df.fillna("")))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 5 — Detalle de fuentes con error por satélite

for r in log:
    errores = r.get("errors", [])
    if errores:
        print(f"\n── Fuentes fallidas en {r['target']} ──")
        sin_permiso = [e for e in errores if e.get("permiso_denegado")]
        otros       = [e for e in errores if not e.get("permiso_denegado")]

        if sin_permiso:
            print(f"  SIN PERMISOS ({len(sin_permiso)}) — no se migraron, requieren acceso:")
            for e in sin_permiso:
                print(f"    - {e['source']}")
                print(f"      {e['error']}")

        if otros:
            print(f"  OTROS ERRORES ({len(otros)}):")
            for e in otros:
                print(f"    - {e['source']}")
                print(f"      {e['error']}")

if all(not r.get("errors") for r in log):
    print("  Todas las fuentes se leyeron correctamente.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 6 — Validación: columnas por satélite

print("Columnas cargadas por satélite:\n")
print(f"{'SATÉLITE':<50} {'COLUMNAS':>8}  {'FILAS':>12}")
print("-" * 75)

for r in log:
    if r["status"] != "OK":
        continue
    print(f"{r['target']:<50} {r['cols']:>8,}  {r['rows']:>12,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 7 — Validación: conteo fuente vs destino

print("Validación de conteos por tabla fuente vs satélite destino:\n")
print(f"{'FUENTE':<60} {'SRC':>10}  {'SAT':>12}  {'%':>6}")
print("-" * 95)

for target, sources in satellites.items():
    try:
        sat_total = spark.sql(
            f"SELECT COUNT(*) AS n FROM {target}"
        ).collect()[0]["n"]
    except Exception:
        sat_total = 0

    for source in sources:
        try:
            src_count = spark.sql(
                f"SELECT COUNT(*) AS n FROM {source}"
            ).collect()[0]["n"]
            src_name = source.split(".")[-1].strip("`")
            sat_rows_from_src = spark.sql(
                f"SELECT COUNT(*) AS n FROM {target} WHERE dv_record_source = '{src_name}'"
            ).collect()[0]["n"]
            pct = f"{sat_rows_from_src/src_count*100:.1f}%" if src_count > 0 else "N/A"
            match = "✓" if sat_rows_from_src == src_count else "✗"
            print(f"{source:<60} {src_count:>10,}  {sat_rows_from_src:>12,}  {match} {pct}")
        except Exception as ex:
            print(f"{source:<60} {'ERROR':>10}  {str(ex)[:30]}")
