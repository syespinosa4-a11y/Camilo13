# Databricks notebook source
# NTT DATA
# MAGIC %md
# MAGIC # Migración de tablas: axa_col_dv → uc-axa-cli.silver

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 1 — Configuración de tablas a migrar

MIGRATION_MAP = [  # Diccionario con tablas a migrar

    # ── sat_arl  ←  core_as400 : 6 tablas ──────────────────────────────────
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

    # ── sat_beyond_health  ←  core_bh : 7 tablas ────────────────────────────
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

    # ── sat_pyc  ←  core_sise : 16 tablas ───────────────────────────────────
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

# ── Mapa: nombre de tabla destino → columna PK secuencial ───────────────────
# NUEVO: define qué columna de ID se agrega a cada satélite
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
from datetime import datetime

spark = SparkSession.builder.getOrCreate()


def table_exists(table_sql: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {table_sql}")
        return True
    except Exception:
        return False


def get_id_col(target: str) -> str:
    """Devuelve el nombre de la columna PK para el satélite destino, o None."""
    table_name = target.split(".")[-1].strip("`")
    return TARGET_ID_MAP.get(table_name)


def get_max_id(target: str, id_col: str) -> int:
    """
    Consulta el máximo ID actual en la tabla destino para continuar la
    secuencia. Devuelve 0 si la tabla no existe o está vacía.
    """
    try:
        row = spark.sql(
            f"SELECT COALESCE(MAX(CAST(`{id_col}` AS BIGINT)), 0) AS m FROM {target}"
        ).collect()[0]
        return int(row["m"]) if row["m"] is not None else 0
    except Exception:
        return 0


def add_sequential_id(df, id_col: str, start_after: int):
    """
    NUEVO: Agrega la columna `id_col` como primer campo del DataFrame.
    Los valores van de (start_after + 1) hasta (start_after + n_filas),
    garantizando unicidad dentro de la ejecución actual.
    """
    window = Window.orderBy(F.monotonically_increasing_id())
    df = df.withColumn(
        id_col,
        (F.row_number().over(window) + F.lit(start_after)).cast("long"),
    )
    # Coloca el ID como primera columna
    return df.select([id_col] + [c for c in df.columns if c != id_col])


def read_source(source: str):
    sql_error = None

    # Estrategia 1: SQL con TRY_CAST
    try:
        cols = spark.sql(f"SELECT * FROM {source} LIMIT 0").columns
        cast_exprs = ", ".join([f"TRY_CAST(`{c}` AS STRING) AS `{c}`" for c in cols])
        df = spark.sql(f"SELECT {cast_exprs} FROM {source}")
        df.limit(1).collect()
        return df
    except Exception as e:
        sql_error = e

    # Estrategia 2: Parquet por path (fallback para tablas sin row filters)
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


def migrate_table(source: str, target: str) -> dict:
    result = {
        "source": source, "target": target,
        "status": None, "rows": None, "error": None,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }

    table_name = source.split(".")[-1].strip("`")
    schema_sql  = target.rsplit(".", 1)[0]
    tmp_target  = f"{schema_sql}.`_tmp_{table_name}`"
    bak_target  = f"{schema_sql}.`_bak_{table_name}`"
    temp_view   = f"_vw_{table_name}"

    try:
        spark.sql(f"DROP TABLE IF EXISTS {tmp_target}")

        df = read_source(source)
        row_count = df.count()

        # ── NUEVO: agregar columna PK secuencial ────────────────────────────
        id_col = get_id_col(target)
        if id_col:
            # Continúa desde el máximo ID existente en el destino (0 si no existe)
            max_id = get_max_id(target, id_col) if table_exists(target) else 0
            df = add_sequential_id(df, id_col, start_after=max_id)
        # ────────────────────────────────────────────────────────────────────

        df.createOrReplaceTempView(temp_view)
        spark.sql(f"CREATE TABLE {tmp_target} AS SELECT * FROM {temp_view}")
        spark.catalog.dropTempView(temp_view)

        if table_exists(target):
            spark.sql(f"DROP TABLE IF EXISTS {bak_target}")
            spark.sql(f"ALTER TABLE {target} RENAME TO {bak_target}")

        spark.sql(f"ALTER TABLE {tmp_target} RENAME TO {target}")

        if table_exists(bak_target):
            spark.sql(f"DROP TABLE IF EXISTS {bak_target}")

        result["status"] = "OK"
        result["rows"]   = row_count
        print(f"  ✓  {source}  →  {target}  ({row_count:,} filas)")

    except Exception as exc:
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
# Imprime mensaje de inicio y crea lista vacía "log"

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
# Crea DataFrame con columnas relevantes y muestra resumen de resultados

import pandas as pd

summary_df = pd.DataFrame(log)[["source", "target", "status", "rows", "timestamp", "error"]]

ok_count    = summary_df[summary_df["status"] == "OK"].shape[0]
error_count = summary_df[summary_df["status"] == "ERROR"].shape[0]

print(f"\nTablas migradas correctamente : {ok_count}")
print(f"Tablas con error              : {error_count}")
print(f"Total procesadas              : {len(log)}\n")

summary_df["rows"] = summary_df["rows"].apply(lambda x: str(int(x)) if pd.notna(x) else "")
display(spark.createDataFrame(summary_df.fillna("")))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 5 — Diagnóstico de tablas con error

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
# Itera sobre el log; compara filas fuente vs destino y verifica que coinciden

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
