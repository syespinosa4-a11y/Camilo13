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
# MAGIC
# MAGIC Aquí se declara el mapa de migración. Cada entrada indica:
# MAGIC - `source`: tabla completa en el catálogo origen (`catalog.schema.table`)
# MAGIC - `target`: tabla satélite completa en el catálogo destino (`catalog.schema.table`)
# MAGIC - `mode`: modo de escritura (`overwrite` reemplaza todo, `append` acumula filas)
# MAGIC
# MAGIC **Solo las tablas listadas aquí serán procesadas.**
# MAGIC Añade o quita entradas según lo que necesites migrar.

MIGRATION_MAP = [

    # ── sat_arl  ←  core_as400 ──────────────────────────────────────────────
    {
        "source": "`axa_col_dv`.`core_as400`.`as_arafild0_aaactaf0`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_arl`",
        "mode":   "overwrite",           # cambia a "append" si prefieres acumular
    },
    {
        "source": "`axa_col_dv`.`core_as400`.`as_arafild0_aaafaaf0`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_arl`",
        "mode":   "append",
    },

    # ── sat_beyond_health  ←  core_bh ───────────────────────────────────────
    {
        "source": "`axa_col_dv`.`core_bh`.`bh_sa_account_to_pay`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_beyond_health`",
        "mode":   "overwrite",
    },
    {
        "source": "`axa_col_dv`.`core_bh`.`bh_sa_accounting_account_closing`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_beyond_health`",
        "mode":   "append",
    },

    # ── sat_pyc  ←  core_sise ───────────────────────────────────────────────
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_magente`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
        "mode":   "overwrite",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_maseg_header`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
        "mode":   "append",
    },

    # Añade más entradas aquí con el mismo formato ...
]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 2 — Función auxiliar de migración
# MAGIC
# MAGIC `migrate_table` realiza tres pasos por cada entrada del mapa:
# MAGIC 1. Lee la tabla fuente como DataFrame de Spark.
# MAGIC 2. Escribe en la tabla destino usando el modo configurado.
# MAGIC 3. Registra el resultado (éxito o error) para el resumen final.
# MAGIC
# MAGIC El uso de backticks en los nombres permite manejar catálogos con guiones
# MAGIC (ej. `uc-axa-cli`) sin que Spark los interprete como operadores de resta.

from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

def migrate_table(source: str, target: str, mode: str) -> dict:
    """
    Lee `source` y escribe en `target` con el modo indicado.
    Devuelve un dict con el resultado para el log final.
    """
    result = {"source": source, "target": target, "mode": mode,
              "status": None, "rows": None, "error": None,
              "timestamp": datetime.now().isoformat(timespec="seconds")}
    try:
        df = spark.sql(f"SELECT * FROM {source}")
        row_count = df.count()

        (
            df.write
              .format("delta")          # Unity Catalog exige formato Delta
              .mode(mode)
              .option("mergeSchema", "true")   # permite diferencias de esquema entre tablas fuente
              .saveAsTable(target)
        )

        result["status"] = "OK"
        result["rows"]   = row_count
        print(f"  ✓  {source}  →  {target}  [{mode}]  ({row_count:,} filas)")

    except Exception as exc:
        result["status"] = "ERROR"
        result["error"]  = str(exc)
        print(f"  ✗  {source}  →  {target}  [{mode}]  ERROR: {exc}")

    return result

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 3 — Ejecución de la migración
# MAGIC
# MAGIC Itera sobre cada entrada de `MIGRATION_MAP` en orden y llama a `migrate_table`.
# MAGIC Los resultados se acumulan en `log` para el resumen del bloque 4.
# MAGIC
# MAGIC Si una tabla falla, el proceso **continúa** con las siguientes
# MAGIC (el error queda registrado pero no detiene la ejecución).

print("=" * 65)
print(f"  INICIO MIGRACIÓN  {datetime.now().isoformat(timespec='seconds')}")
print("=" * 65)

log = []

for entry in MIGRATION_MAP:
    log.append(
        migrate_table(
            source=entry["source"],
            target=entry["target"],
            mode=entry["mode"],
        )
    )

print("=" * 65)
print(f"  FIN MIGRACIÓN     {datetime.now().isoformat(timespec='seconds')}")
print("=" * 65)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 4 — Resumen de resultados
# MAGIC
# MAGIC Muestra una tabla con el estado final de cada migración.
# MAGIC Útil para identificar rápidamente qué tablas fallaron y por qué.

import pandas as pd

summary_df = pd.DataFrame(log)[["source", "target", "mode", "status", "rows", "timestamp", "error"]]

ok_count    = summary_df[summary_df["status"] == "OK"].shape[0]
error_count = summary_df[summary_df["status"] == "ERROR"].shape[0]

print(f"\nTablas migradas correctamente : {ok_count}")
print(f"Tablas con error              : {error_count}")
print(f"Total procesadas              : {len(log)}\n")

display(spark.createDataFrame(summary_df.fillna("")))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 5 — Validación rápida (opcional)
# MAGIC
# MAGIC Compara el conteo de filas entre la tabla fuente y la destino
# MAGIC para verificar que la migración fue completa.
# MAGIC Solo se validan las tablas que tuvieron estado "OK" en el paso anterior.

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
