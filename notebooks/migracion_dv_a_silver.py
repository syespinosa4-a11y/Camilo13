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
# MAGIC Cada entrada del mapa indica:
# MAGIC - `source`: tabla completa en el catálogo origen
# MAGIC - `target`: tabla satélite destino
# MAGIC - `mode`: `overwrite` reemplaza todo, `append` acumula filas
# MAGIC - `id_col`: nombre de la columna autoincremental NOT NULL que se genera al vuelo
# MAGIC   (pon `None` si la tabla destino no tiene esa restricción)
# MAGIC
# MAGIC **Solo las tablas listadas aquí serán procesadas.**

MIGRATION_MAP = [

    # ── sat_arl  ←  core_as400 ──────────────────────────────────────────────
    {
        "source": "`axa_col_dv`.`core_as400`.`as_arafild0_aaempaf0`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_arl`",
        "mode":   "overwrite",
        "id_col": "id_sat_arl",      # columna NOT NULL autoincremental en el destino
    },
    {
        "source": "`axa_col_dv`.`core_as400`.`as_arafild0_aaafaaf0`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_arl`",
        "mode":   "append",
        "id_col": "id_sat_arl",
    },

    # ── sat_beyond_health  ←  core_bh ───────────────────────────────────────
    {
        "source": "`axa_col_dv`.`core_bh`.`bh_sa_person`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_beyond_health`",
        "mode":   "overwrite",
        "id_col": "id_sat_beyond_health",
    },
    {
        "source": "`axa_col_dv`.`core_bh`.`bh_sa_address_telephone_number`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_beyond_health`",
        "mode":   "append",
        "id_col": "id_sat_beyond_health",
    },

    # ── sat_pyc  ←  core_sise ───────────────────────────────────────────────
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_mpersona`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
        "mode":   "overwrite",
        "id_col": "id_sat_pyc",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_maseg_header`",
        "target": "`uc-axa-cli`.`silver`.`sv_sat_pyc`",
        "mode":   "append",
        "id_col": "id_sat_pyc",
    },

    # Añade más entradas aquí con el mismo formato ...
]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 2 — Función auxiliar de migración
# MAGIC
# MAGIC `migrate_table` realiza los siguientes pasos:
# MAGIC 1. Lee la tabla fuente como DataFrame de Spark.
# MAGIC 2. Si `id_col` está definido, genera esa columna como número secuencial (`row_number`)
# MAGIC    para satisfacer la restricción NOT NULL del satélite destino.
# MAGIC    - En modo `overwrite` el contador parte desde 1.
# MAGIC    - En modo `append` consulta el máximo id actual en el destino y continúa desde ahí,
# MAGIC      evitando colisiones de claves con filas ya existentes.
# MAGIC 3. Escribe en Delta con `mergeSchema=true` para tolerar columnas distintas entre fuentes.
# MAGIC 4. Registra el resultado para el resumen del bloque 4.

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

def migrate_table(source: str, target: str, mode: str, id_col: str = None) -> dict:
    result = {"source": source, "target": target, "mode": mode,
              "status": None, "rows": None, "error": None,
              "timestamp": datetime.now().isoformat(timespec="seconds")}
    try:
        df = spark.sql(f"SELECT * FROM {source}")
        row_count = df.count()

        if id_col:
            if mode == "append":
                # Continúa la secuencia desde el máximo id existente en el destino
                try:
                    max_id = spark.sql(
                        f"SELECT COALESCE(MAX({id_col}), 0) AS max_id FROM {target}"
                    ).collect()[0]["max_id"]
                except Exception:
                    max_id = 0   # la tabla destino aún no existe
            else:
                max_id = 0       # overwrite: siempre parte desde 1

            window = Window.orderBy(F.monotonically_increasing_id())
            df = df.withColumn(id_col, F.row_number().over(window) + max_id)

        (
            df.write
              .format("delta")
              .mode(mode)
              .option("mergeSchema", "true")
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
            id_col=entry.get("id_col"),   # None si la entrada no declara id_col
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
