# Databricks notebook source
# NTT DATA 2026
# Migración de tablas a los satélites — Data Vault
# MAGIC %md
# MAGIC # Migración de satélites: axa_col_dv → uc_axa_cli.silver
# MAGIC
# MAGIC Cada satélite acumula TODAS las tablas fuente de su sistema origen,
# MAGIC unidas horizontalmente (UNION ALL con mergeSchema).
# MAGIC Columnas ausentes en alguna fuente se rellenan con NULL.
# MAGIC
# MAGIC | Satélite destino                       | Fuentes | Sistema  |
# MAGIC |-----------------------------------------|---------|----------|
# MAGIC | uc_axa_cli.silver.sat_arl               | 6       | AS400    |
# MAGIC | uc_axa_cli.silver.sat_beyond_health     | 7       | BH       |
# MAGIC | uc_axa_cli.silver.sat_pyc               | 16      | SISE     |

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 1 — Configuración

# Cada entrada: fuente → satélite destino.
# Por defecto "modo": "UNION" (apilar filas, comportamiento histórico de
# los 3 satélites actuales). Un satélite puede en cambio declarar
# "modo": "JOIN" en TODAS sus entradas para combinar columnas de varias
# fuentes en una sola fila (join horizontal por una columna llave), en
# vez de apilarlas. Ver join_sources() en el Bloque 2.
MIGRATION_MAP = [

    # ── sat_arl  ←  core_as400 : 6 tablas ────────────────────────────────
    {
        "source": "`axa_col_dv`.`core_as400`.`as_arafild0_aaempaf0`",
        "target": "`uc_axa_cli`.`silver`.`sat_arl`",
    },
    {
        "source": "`axa_col_dv`.`core_as400`.`as_arafild0_aaafaaf0`",
        "target": "`uc_axa_cli`.`silver`.`sat_arl`",
    },
    {
        "source": "`axa_col_dv`.`core_as400`.`as_arafild0_aaafnaf0`",
        "target": "`uc_axa_cli`.`silver`.`sat_arl`",
    },
    {
        "source": "`axa_col_dv`.`core_as400`.`as_arafild0_aaciuaf0`",
        "target": "`uc_axa_cli`.`silver`.`sat_arl`",
    },
    {
        "source": "`axa_col_dv`.`core_as400`.`as_arafild0_aaicoaf0`",
        "target": "`uc_axa_cli`.`silver`.`sat_arl`",
    },
    {
        "source": "`axa_col_dv`.`core_as400`.`as_arafild0_aacenaf0`",
        "target": "`uc_axa_cli`.`silver`.`sat_arl`",
    },

    # ── sat_beyond_health  ←  core_bh : 7 tablas ─────────────────────────
    {
        "source": "`axa_col_dv`.`core_bh`.`bh_sa_person`",
        "target": "`uc_axa_cli`.`silver`.`sat_beyond_health`",
    },
    {
        "source": "`axa_col_dv`.`core_bh`.`bh_sa_address`",
        "target": "`uc_axa_cli`.`silver`.`sat_beyond_health`",
    },
    {
        "source": "`axa_col_dv`.`core_bh`.`bh_sa_institution`",
        "target": "`uc_axa_cli`.`silver`.`sat_beyond_health`",
    },
    {
        "source": "`axa_col_dv`.`core_bh`.`bh_sa_country`",
        "target": "`uc_axa_cli`.`silver`.`sat_beyond_health`",
    },
    {
        "source": "`axa_col_dv`.`core_bh`.`bh_sa_city`",
        "target": "`uc_axa_cli`.`silver`.`sat_beyond_health`",
    },
    {
        "source": "`axa_col_dv`.`core_bh`.`bh_sa_affiliation_contract`",
        "target": "`uc_axa_cli`.`silver`.`sat_beyond_health`",
    },
    {
        "source": "`axa_col_dv`.`core_bh`.`bh_sa_member`",
        "target": "`uc_axa_cli`.`silver`.`sat_beyond_health`",
    },

    # ── sat_pyc  ←  core_sise : 16 tablas ────────────────────────────────
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_mpersona`",
        "target": "`uc_axa_cli`.`silver`.`sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_maseg_header`",
        "target": "`uc_axa_cli`.`silver`.`sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_mpersona_dir`",
        "target": "`uc_axa_cli`.`silver`.`sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_mpersona_telef`",
        "target": "`uc_axa_cli`.`silver`.`sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_tmunicipio`",
        "target": "`uc_axa_cli`.`silver`.`sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_sg_mpersona_aut_datos`",
        "target": "`uc_axa_cli`.`silver`.`sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_tpais`",
        "target": "`uc_axa_cli`.`silver`.`sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_tciuu`",
        "target": "`uc_axa_cli`.`silver`.`sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_tdpto`",
        "target": "`uc_axa_cli`.`silver`.`sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_magente`",
        "target": "`uc_axa_cli`.`silver`.`sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_sv_pv_header`",
        "target": "`uc_axa_cli`.`silver`.`sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_sv_tramo`",
        "target": "`uc_axa_cli`.`silver`.`sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_sg_pv_header`",
        "target": "`uc_axa_cli`.`silver`.`sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_sg_tramo`",
        "target": "`uc_axa_cli`.`silver`.`sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_sv_di_header`",
        "target": "`uc_axa_cli`.`silver`.`sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_sg_di_header`",
        "target": "`uc_axa_cli`.`silver`.`sat_pyc`",
    },
    {
        "source": "`axa_col_dv`.`core_sise`.`ss_sg_di_benef`",
        "target": "`uc_axa_cli`.`silver`.`sat_pyc`",
    },

    # ── sat_afiliados_360  ←  ejemplo de modo JOIN: 2 fuentes combinadas
    # por columnas (no apiladas) ──────────────────────────────────────
    # {
    #     "source": "`axa_col_dv`.`bronze`.`af_persona`",
    #     "target": "`uc_axa_cli`.`silver`.`sat_afiliados_360`",
    #     "modo": "JOIN",
    #     "orden_join": 1,
    #     "columna_enlace": "id_persona",
    #     "tipo_join": "LEFT",
    #     "prefijo": "per_",
    # },
    # {
    #     "source": "`axa_col_dv`.`bronze`.`af_contacto`",
    #     "target": "`uc_axa_cli`.`silver`.`sat_afiliados_360`",
    #     "modo": "JOIN",
    #     "orden_join": 2,
    #     "columna_enlace": "id_persona=id_cliente",
    #     "tipo_join": "LEFT",
    #     "prefijo": "cto_",
    # },
]

# PK de cada satélite
TARGET_ID_MAP = {
    "sat_arl":           "id_sat_arl",
    "sat_beyond_health": "id_sat_beyond_health",
    "sat_pyc":           "id_sat_pyc",
}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 2 — Funciones auxiliares

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

spark = SparkSession.builder.getOrCreate()

# Algunas tablas fuente (p. ej. AS400) tienen columnas GENERATED ALWAYS AS
# (CAST(... AS BIGINT)): ese cast interno se ejecuta con CAST normal (no
# TRY_CAST) sin importar cómo se consulte la tabla desde afuera, y revienta
# con [CAST_INVALID_INPUT] si el valor crudo no es numérico válido. Desactivar
# ANSI a nivel de sesión hace que esos casts devuelvan NULL en vez de abortar.
try:
    spark.conf.set("spark.sql.ansi.enabled", "false")
except Exception:
    pass

LOAD_TS = datetime.now(timezone.utc).isoformat(timespec="seconds")

# Número de hilos para leer fuentes en paralelo (cada lectura es un job de
# Spark independiente; el driver puede despachar varios a la vez).
MAX_WORKERS = 6


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


def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def get_id_col(target: str) -> str:
    table_name = target.split(".")[-1].strip("`")
    return TARGET_ID_MAP.get(table_name)


def read_source(source: str):
    """
    Lee la tabla fuente convirtiendo todo a STRING, usando solo la API de
    DataFrame (sin sentencias SQL).
    Estrategia 1: spark.table(...) + try_cast columna por columna.
    Estrategia 2: lectura directa de archivos Parquet (fallback).
    """
    catalog_error = None
    try:
        raw = spark.table(source)
        df = raw.select(
            [F.try_cast(F.col(c), "string").alias(c) for c in raw.columns]
        )
        # IMPORTANTE: se valida con COUNT() (escaneo completo), no con
        # LIMIT(1). Algunas columnas tienen máscaras de Unity Catalog que
        # devuelven un placeholder de redacción (p. ej. '***' o 'XXXXXXX')
        # incompatible con el tipo real de la columna; ese placeholder solo
        # aparece en ciertas filas (no necesariamente la primera), así que
        # LIMIT(1) no detecta el problema y la falla solo se ve después,
        # al escribir el satélite completo — tumbando TODO el satélite por
        # una sola fuente. Forzar el escaneo completo aquí permite excluir
        # solo esta fuente puntual y dejar que el resto del satélite migre.
        df.count()
        return df
    except Exception as e:
        catalog_error = e

    try:
        location = (
            DeltaTable.forName(spark, source)
            .detail()
            .select("location")
            .collect()[0][0]
        )
        df = spark.read.option("mergeSchema", "true").format("parquet").load(location)
        for c in df.columns:
            df = df.withColumn(c, F.col(c).cast("string"))
        df.count()
        return df
    except Exception:
        raise catalog_error


def _read_one(source: str):
    """Lee una fuente y le agrega dv_record_source. Lanza la excepción tal cual
    para que el llamador decida cómo clasificarla."""
    df = read_source(source)
    # Blindaje extra: forzar TODAS las columnas a STRING explícitamente aquí
    # (no solo dentro de read_source). Si por cualquier motivo una columna
    # llegó con un tipo distinto de STRING, el unionByName posterior intenta
    # resolver un tipo común entre fuentes usando CAST normal (no TRY_CAST),
    # lo que puede reventar con [CAST_INVALID_INPUT] si un valor no es
    # numérico válido. Re-castear aquí garantiza que TODAS las fuentes ya
    # tengan tipos idénticos antes de unir, así Spark no necesita inferir
    # ni convertir nada en el union.
    df = df.select([F.col(c).cast("string").alias(c) for c in df.columns])
    src_name = source.split(".")[-1].strip("`")
    return df.withColumn("dv_record_source", F.lit(src_name))


def join_sources(entries: list):
    """
    Combina varias fuentes EN COLUMNAS (join horizontal) en vez de apilarlas.
    `entries` viene ordenado por "orden_join": la primera fuente es la base
    (izquierda); cada fuente siguiente se une a lo acumulado por su
    "columna_enlace" ("col" si el nombre es igual en ambos lados, o
    "col_izq=col_der" si difiere) usando "tipo_join" (default LEFT).
    Cada fuente puede declarar un "prefijo" para sus columnas y evitar
    colisiones de nombre con las demás fuentes unidas (la columna de enlace
    nunca se prefija, para poder seguir uniendo por ella).
    No hay paralelismo aquí: cada join depende del resultado del anterior.
    """
    entries_sorted = sorted(entries, key=lambda e: e.get("orden_join", 1))
    failed = []

    base = entries_sorted[0]
    try:
        combined = _read_one(base["source"])
        prefijo = base.get("prefijo")
        if prefijo:
            combined = combined.select([
                F.col(c).alias(f"{prefijo}{c}") if c != "dv_record_source" else F.col(c)
                for c in combined.columns
            ])
        print(f"    ✓ leída (base JOIN)  {base['source']}")
    except Exception as e:
        raise RuntimeError(
            f"La fuente base del JOIN no pudo leerse: {base['source']} → {_short_error(e)}"
        ) from e

    for entry in entries_sorted[1:]:
        source = entry["source"]
        try:
            df = read_source(source)
            df = df.select([F.col(c).cast("string").alias(c) for c in df.columns])

            enlace = entry.get("columna_enlace", "") or ""
            if "=" in enlace:
                left_col, right_col = [x.strip() for x in enlace.split("=", 1)]
            else:
                left_col = right_col = enlace.strip()

            prefijo = entry.get("prefijo")
            if prefijo:
                df = df.select([
                    F.col(c).alias(f"{prefijo}{c}") if c != right_col else F.col(c)
                    for c in df.columns
                ])

            join_type = (entry.get("tipo_join") or "LEFT").lower()
            combined = combined.join(
                df, combined[left_col] == df[right_col], how=join_type
            )
            if left_col != right_col:
                combined = combined.drop(df[right_col])

            print(f"    ✓ unida (JOIN {join_type.upper()})  {source}")
        except Exception as e:
            es_permiso = _is_permission_error(e)
            failed.append({
                "source": source,
                "error": _short_error(e),
                "permiso_denegado": es_permiso,
            })
            motivo = "SIN PERMISOS" if es_permiso else "ERROR"
            print(f"    ✗ {motivo}  {source}  → {_short_error(e)}")

    return combined, failed


def union_all_sources(sources: list) -> "DataFrame":
    """
    Lee todas las tablas fuente de un satélite EN PARALELO (hilos) y las une
    con UNION ALL. Cada tabla aporta sus propias columnas; las columnas
    ausentes se rellenan con NULL → el satélite queda con TODAS las columnas
    de TODAS las fuentes.
    No se hace count() por fuente aquí: forzaría un scan completo extra por
    cada tabla solo para imprimir un número; el conteo real se obtiene una
    sola vez, después de escribir el satélite (Bloque 3).
    """
    dfs = []
    failed = []

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(sources))) as pool:
        future_to_source = {pool.submit(_read_one, s): s for s in sources}
        for future in as_completed(future_to_source):
            source = future_to_source[future]
            try:
                dfs.append(future.result())
                print(f"    ✓ leída  {source}")
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


def migrate_satellite(target: str, entries: list) -> dict:
    """
    Migra TODAS las fuentes de un satélite en una sola operación. El modo de
    combinación se decide por entrada (campo "modo" en MIGRATION_MAP, default
    "UNION"): todas las entradas de un mismo satélite deben compartir el
    mismo modo.
      - UNION (default): apila filas de cada fuente (allowMissingColumns).
      - JOIN: combina columnas de cada fuente en una sola fila, uniendo por
        "columna_enlace" en el orden de "orden_join" (ver join_sources()).
    Luego, en ambos modos:
    1. Agrega columnas Data Vault: dv_load_date, dv_record_source, PK secuencial.
    2. Escribe directo al destino con writeTo(...).createOrReplace() (atómico).
    """
    sources = [e["source"] for e in entries]
    modo = "JOIN" if all((e.get("modo") or "UNION").upper() == "JOIN" for e in entries) else "UNION"

    result = {
        "target":    target,
        "sources":   len(sources),
        "modo":      modo,
        "status":    None,
        "rows":      None,
        "cols":      None,
        "errors":    [],
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }

    id_col = get_id_col(target)

    # Estado por fuente individual (independiente del estado del satélite):
    # se inicializa OK para todas y se corrige según lo que pase.
    result["source_status"] = {s: {"status": "OK", "error": ""} for s in sources}

    try:
        print(f"\n  Leyendo {len(sources)} fuentes para {target} (modo {modo})...")
        if modo == "JOIN":
            combined, failed = join_sources(entries)
        else:
            combined, failed = union_all_sources(sources)
        result["errors"] = failed
        for e in failed:
            result["source_status"][e["source"]] = {
                "status": "SIN PERMISO" if e["permiso_denegado"] else "ERROR LECTURA",
                "error": e["error"],
            }

        # Columnas Data Vault de auditoría
        combined = combined.withColumn("dv_load_date", F.lit(LOAD_TS))

        # PK secuencial: monotonically_increasing_id() es único y creciente
        # por partición, SIN necesitar un Window.orderBy() global (que
        # colapsa TODO el dataset en una sola partición para ordenar y es
        # el cuello de botella real con tablas grandes). No es estrictamente
        # consecutivo (puede tener huecos), pero es único — suficiente para
        # una PK secuencial de auditoría.
        if id_col:
            combined = combined.withColumn(
                id_col, F.monotonically_increasing_id().cast("long")
            )
            # id_col al frente, luego auditoría, luego el resto
            audit_cols = [id_col, "dv_load_date", "dv_record_source"]
            data_cols  = [c for c in combined.columns if c not in audit_cols]
            combined   = combined.select(audit_cols + data_cols)

        # NO se hace repartition("dv_record_source") antes de escribir:
        # las fuentes tienen tamaños muy distintos entre sí (una tabla
        # maestra grande junto a varias tablas de referencia pequeñas), así
        # que particionar por esa columna manda TODAS las filas de la fuente
        # grande a una sola partición/task — exactamente lo que se quedaba
        # "colgado" sin avanzar. Se deja que Spark/AQE decida el paralelismo
        # de escritura; Delta sigue escribiendo archivos separados por valor
        # de dv_record_source gracias al PARTITIONED BY de abajo.
        # No se usa spark.sparkContext (no existe en Spark Connect /
        # cómputo serverless): se lee el nivel de paralelismo desde la
        # configuración SQL, que sí está disponible en ambos modos.
        try:
            shuffle_partitions = int(spark.conf.get("spark.sql.shuffle.partitions"))
        except Exception:
            shuffle_partitions = 200
        combined = combined.repartition(shuffle_partitions)

        # Escritura atómica directa al destino con createOrReplace() (API
        # de DataFrame, sin SQL): reemplaza por completo la tabla destino
        # en una sola operación, sin necesidad de tabla temporal + RENAME.
        (
            combined.writeTo(target)
            .using("delta")
            .partitionedBy(F.col("dv_record_source"))
            .createOrReplace()
        )

        # Conteo y columnas a partir de la tabla YA escrita (un solo scan,
        # en vez de recomputar todo el DAG de lectura/unión otra vez).
        row_count = spark.table(target).count()
        col_count = len(spark.table(target).columns)

        result["status"] = "OK"
        result["rows"]   = row_count
        result["cols"]   = col_count
        print(f"\n  ✓ {target}")
        print(f"    Filas  : {row_count:,}")
        print(f"    Columnas: {col_count}")
        if failed:
            print(f"    Fuentes con error: {len(failed)}/{len(sources)}")

    except Exception as exc:
        result["status"] = "ERROR"
        result["error"]  = _short_error(exc)
        print(f"\n  ✗ {target}  ERROR: {_short_error(exc)}")

        # Si el satélite falla DESPUÉS de leer las fuentes (p. ej. en el
        # CREATE TABLE / write), ninguna fuente individual quedó marcada
        # como fallida (todas leyeron OK). Para que la tabla de detalle por
        # fuente no diga "OK" en fuentes que en realidad no se migraron,
        # se marcan como fallidas-por-error-de-satélite las que aún seguían
        # en estado OK.
        for s, st in result["source_status"].items():
            if st["status"] == "OK":
                st["status"] = "ERROR ESCRITURA SATÉLITE"
                st["error"] = _short_error(exc)

    return result

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 3 — Agrupar fuentes por satélite y ejecutar migración
# MAGIC
# MAGIC A diferencia del script anterior que procesaba tabla por tabla
# MAGIC (y cada una reemplazaba a la anterior), ahora se agrupan TODAS
# MAGIC las fuentes de cada satélite y se escriben de una sola vez.

# Agrupar entradas del MIGRATION_MAP por target (se conserva la entrada
# completa, no solo el source, para tener disponible "modo"/"orden_join"/
# "columna_enlace"/"tipo_join"/"prefijo" en migrate_satellite).
satellites = defaultdict(list)
for entry in MIGRATION_MAP:
    satellites[entry["target"]].append(entry)

print("=" * 65)
print(f"  INICIO MIGRACIÓN  {datetime.now().isoformat(timespec='seconds')}")
print(f"  Satélites a procesar: {len(satellites)}")
print("=" * 65)

log = []
for target, entries in satellites.items():
    log.append(migrate_satellite(target=target, entries=entries))

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


def _count_source(source: str):
    n = spark.table(source).count()
    return source, n


for target, entries in satellites.items():
    sources = [e["source"] for e in entries]
    modo = "JOIN" if all((e.get("modo") or "UNION").upper() == "JOIN" for e in entries) else "UNION"
    if modo == "JOIN":
        # En modo JOIN cada fila del satélite combina columnas de varias
        # fuentes; el conteo fuente-vs-destino fila a fila solo es
        # comparable contra la fuente base (orden_join=1), las demás
        # fuentes no aportan filas propias sino columnas adicionales.
        print(f"\n── {target} (modo JOIN, validación de conteo no aplica fuente por fuente) ──")
        continue
    # Un solo groupBy por satélite (aprovecha el partition pruning de
    # PARTITIONED BY dv_record_source) en vez de N consultas filtradas.
    try:
        sat_counts = {
            row["dv_record_source"]: row["n"]
            for row in (
                spark.table(target)
                .groupBy("dv_record_source")
                .count()
                .withColumnRenamed("count", "n")
                .collect()
            )
        }
    except Exception:
        sat_counts = {}

    # Conteos de fuentes en paralelo.
    src_counts = {}
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(sources))) as pool:
        futures = [pool.submit(_count_source, s) for s in sources]
        for future in as_completed(futures):
            try:
                source, n = future.result()
                src_counts[source] = n
            except Exception as ex:
                src_counts[future] = None  # marcado abajo como ERROR

    for source in sources:
        src_count = src_counts.get(source)
        if src_count is None:
            print(f"{source:<60} {'ERROR':>10}  no se pudo contar la fuente")
            continue
        src_name = source.split(".")[-1].strip("`")
        sat_rows_from_src = sat_counts.get(src_name, 0)
        pct = f"{sat_rows_from_src/src_count*100:.1f}%" if src_count > 0 else "N/A"
        match = "✓" if sat_rows_from_src == src_count else "✗"
        print(f"{source:<60} {src_count:>10,}  {sat_rows_from_src:>12,}  {match} {pct}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 8 — Detalle por tabla fuente (OK / ERROR y motivo)
# MAGIC
# MAGIC A diferencia del Bloque 4 (que resume por SATÉLITE), esta tabla muestra
# MAGIC el resultado de CADA tabla fuente individual: si se migró bien, si
# MAGIC falló por permisos, por error de lectura, o porque el satélite completo
# MAGIC falló en una etapa posterior (escritura/cast), aunque la fuente en sí
# MAGIC se haya leído correctamente.

tablas_detalle = []
for r in log:
    source_status = r.get("source_status", {})
    for source, st in source_status.items():
        src_name = source.split(".")[-1].strip("`")
        tablas_detalle.append({
            "tabla_fuente": src_name,
            "fuente_completa": source,
            "satelite_destino": r["target"],
            "status": st["status"],
            "motivo": st["error"],
        })

tablas_df = pd.DataFrame(tablas_detalle)

ok_tablas    = sum(1 for t in tablas_detalle if t["status"] == "OK")
error_tablas = len(tablas_detalle) - ok_tablas

print(f"Tablas migradas correctamente : {ok_tablas}")
print(f"Tablas con error               : {error_tablas}")
print(f"Total de tablas fuente         : {len(tablas_detalle)}\n")

display(spark.createDataFrame(tablas_df.fillna("")))
