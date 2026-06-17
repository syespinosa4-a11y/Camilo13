# Databricks notebook source
# NTT DATA 2026
# MAGIC %md
# MAGIC # EVAL_REGLAS_CALIDAD — Motor de evaluación dinámica de reglas de calidad
# MAGIC
# MAGIC Lee las reglas desde `uc-axa-cli.silver.sv_reglas_calidad`, ejecuta la
# MAGIC expresión SQL parametrizada de cada una contra las 3 fuentes satélite
# MAGIC (ARL, Beyond Health, PyC) y registra el resultado en
# MAGIC `uc-axa-cli.gold.fact_reporte_de_calidad`.
# MAGIC
# MAGIC Maneja de forma aislada: mapeos de columna pendientes (`PENDIENTE_MAPEO`)
# MAGIC y errores de ejecución (`ERROR_EJECUCION`), sin detener el proceso global.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 1 — Configuración

import re
import traceback
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException, ParseException

spark = SparkSession.builder.getOrCreate()

SAT_ARL   = "`uc-axa-cli`.`silver`.`sv_sat_arl`"
SAT_SALUD = "`uc-axa-cli`.`silver`.`sv_sat_beyond_health`"
SAT_PYC   = "`uc-axa-cli`.`silver`.`sv_sat_pyc`"
HUB_TABLE = "`uc-axa-cli`.`silver`.`sv_hub_clientes`"

REGLAS_TABLE = "`uc-axa-cli`.`silver`.`sv_reglas_calidad`"
FACT_TABLE   = "`uc-axa-cli`.`gold`.`fact_reporte_de_calidad`"

HOY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

FUENTES = {
    "arl":   {"tabla": SAT_ARL,   "pk_sat": "id_sat_arl",           "satelite": "sat_arl"},
    "salud": {"tabla": SAT_SALUD, "pk_sat": "id_sat_beyond_health", "satelite": "sat_beyond_health"},
    "pyc":   {"tabla": SAT_PYC,   "pk_sat": "id_sat_pyc",           "satelite": "sat_pyc"},
}

print(f"Fecha de ejecución : {HOY}")
print(f"Tabla de reglas     : {REGLAS_TABLE}")
print(f"Tabla destino       : {FACT_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 2 — Mapeo de columnas por atributo y fuente
# MAGIC
# MAGIC Cada entrada puede ser:
# MAGIC - `("single", "COL")`            → columna única
# MAGIC - `("coalesce", ["COL1","COL2"])` → "COL1 o COL2" (se toma la primera no nula)
# MAGIC - `("concat", ["COL1","COL2"])`   → "COL1 + COL2" (concatenación con espacio)
# MAGIC - `None`                          → sin mapeo todavía (PENDIENTE — no se evalúa)
# MAGIC
# MAGIC Fuente: Excel `nombres_columnas.xlsx` (versión corregida) provisto por Calidad de Datos.

COLUMN_MAP = {
    "tipo_documento": {
        "arl":   ("single", ["tipo_documento"]),
        "salud": None,
        "pyc":   None,
    },
    "num_documento": {
        "arl":   ("single", ["numero_documento"]),
        "salud": None,
        "pyc":   None,
    },
    "primer_nombre": {
        "arl":   ("single", ["primer_nombre"]),
        "salud": None,
        "pyc":   ("single", ["TXT_NOMBRE"]),
    },
    "primer_apellido": {
        "arl":   ("single", ["primer_apellido"]),
        "salud": None,
        "pyc":   None,
    },
    "email": {
        "arl":   ("single", ["email"]),
        "salud": None,
        "pyc":   None,
    },
    "celular": {
        "arl":   ("single", ["celular"]),
        "salud": None,
        "pyc":   None,
    },
    "direccion_residencia": {
        "arl":   ("single", ["direccion_residencia"]),
        "salud": None,
        "pyc":   None,
    },
    "ciudad_residencia": {
        "arl":   ("single", ["ciudad_residencia"]),
        "salud": ("single", ["cit_ncode"]),
        "pyc":   None,
    },
    "fecha_nacimiento": {
        "arl":   ("single", ["fecha_nacimiento"]),
        "salud": None,
        "pyc":   None,
    },
    "genero": {
        "arl":   ("single", ["genero"]),
        "salud": None,
        "pyc":   None,
    },
    "eps_actual": {
        "arl":   None,
        "salud": ("single", ["INS_CLEGALCODE"]),
        "pyc":   None,
    },
    "fecha_expedicion_documento": {
        "arl":   None,
        "salud": None,
        "pyc":   None,
    },
    "atdp": {
        "arl":   ("single",   ["atdp"]),
        "salud": ("coalesce", ["INS_BEMAIL_SEND", "INS_BSMS_SEND"]),
        "pyc":   None,
    },
    "preferencia_contacto": {
        "arl":   None,
        "salud": ("single", ["PER_NCODE"]),
        "pyc":   None,
    },
    "atdp_comercial": {
        "arl":   None,
        "salud": ("single", ["EAC_NCODE"]),
        "pyc":   None,
    },
    "tipo_persona": {
        "arl":   ("single", ["tipo_persona"]),
        "salud": None,
        "pyc":   None,
    },
    "estado_civil": {
        "arl":   ("single", ["estado_civil"]),
        "salud": None,
        "pyc":   None,
    },
    "actividad_economica": {
        "arl":   None,
        "salud": None,
        "pyc":   None,
    },
    "departamento": {
        "arl":   None,
        "salud": ("single", ["dep_ncode"]),
        "pyc":   None,
    },
    "pais": {
        "arl":   None,
        "salud": None,
        "pyc":   None,
    },
    "rol": {
        "arl":   None,
        "salud": None,
        "pyc":   None,
    },
    "tipo_contrato": {
        "arl":   None,
        "salud": None,
        "pyc":   None,
    },
    "plan": {
        "arl":   None,
        "salud": ("single", ["PLA_NCODE"]),
        "pyc":   None,
    },
    "contrato": {
        "arl":   None,
        "salud": ("single", ["ACO_CONTRACTCODE"]),
        "pyc":   None,
    },
    "asesor": {
        "arl":   None,
        "salud": None,
        "pyc":   None,
    },
    "estado": {
        "arl":   None,
        "salud": None,
        "pyc":   None,
    },
    "fecha_inicio_vigencia": {
        "arl":   None,
        "salud": None,
        "pyc":   None,
    },
    "fecha_fin_vigencia": {
        "arl":   None,
        "salud": None,
        "pyc":   None,
    },
    "ramo": {
        "arl":   None,
        "salud": None,
        "pyc":   None,
    },
}

CANONICAL_ATTRS = list(COLUMN_MAP.keys())


def col_expr(spec):
    """Traduce un spec de COLUMN_MAP a una expresión SQL."""
    if spec is None:
        return None
    kind, cols = spec
    quoted = [f"`{c}`" for c in cols]
    if kind == "single":
        return quoted[0]
    if kind == "coalesce":
        return "COALESCE(" + ", ".join(quoted) + ")"
    if kind == "concat":
        return "CONCAT_WS(' ', " + ", ".join(quoted) + ")"
    raise ValueError(f"Tipo de spec desconocido: {kind}")


# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 3 — Vistas canónicas por fuente (alias físico → atributo canónico)

canonical_attrs_por_fuente = {}

for fuente, cfg in FUENTES.items():
    select_exprs = [f"`{cfg['pk_sat']}` AS __pk_sat__"]
    disponibles = []
    for attr in CANONICAL_ATTRS:
        spec = COLUMN_MAP[attr].get(fuente)
        expr = col_expr(spec)
        if expr is not None:
            select_exprs.append(f"{expr} AS {attr}")
            disponibles.append(attr)
    canonical_attrs_por_fuente[fuente] = disponibles

    sat_sql = f"""
        SELECT {', '.join(select_exprs)}
        FROM {cfg['tabla']}
    """
    sat_df = spark.sql(sat_sql)

    hub_df = spark.sql(f"""
        SELECT pk_hub_cliente, id_satelite
        FROM {HUB_TABLE}
        WHERE satelite = '{cfg["satelite"]}'
    """)

    canon_df = (
        sat_df.join(
            hub_df,
            sat_df["__pk_sat__"].cast("string") == hub_df["id_satelite"],
            "left",
        ).drop("id_satelite", "__pk_sat__")
    )

    canon_df.createOrReplaceTempView(f"_canon_{fuente}")
    print(f"[{fuente}] atributos disponibles ({len(disponibles)}/{len(CANONICAL_ATTRS)}): {disponibles}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 4 — Carga de reglas y detección de atributos requeridos

reglas_raw = spark.sql(f"""
    SELECT pk_regla_calidad, atributo, tipo_regla, regla
    FROM {REGLAS_TABLE}
    WHERE estado_regla = 1
""").collect()

WORD_RE_CACHE = {attr: re.compile(rf"\b{re.escape(attr)}\b") for attr in CANONICAL_ATTRS}


def atributos_requeridos(regla_sql: str):
    return [attr for attr in CANONICAL_ATTRS if WORD_RE_CACHE[attr].search(regla_sql)]


REGLAS = []
for r in reglas_raw:
    pk_norm = r["pk_regla_calidad"].replace(" ", "")
    req = atributos_requeridos(r["regla"])
    REGLAS.append({
        "pk_regla_calidad": pk_norm,
        "tipo_regla": r["tipo_regla"],
        "regla_sql": r["regla"],
        "atributos_requeridos": req,
        "atributo_principal": req[0] if req else None,
    })

print(f"Reglas cargadas: {len(REGLAS)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 5 — Motor de ejecución dinámica
# MAGIC
# MAGIC Para cada (regla, fuente):
# MAGIC - Si falta el mapeo de algún atributo requerido → `PENDIENTE_MAPEO` (no se ejecuta).
# MAGIC - Si la sintaxis/ejecución falla → `ERROR_EJECUCION` (se registra el log, no detiene el proceso).
# MAGIC - Si ejecuta correctamente → `EJECUTADA_OK` y se generan las filas para `fact_reporte_de_calidad`.

resultados = []          # filas a cargar en fact_reporte_de_calidad
log_pendientes = []       # reglas no evaluadas por falta de mapeo
log_errores = []          # reglas con error de ejecución
metricas = {"procesadas": 0, "exitosas": 0, "incumplidas_filas": 0, "errores": 0, "pendientes": 0}

for regla in REGLAS:
    for fuente, cfg in FUENTES.items():
        metricas["procesadas"] += 1

        disponibles = canonical_attrs_por_fuente[fuente]
        faltantes = [a for a in regla["atributos_requeridos"] if a not in disponibles]

        if not regla["atributos_requeridos"] or faltantes:
            metricas["pendientes"] += 1
            log_pendientes.append({
                "pk_regla_calidad": regla["pk_regla_calidad"],
                "fuente": fuente,
                "atributos_faltantes": faltantes or ["<no se detectó ningún atributo conocido en la regla>"],
            })
            continue

        try:
            if regla["tipo_regla"] == "Tabla":
                partition_cols = ", ".join(regla["atributos_requeridos"])
                sql = f"""
                    SELECT
                        pk_hub_cliente,
                        '{regla["atributo_principal"]}' AS atributo,
                        CAST({regla["atributo_principal"]} AS STRING) AS valor_atributo,
                        CASE WHEN COUNT(*) OVER (PARTITION BY {partition_cols}) > 1
                             THEN 1 ELSE 0 END AS incumple
                    FROM _canon_{fuente}
                """
            else:
                sql = f"""
                    SELECT
                        pk_hub_cliente,
                        '{regla["atributo_principal"]}' AS atributo,
                        CAST({regla["atributo_principal"]} AS STRING) AS valor_atributo,
                        CASE WHEN ({regla["regla_sql"]}) THEN 1 ELSE 0 END AS incumple
                    FROM _canon_{fuente}
                """

            df_eval = spark.sql(sql)
            df_eval = df_eval.withColumn(
                "fk_regla_calidad", F.lit(regla["pk_regla_calidad"])
            ).withColumn(
                "flag_remediado", (F.col("incumple") == 0).cast("int")
            ).drop("incumple")

            metricas["exitosas"] += 1
            metricas["incumplidas_filas"] += df_eval.filter(F.col("flag_remediado") == 0).count()

            resultados.append(df_eval)

        except (AnalysisException, ParseException, Exception) as e:
            metricas["errores"] += 1
            log_errores.append({
                "pk_regla_calidad": regla["pk_regla_calidad"],
                "fuente": fuente,
                "tabla_fuente": cfg["tabla"],
                "atributo": regla["atributo_principal"],
                "fecha_hora": datetime.now(timezone.utc).isoformat(),
                "error": str(e),
                "traceback": traceback.format_exc(),
            })
            print(f"[ERROR_EJECUCION] regla={regla['pk_regla_calidad']} fuente={fuente} → {e}")
            continue

print("Evaluación finalizada.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 6 — Consolidación y MERGE en `fact_reporte_de_calidad`

if resultados:
    df_fact = resultados[0]
    for df in resultados[1:]:
        df_fact = df_fact.unionByName(df)

    df_fact = (
        df_fact
        .withColumnRenamed("pk_hub_cliente", "fk_hub_cliente")
        .withColumn("pivot", F.lit(1))
        .withColumn("flag_resultado", F.lit(1))
        .filter(F.col("fk_hub_cliente").isNotNull())
    )

    df_fact.createOrReplaceTempView("_fact_staging")

    spark.sql(f"""
        MERGE INTO {FACT_TABLE} AS tgt
        USING _fact_staging AS src
          ON  tgt.fk_hub_cliente   = src.fk_hub_cliente
          AND tgt.fk_regla_calidad = src.fk_regla_calidad
          AND tgt.atributo         = src.atributo
        WHEN MATCHED AND tgt.flag_remediado <> src.flag_remediado THEN
          UPDATE SET
            tgt.valor_atributo     = src.valor_atributo,
            tgt.flag_remediado     = src.flag_remediado,
            tgt.fecha_fin_deteccion = CASE WHEN src.flag_remediado = 1 THEN current_date() ELSE NULL END,
            tgt.fecha_actualizacion = current_date(),
            tgt.pivot              = src.pivot,
            tgt.flag_resultado     = src.flag_resultado
        WHEN NOT MATCHED THEN
          INSERT (
            fk_hub_cliente, fk_regla_calidad, atributo, valor_atributo,
            fecha_ini_deteccion, fecha_fin_deteccion, fecha_actualizacion,
            flag_remediado, pivot, flag_resultado, fecha_creacion
          )
          VALUES (
            src.fk_hub_cliente, src.fk_regla_calidad, src.atributo, src.valor_atributo,
            current_date(), NULL, current_date(),
            src.flag_remediado, src.pivot, src.flag_resultado, current_date()
          )
    """)

    spark.catalog.dropTempView("_fact_staging")
    print("MERGE completado en fact_reporte_de_calidad.")
else:
    print("No hay resultados para cargar (revisar reglas pendientes/errores abajo).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 7 — Métricas de ejecución y reporte de incidencias

print("=" * 60)
print("  MÉTRICAS DE EJECUCIÓN")
print("=" * 60)
print(f"  Total combinaciones regla×fuente procesadas : {metricas['procesadas']}")
print(f"  Ejecutadas exitosamente                      : {metricas['exitosas']}")
print(f"  Pendientes por mapeo de columna               : {metricas['pendientes']}")
print(f"  Con error de ejecución                        : {metricas['errores']}")
print(f"  Filas que incumplen la regla evaluada         : {metricas['incumplidas_filas']}")

if log_pendientes:
    print("\n  Reglas PENDIENTES_MAPEO (requieren columna aún no definida):")
    df_pend = spark.createDataFrame(log_pendientes)
    display(df_pend)

if log_errores:
    print("\n  Reglas con ERROR_EJECUCION (alerta operativa — revisar la columna 'regla'):")
    df_err = spark.createDataFrame(
        [{"pk_regla_calidad": e["pk_regla_calidad"], "fuente": e["fuente"],
          "tabla_fuente": e["tabla_fuente"], "atributo": e["atributo"],
          "fecha_hora": e["fecha_hora"], "error": e["error"]} for e in log_errores]
    )
    display(df_err)
