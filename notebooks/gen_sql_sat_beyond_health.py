# Databricks notebook source
# gen_sql_sat_beyond_health.py
# Inspecciona los esquemas reales de las tablas core_bh, detecta las llaves
# de union (*_NCODE) de la misma forma que el script PySpark, y genera el
# SELECT completo listo para ejecutar o guardar en DIM_SATELITE_DEF.

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

CATALOGO = "axa_col_dv"
ESQUEMA  = "core_bh"

def columnas(tabla: str) -> list[str]:
    """Devuelve la lista de columnas (en minusculas) de una tabla."""
    return [f.name.lower() for f in spark.table(f"`{CATALOGO}`.`{ESQUEMA}`.`{tabla}`").schema.fields]

def ncode_cols(tabla: str) -> list[str]:
    """Columnas *_ncode de una tabla (case-insensitive)."""
    return [c for c in columnas(tabla) if c.endswith("ncode")]

def llave_entidad_sql(tabla: str, alias: str) -> str | None:
    """
    Genera COALESCE(alias.per_ncode, alias.ins_ncode) si la tabla tiene
    alguna de esas columnas; solo la que exista si solo tiene una;
    None si no tiene ninguna.
    """
    cols = set(columnas(tabla))
    tiene_per = "per_ncode" in cols
    tiene_ins = "ins_ncode" in cols
    if tiene_per and tiene_ins:
        return f"COALESCE(CAST({alias}.per_ncode AS STRING), CAST({alias}.ins_ncode AS STRING))"
    if tiene_per:
        return f"CAST({alias}.per_ncode AS STRING)"
    if tiene_ins:
        return f"CAST({alias}.ins_ncode AS STRING)"
    return None

def llave_comun_sql(tabla_izq: str, alias_izq: str, tabla_der: str, alias_der: str) -> str | None:
    """
    Detecta el *_ncode comun entre dos tablas (excluyendo per_ncode/ins_ncode
    que son llaves de entidad, no de relacion entre tablas).
    Devuelve 'alias_izq.col = alias_der.col' o None si no hay llave comun.
    """
    excluir = {"per_ncode", "ins_ncode"}
    ncodes_izq = {c for c in ncode_cols(tabla_izq) if c not in excluir}
    ncodes_der = {c for c in ncode_cols(tabla_der) if c not in excluir}
    comunes = ncodes_izq & ncodes_der
    if not comunes:
        return None
    col = sorted(comunes)[0]          # toma la primera en orden alfabetico
    return f"{alias_izq}.{col} = {alias_der}.{col}"

# COMMAND ----------
# Detectar llaves

llave_base    = "COALESCE(CAST(base.per_ncode AS STRING), CAST(base.ins_ncode AS STRING))"
llave_addr    = llave_entidad_sql("bh_sa_address",              "addr")
llave_aco     = llave_entidad_sql("bh_sa_affiliation_contract", "aco")
llave_aco_mem = llave_comun_sql("bh_sa_affiliation_contract", "aco", "bh_sa_member",  "mem")
llave_addr_ciu= llave_comun_sql("bh_sa_address",              "addr", "bh_sa_city",   "ciu")

print("=== Llaves detectadas ===")
print(f"base (entidad)  : {llave_base}")
print(f"addr (entidad)  : {llave_addr}")
print(f"aco  (entidad)  : {llave_aco}")
print(f"aco  → mem      : {llave_aco_mem}")
print(f"addr → ciu      : {llave_addr_ciu}")

# COMMAND ----------
# Construir el SELECT con las llaves reales

on_addr = f"{llave_base}\n    = {llave_addr}" if llave_addr else "/* ERROR: bh_sa_address no tiene llave de entidad */"
on_aco  = f"{llave_base}\n    = {llave_aco}"  if llave_aco  else "/* ERROR: bh_sa_affiliation_contract no tiene llave de entidad */"
on_mem  = llave_aco_mem  or "/* ERROR: sin llave comun *_NCODE entre aco y mem */"
on_ciu  = llave_addr_ciu or "/* ERROR: sin llave comun *_NCODE entre addr y ciu */"

sql = f"""SELECT
  {llave_base} AS id_entidad_hub,
  base.tipo_entidad,
  base.per_ncode,
  base.ins_ncode,
  addr.cit_ncode,
  aco.aco_ncode,
  monotonically_increasing_id() AS id_sat_beyond_health,
  current_timestamp()           AS dv_load_date,
  current_date()                AS fecha_creacion

FROM (
  SELECT per_ncode, NULL AS ins_ncode, 'PERSONA'     AS tipo_entidad
  FROM `{CATALOGO}`.`{ESQUEMA}`.`bh_sa_person`
  UNION ALL
  SELECT NULL AS per_ncode, ins_ncode, 'INSTITUCION' AS tipo_entidad
  FROM `{CATALOGO}`.`{ESQUEMA}`.`bh_sa_institution`
) base

LEFT JOIN `{CATALOGO}`.`{ESQUEMA}`.`bh_sa_address` addr
  ON  {on_addr}

LEFT JOIN `{CATALOGO}`.`{ESQUEMA}`.`bh_sa_affiliation_contract` aco
  ON  {on_aco}

LEFT JOIN `{CATALOGO}`.`{ESQUEMA}`.`bh_sa_member` mem
  ON {on_mem}

LEFT JOIN `{CATALOGO}`.`{ESQUEMA}`.`bh_sa_city` ciu
  ON {on_ciu}
"""

print("\n=== SQL generado ===\n")
print(sql)

# COMMAND ----------
# Ejecutar el SELECT para validar (muestra las primeras 20 filas)

df = spark.sql(sql)
df.show(20, truncate=False)
print(f"\nTotal columnas: {len(df.columns)}")
print(f"Columnas: {df.columns}")
