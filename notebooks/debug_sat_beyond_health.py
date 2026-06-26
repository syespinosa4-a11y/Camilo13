# Databricks notebook source
# Debug funcion por funcion / paso por paso — sat_beyond_health
# No reemplaza al notebook de produccion (satelites_parametrizados.py).
# Premisa: sigue parametrizado, todo sale de DIM_PARAMETROS via fuente()/get_param().
# Cada celda hace UNA sola cosa — ejecuta celda por celda y revisa el resultado
# antes de pasar a la siguiente.

# COMMAND ----------
# PASO 0 — imports

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
print("imports OK")

# COMMAND ----------
# PASO 1 — variables de parametrizacion (catalogo/esquema de DIM_PARAMETROS y grupo)

PARAMS_TABLE = "`uc_axa_cli`.`silver`.`dim_parametros`"
GRUPO = "sat_beyond_health"
print("PARAMS_TABLE:", PARAMS_TABLE)
print("GRUPO:", GRUPO)

# COMMAND ----------
# PASO 2 — funcion cargar_parametros (la definimos, todavia no la ejecutamos)

def cargar_parametros():
    return spark.table(PARAMS_TABLE).cache()

print("funcion cargar_parametros definida")

# COMMAND ----------
# PASO 3 — ejecutar cargar_parametros y ver que SI trae filas

df_params = cargar_parametros()
print("filas en DIM_PARAMETROS:", df_params.count())
df_params.show(5, truncate=False)

# COMMAND ----------
# PASO 4 — ver TODAS las filas de parametros del grupo sat_beyond_health
# (esto es clave: si esto sale vacio, fuente()/get_param() nunca van a
# funcionar porque no hay de donde leer catalogo_fuente/esquema_fuente)

df_params.filter(F.col("grupo_parametros") == GRUPO).show(50, truncate=False)

# COMMAND ----------
# PASO 5 — funcion get_param (la definimos)

def get_param(df_params, grupo, nombre, default=None):
    row = (
        df_params
        .filter((F.col("grupo_parametros") == grupo) & (F.col("nombre") == nombre))
        .select("valor")
        .first()
    )
    return row[0] if row else default

print("funcion get_param definida")

# COMMAND ----------
# PASO 6 — leer catalogo_fuente y esquema_fuente con get_param
# Si alguno sale en None, ESE es el problema raiz: faltan esas filas en
# DIM_PARAMETROS para el grupo sat_beyond_health.

catalogo_fuente = get_param(df_params, GRUPO, "catalogo_fuente")
esquema_fuente = get_param(df_params, GRUPO, "esquema_fuente")
print("catalogo_fuente:", catalogo_fuente)
print("esquema_fuente:", esquema_fuente)

# COMMAND ----------
# PASO 7 — leer tambien catalogo / esquema / tabla_destino / id_columna_pk
# (destino del satelite, para confirmar que tambien estan parametrizados)

print("catalogo (destino):", get_param(df_params, GRUPO, "catalogo"))
print("esquema (destino):", get_param(df_params, GRUPO, "esquema"))
print("tabla_destino:", get_param(df_params, GRUPO, "tabla_destino"))
print("id_columna_pk:", get_param(df_params, GRUPO, "id_columna_pk"))
print("estado:", get_param(df_params, GRUPO, "estado"))
print("constructor:", get_param(df_params, GRUPO, "constructor"))

# COMMAND ----------
# PASO 8 — funcion fuente (la definimos)

def fuente(df_params, grupo, tabla_fisica):
    catalogo = get_param(df_params, grupo, "catalogo_fuente")
    esquema = get_param(df_params, grupo, "esquema_fuente")
    return spark.table(f"`{catalogo}`.`{esquema}`.`{tabla_fisica}`")

print("funcion fuente definida")

# COMMAND ----------
# PASO 9 — probar fuente() con UNA sola tabla: bh_sa_member
# Si esto falla (tabla no existe / catalogo o esquema vacio), el error
# aparece aqui mismo con el nombre completo que intento leer.

m = fuente(df_params, GRUPO, "bh_sa_member")
print("bh_sa_member -> filas:", m.count())
m.show(5)

# COMMAND ----------
# PASO 10 — probar fuente() con bh_sa_affiliation_contract

a = fuente(df_params, GRUPO, "bh_sa_affiliation_contract")
print("bh_sa_affiliation_contract -> filas:", a.count())
a.show(5)

# COMMAND ----------
# PASO 11 — ver si las columnas de cruce existen y que tipo de dato tienen
# (m.ACO_NCODE vs a.ACO_NCODE) — un mismatch de tipo (string vs int) hace
# que el join no traiga nada aunque los valores "parezcan" iguales.

m.select("ACO_NCODE").printSchema()
a.select("ACO_NCODE").printSchema()

# COMMAND ----------
# PASO 12 — comparar valores reales de ACO_NCODE de ambos lados (sin join,
# solo distinct) para confirmar que de verdad coinciden valores

m.select("ACO_NCODE").distinct().show(10)
a.select("ACO_NCODE").distinct().show(10)

# COMMAND ----------
# PASO 13 — recien aqui, el primer join: m + a (inner, ACO_NCODE)

df1 = m.join(a, a["ACO_NCODE"] == m["ACO_NCODE"], "inner")
print("m x a (ACO_NCODE) -> filas:", df1.count())

# Si PASO 13 da 0 filas pero PASO 9/10 si traian filas, el problema esta en
# la condicion del join (tipo de dato distinto, o los valores no coinciden
# realmente) y no en la parametrizacion ni en la lectura de las tablas.
