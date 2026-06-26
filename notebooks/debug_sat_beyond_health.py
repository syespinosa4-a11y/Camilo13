# Databricks notebook source
# Debug bloque por bloque — sat_beyond_health / _bh_titulares
# No reemplaza al notebook de produccion (satelites_parametrizados.py),
# es solo para encontrar en cual join se rompe el cruce.
# Premisa: sigue parametrizado, todo sale de DIM_PARAMETROS via fuente()/get_param().

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
PARAMS_TABLE = "`uc_axa_cli`.`silver`.`dim_parametros`"
GRUPO = "sat_beyond_health"


def cargar_parametros():
    return spark.table(PARAMS_TABLE).cache()


def get_param(df_params, grupo, nombre, default=None):
    row = (
        df_params
        .filter((F.col("grupo_parametros") == grupo) & (F.col("nombre") == nombre))
        .select("valor")
        .first()
    )
    return row[0] if row else default


def fuente(df_params, grupo, tabla_fisica):
    catalogo = get_param(df_params, grupo, "catalogo_fuente")
    esquema = get_param(df_params, grupo, "esquema_fuente")
    return spark.table(f"`{catalogo}`.`{esquema}`.`{tabla_fisica}`")


df_params = cargar_parametros()
print("catalogo_fuente:", get_param(df_params, GRUPO, "catalogo_fuente"))
print("esquema_fuente:", get_param(df_params, GRUPO, "esquema_fuente"))

# COMMAND ----------
# PASO 1 — leer cada tabla fuente sola y contar filas.
# Si alguna sale en 0, el problema es el nombre de tabla / catalogo / esquema,
# no el join.

m = fuente(df_params, GRUPO, "bh_sa_member")
a = fuente(df_params, GRUPO, "bh_sa_affiliation_contract")
pln = fuente(df_params, GRUPO, "bh_sa_plan")
prod = fuente(df_params, GRUPO, "bh_sa_product")
p = fuente(df_params, GRUPO, "bh_sa_person")
t = fuente(df_params, GRUPO, "bh_sa_identification_type")
i = fuente(df_params, GRUPO, "bh_sa_institution")

print("m  (bh_sa_member):              ", m.count())
print("a  (bh_sa_affiliation_contract): ", a.count())
print("pln(bh_sa_plan):                ", pln.count())
print("prod(bh_sa_product):            ", prod.count())
print("p  (bh_sa_person):              ", p.count())
print("t  (bh_sa_identification_type): ", t.count())
print("i  (bh_sa_institution):         ", i.count())

# COMMAND ----------
# PASO 2 — join 1: m + a  (inner, ACO_NCODE)
df1 = m.join(a, a["ACO_NCODE"] == m["ACO_NCODE"], "inner")
print("m x a (ACO_NCODE):", df1.count())
df1.select("ACO_NCODE").show(5)

# COMMAND ----------
# PASO 3 — join 2: + pln  (left, PLA_NCODE)
df2 = df1.join(pln, pln["PLA_NCODE"] == a["PLA_NCODE"], "left")
print("+ pln (PLA_NCODE):", df2.count())

# COMMAND ----------
# PASO 4 — join 3: + prod (left, PRO_NCODE)
df3 = df2.join(prod, prod["PRO_NCODE"] == pln["PRO_NCODE"], "left")
print("+ prod (PRO_NCODE):", df3.count())

# COMMAND ----------
# PASO 5 — join 4: + p (left, PER_NCODE)
df4 = df3.join(p, p["PER_NCODE"] == a["PER_NCODE"], "left")
print("+ p (PER_NCODE):", df4.count())
print("p.PER_NCODE nulos despues del join:", df4.filter(p["PER_NCODE"].isNull()).count())

# COMMAND ----------
# PASO 6 — join 5: + t (left, ITY_NCODE = p.TID_NCODE)
df5 = df4.join(t, t["ITY_NCODE"] == p["TID_NCODE"], "left")
print("+ t (ITY_NCODE = TID_NCODE):", df5.count())

# COMMAND ----------
# PASO 7 — join 6: + i (left, INS_NCODE = a.INS_NCODE)
df6 = df5.join(i, i["INS_NCODE"] == a["INS_NCODE"], "left")
print("+ i (INS_NCODE):", df6.count())
print("i.INS_NCODE no nulo (filas institucion):", df6.filter(i["INS_NCODE"].isNotNull()).count())

# COMMAND ----------
# PASO 8 — aqui se compara contra el conteo esperado.
# Si en algun paso el conteo se va a 0 o crece de forma anormal (cardinalidad
# explotando por duplicados en la tabla de la derecha), ese es el join que
# rompe. Anota en cual paso pasa y seguimos con el resto de los joins de
# _bh_titulares (t0/institucion, h/member_status, re_/residencial,
# cit_res/ciudad, ad_nov/novedades) de la misma forma.
