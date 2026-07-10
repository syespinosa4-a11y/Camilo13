# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
CONFIG = {
    "catalogo_fuente": "axa_col_slv_dv",   # <-- verificar con: spark.sql("SHOW CATALOGS").show()
    "esquema_fuente":  "core_bh",
    "catalogo_destino": "uc_axa_cli",
    "esquema_destino":  "silver",
    "tabla_destino":    "sat_beyond_health",
}

TABLAS_BH = [
    "bh_sa_person",
    "bh_sa_address",
    "bh_sa_institution",
    "bh_sa_city",
    "bh_sa_affiliation_contract",
    "bh_sa_member",
]

# COMMAND ----------
# Lectura de tablas fuente

def fuente(tabla):
    cat = CONFIG["catalogo_fuente"]
    esq = CONFIG["esquema_fuente"]
    return spark.table(f"`{cat}`.`{esq}`.`{tabla}`")

tbls = {t: fuente(t) for t in TABLAS_BH}

person               = tbls["bh_sa_person"]
address              = tbls["bh_sa_address"]
institution          = tbls["bh_sa_institution"]
city                 = tbls["bh_sa_city"]
affiliation_contract = tbls["bh_sa_affiliation_contract"]
member               = tbls["bh_sa_member"]

# COMMAND ----------
# Desambiguacion de llaves que aparecen en mas de una tabla.
# per_ncode existe en: member, affiliation_contract, address, person.
# ins_ncode existe en: affiliation_contract, institution.
# cit_ncode existe en: address, city.
# Se renombran en las tablas donde son clave foranea (no en la tabla duena).

affiliation_contract_wide = affiliation_contract.withColumnRenamed(
    "per_ncode", "aco_per_ncode"
).withColumnRenamed(
    "ins_ncode", "aco_ins_ncode"
)

address_wide = address.withColumnRenamed(
    "per_ncode", "add_per_ncode"
).withColumnRenamed(
    "cit_ncode", "add_cit_ncode"
)

# COMMAND ----------
# Pre-agregado: residencial
# bh_sa_address (lty_ncode = 1) + bh_sa_city agrupado por per_ncode.
# Produce una fila por persona con su direccion y codigo de ciudad.

df_residencial = (
    address_wide.filter(F.col("lty_ncode") == 1)
    .join(
        city.select("cit_ncode", "cit_clegalcode"),
        address_wide["add_cit_ncode"] == city["cit_ncode"],
        how="left",
    )
    .groupBy("add_per_ncode")
    .agg(
        F.max("add_caddress").alias("dir_res"),
        F.max("cit_clegalcode").alias("ciu_res_codigo"),
    )
    .withColumnRenamed("add_per_ncode", "res_per_ncode")
)

# Lookup nombre de ciudad (une por cit_clegalcode, no por cit_ncode)
df_ciudad_nombre = city.select(
    F.col("cit_clegalcode").alias("ciu_res_codigo"),
    F.col("cit_cname").alias("ciudad_residencia"),
)

# COMMAND ----------
# RAMA TITULAR  (persona viene de affiliation_contract.aco_per_ncode)
# En este fragmento, se crea un DataFrame df_titular que contiene la informacion de los titulares.
# Se une la tabla member con la tabla affiliation_contract para obtener el codigo de la afiliacion
# y el codigo de la persona titular. Luego, se une con la tabla person para obtener la informacion
# de la persona titular. Tambien se une con la tabla institution para obtener la informacion de la
# institucion. Finalmente, se une con los DataFrames df_residencial y df_ciudad_nombre para obtener
# la direccion residencial y el nombre de la ciudad.

df_titular = (
    member
    # member <-> affiliation_contract por aco_ncode
    .join(
        affiliation_contract_wide,
        on="aco_ncode",
        how="inner",
    )
    # affiliation_contract <-> person: titular viene del contrato (aco_per_ncode)
    .join(
        person,
        affiliation_contract_wide["aco_per_ncode"] == person["per_ncode"],
        how="left",
    )
    # affiliation_contract <-> institution: institucion del contrato (aco_ins_ncode)
    .join(
        institution,
        affiliation_contract_wide["aco_ins_ncode"] == institution["ins_ncode"],
        how="left",
    )
    # persona <-> residencial por per_ncode del titular (aco_per_ncode)
    .join(
        df_residencial,
        affiliation_contract_wide["aco_per_ncode"] == df_residencial["res_per_ncode"],
        how="left",
    )
    # residencial <-> nombre ciudad
    .join(df_ciudad_nombre, on="ciu_res_codigo", how="left")
    .withColumn("rol", F.lit("TITULAR"))
    .drop("res_per_ncode")   # clave auxiliar del pre-agregado, no va al satelite
)

# COMMAND ----------
# RAMA BENEFICIARIO  (persona viene de member.per_ncode)
# En este fragmento, se crea un DataFrame df_beneficiario que contiene la informacion de los beneficiarios.
# Se une la tabla member con la tabla affiliation_contract para obtener el codigo de la afiliacion
# y el codigo de la institucion. Luego, se une con la tabla person para obtener la informacion de la
# persona beneficiaria. Tambien se une con la tabla institution para obtener la informacion de la
# institucion. Finalmente, se une con los DataFrames df_residencial y df_ciudad_nombre para obtener
# la direccion residencial y el nombre de la ciudad.

df_beneficiario = (
    member
    # member <-> affiliation_contract por aco_ncode
    .join(
        affiliation_contract_wide,
        on="aco_ncode",
        how="inner",
    )
    # member <-> person: beneficiario viene del member (member.per_ncode)
    .join(
        person,
        member["per_ncode"] == person["per_ncode"],
        how="left",
    )
    # affiliation_contract <-> institution: misma institucion del contrato
    .join(
        institution,
        affiliation_contract_wide["aco_ins_ncode"] == institution["ins_ncode"],
        how="left",
    )
    # member.per_ncode <-> residencial
    .join(
        df_residencial,
        member["per_ncode"] == df_residencial["res_per_ncode"],
        how="left",
    )
    # residencial <-> nombre ciudad
    .join(df_ciudad_nombre, on="ciu_res_codigo", how="left")
    .withColumn("rol", F.lit("BENEFICIARIO"))
    .drop("res_per_ncode")
)

# COMMAND ----------
# Union horizontal titular + beneficiario.
# Ambas ramas tienen exactamente las mismas columnas (todas las de las 6 tablas + rol).

df_resultado = df_titular.unionByName(df_beneficiario)

print("columnas del satelite:", len(df_resultado.columns))
print(df_resultado.columns)
print("filas titular:     ", df_titular.count())
print("filas beneficiario:", df_beneficiario.count())
print("filas total:       ", df_resultado.count())
df_resultado.show(5, truncate=False)
