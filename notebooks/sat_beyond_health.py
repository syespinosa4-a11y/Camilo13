# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# CONFIG: unico lugar a editar cuando cambien catalogos, esquemas o llaves

CONFIG = {
    "catalogo_fuente":  "axa_col_slv_dv",   # <-- verificar con: spark.sql("SHOW CATALOGS").show()
    "esquema_fuente":   "core_bh",
    "catalogo_destino": "uc_axa_cli",
    "esquema_destino":  "silver",
    "tabla_destino":    "sat_beyond_health",
}

# Tablas fuente
TABLAS_BH = [
    "bh_sa_person",
    "bh_sa_address",
    "bh_sa_institution",
    "bh_sa_city",
    "bh_sa_affiliation_contract",
    "bh_sa_member",
]

# Columnas que existen en mas de una tabla: se renombran en la tabla donde son
# clave foranea para que PySpark no genere ambiguedad al hacer el join wide.
# Formato: { nombre_tabla: { col_original: col_nueva } }
RENOMBRES = {
    "bh_sa_affiliation_contract": {
        "per_ncode": "aco_per_ncode",
        "ins_ncode": "aco_ins_ncode",
    },
    "bh_sa_address": {
        "per_ncode": "add_per_ncode",
        "cit_ncode": "add_cit_ncode",
    },
}

# Llaves de join entre tablas
LLAVES = {
    # join comun a ambas ramas
    "member_contrato":          ("aco_ncode",      "aco_ncode"),
    "residencial_ciudad":       ("ciu_res_codigo",  "ciu_res_codigo"),
    # rama TITULAR: persona e institucion vienen del contrato
    "titular_persona":          ("aco_per_ncode",  "per_ncode"),
    "titular_institucion":      ("aco_ins_ncode",  "ins_ncode"),
    "titular_residencial":      ("aco_per_ncode",  "res_per_ncode"),
    # rama BENEFICIARIO: persona viene del member
    "beneficiario_persona":     ("per_ncode",      "per_ncode"),
    "beneficiario_institucion": ("aco_ins_ncode",  "ins_ncode"),
    "beneficiario_residencial": ("per_ncode",      "res_per_ncode"),
}

# Pre-agregado residencial: parametros de filtro y columnas de salida
RESIDENCIAL = {
    "tabla":             "bh_sa_address",
    "filtro_col":        "lty_ncode",        # columna que distingue tipo de direccion
    "filtro_val":        1,                  # valor = residencial
    "llave_persona":     "add_per_ncode",    # llave tras el renombre de ambiguedad
    "col_direccion":     "add_caddress",     # columna de direccion en bh_sa_address
    "join_ciudad_llave": ("add_cit_ncode",   # llave de address hacia city
                          "cit_ncode"),
    "col_ciudad_codigo": "cit_clegalcode",   # codigo legal de ciudad (bh_sa_city)
    "col_ciudad_nombre": "cit_cname",        # nombre de ciudad (bh_sa_city)
    "alias_dir":         "dir_res",
    "alias_ciu_codigo":  "ciu_res_codigo",
    "alias_ciu_nombre":  "ciudad_residencia",
    "alias_llave":       "res_per_ncode",    # nombre de la llave en el df pre-agregado
}

# COMMAND ----------
# Helpers

def fuente(tabla: str):
    cat = CONFIG["catalogo_fuente"]
    esq = CONFIG["esquema_fuente"]
    return spark.table(f"`{cat}`.`{esq}`.`{tabla}`")


def aplicar_renombres(df, nombre_tabla: str):
    """Renombra las columnas conflictivas segun RENOMBRES."""
    for col_orig, col_nueva in RENOMBRES.get(nombre_tabla, {}).items():
        df = df.withColumnRenamed(col_orig, col_nueva)
    return df


def llave(nombre: str):
    """Devuelve la condicion de join a partir de LLAVES."""
    izq, der = LLAVES[nombre]
    return izq, der

# COMMAND ----------
# Lectura y desambiguacion de tablas fuente

tbls = {}
for t in TABLAS_BH:
    tbls[t] = aplicar_renombres(fuente(t), t)

member               = tbls["bh_sa_member"]
affiliation_contract = tbls["bh_sa_affiliation_contract"]
person               = tbls["bh_sa_person"]
institution          = tbls["bh_sa_institution"]
address              = tbls["bh_sa_address"]
city                 = tbls["bh_sa_city"]

# COMMAND ----------
# Pre-agregado: residencial
# bh_sa_address (lty_ncode = 1) + bh_sa_city agrupado por per_ncode.
# Produce una fila por persona con su direccion y codigo de ciudad.

R = RESIDENCIAL
_add_llave_izq, _add_llave_der = R["join_ciudad_llave"]

df_residencial = (
    address.filter(F.col(R["filtro_col"]) == R["filtro_val"])
    .join(
        city.select(_add_llave_der, R["col_ciudad_codigo"]),
        address[_add_llave_izq] == city[_add_llave_der],
        how="left",
    )
    .groupBy(R["llave_persona"])
    .agg(
        F.max(R["col_direccion"]).alias(R["alias_dir"]),
        F.max(R["col_ciudad_codigo"]).alias(R["alias_ciu_codigo"]),
    )
    .withColumnRenamed(R["llave_persona"], R["alias_llave"])
)

df_ciudad_nombre = city.select(
    F.col(R["col_ciudad_codigo"]).alias(R["alias_ciu_codigo"]),
    F.col(R["col_ciudad_nombre"]).alias(R["alias_ciu_nombre"]),
)

# COMMAND ----------
# RAMA TITULAR
# En este fragmento, se crea un DataFrame df_titular que contiene la informacion de los titulares.
# Se une la tabla member con la tabla affiliation_contract para obtener el codigo de la afiliacion
# y el codigo de la persona titular. Luego, se une con la tabla person para obtener la informacion
# de la persona titular. Tambien se une con la tabla institution para obtener la informacion de la
# institucion. Finalmente, se une con los DataFrames df_residencial y df_ciudad_nombre para obtener
# la direccion residencial y el nombre de la ciudad.

_l = llave  # alias corto

df_titular = (
    member
    .join(affiliation_contract,
          on=_l("member_contrato")[0],
          how="inner")
    .join(person,
          affiliation_contract[_l("titular_persona")[0]] == person[_l("titular_persona")[1]],
          how="left")
    .join(institution,
          affiliation_contract[_l("titular_institucion")[0]] == institution[_l("titular_institucion")[1]],
          how="left")
    .join(df_residencial,
          affiliation_contract[_l("titular_residencial")[0]] == df_residencial[_l("titular_residencial")[1]],
          how="left")
    .join(df_ciudad_nombre,
          on=_l("residencial_ciudad")[0],
          how="left")
    .withColumn("rol", F.lit("TITULAR"))
    .drop(R["alias_llave"])
)

# COMMAND ----------
# RAMA BENEFICIARIO
# En este fragmento, se crea un DataFrame df_beneficiario que contiene la informacion de los beneficiarios.
# Se une la tabla member con la tabla affiliation_contract para obtener el codigo de la afiliacion
# y el codigo de la institucion. Luego, se une con la tabla person para obtener la informacion de la
# persona beneficiaria. Tambien se une con la tabla institution para obtener la informacion de la
# institucion. Finalmente, se une con los DataFrames df_residencial y df_ciudad_nombre para obtener
# la direccion residencial y el nombre de la ciudad.

df_beneficiario = (
    member
    .join(affiliation_contract,
          on=_l("member_contrato")[0],
          how="inner")
    .join(person,
          member[_l("beneficiario_persona")[0]] == person[_l("beneficiario_persona")[1]],
          how="left")
    .join(institution,
          affiliation_contract[_l("beneficiario_institucion")[0]] == institution[_l("beneficiario_institucion")[1]],
          how="left")
    .join(df_residencial,
          member[_l("beneficiario_residencial")[0]] == df_residencial[_l("beneficiario_residencial")[1]],
          how="left")
    .join(df_ciudad_nombre,
          on=_l("residencial_ciudad")[0],
          how="left")
    .withColumn("rol", F.lit("BENEFICIARIO"))
    .drop(R["alias_llave"])
)

# COMMAND ----------
# Union horizontal titular + beneficiario

df_resultado = df_titular.unionByName(df_beneficiario)

print("columnas del satelite:", len(df_resultado.columns))
print(df_resultado.columns)
print("filas titular:     ", df_titular.count())
print("filas beneficiario:", df_beneficiario.count())
print("filas total:       ", df_resultado.count())
df_resultado.show(5, truncate=False)
