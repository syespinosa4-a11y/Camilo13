# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# Configuracion

CONFIG = {
    "catalogo_fuente":  "axa_col_slv_dv",   # <-- verificar con: spark.sql("SHOW CATALOGS").show()
    "esquema_fuente":   "core_bh",
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

def fuente(tabla: str):
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
# Pre-agregado: residencial
# bh_sa_address (lty_ncode = 1) + bh_sa_city agrupado por per_ncode.
# Produce una fila por persona con su direccion y codigo de ciudad.

df_residencial = (
    address.filter(F.col("lty_ncode") == 1)
    .join(
        city.select("cit_ncode", "cit_clegalcode"),
        on="cit_ncode",
        how="left",
    )
    .groupBy("per_ncode")
    .agg(
        F.max("add_caddress").alias("dir_res"),
        F.max("cit_clegalcode").alias("ciu_res_codigo"),
    )
)

# Lookup nombre de ciudad (une por cit_clegalcode, no por cit_ncode)
df_ciudad_nombre = city.select(
    F.col("cit_clegalcode").alias("ciu_res_codigo"),
    F.col("cit_cname").alias("ciudad_residencia"),
)

# COMMAND ----------
# RAMA TITULAR
# Persona titular: bh_sa_affiliation_contract.per_ncode
# Institucion:     bh_sa_affiliation_contract.ins_ncode

df_titular = (
    member
    .join(
        affiliation_contract.select(
            "aco_ncode",
            F.col("per_ncode").alias("per_ncode_titular"),
            F.col("ins_ncode").alias("ins_ncode_titular"),
        ),
        on="aco_ncode",
        how="inner",
    )
    .join(
        person.select(
            F.col("per_ncode").alias("per_ncode_titular"),
            "per_cidentificationnumber",
            "per_cfirstname",
            "per_cmiddlename",
            "per_clastname",
            "per_cemail",
            "per_cmobilephone",
            "per_dbirthdate",
            "per_cgender",
            "tid_ncode",
        ),
        on="per_ncode_titular",
        how="left",
    )
    .join(
        institution.select(
            F.col("ins_ncode").alias("ins_ncode_titular"),
            "ins_cidentificationnumber",
            "ins_cname",
            "ins_cemail",
            "ins_cmobil_phone",
        ),
        on="ins_ncode_titular",
        how="left",
    )
    .join(
        df_residencial.select(
            F.col("per_ncode").alias("per_ncode_titular"),
            "dir_res",
            "ciu_res_codigo",
        ),
        on="per_ncode_titular",
        how="left",
    )
    .join(df_ciudad_nombre, on="ciu_res_codigo", how="left")
    .select(
        F.col("per_ncode_titular").alias("per_ncode"),
        "per_cidentificationnumber",
        "per_cfirstname",
        "per_cmiddlename",
        "per_clastname",
        "per_cemail",
        "per_cmobilephone",
        "per_dbirthdate",
        "per_cgender",
        "ins_cidentificationnumber",
        "ins_cname",
        "ins_cemail",
        "ins_cmobil_phone",
        "dir_res",
        "ciudad_residencia",
        "mem_ncode",
        "aco_ncode",
        F.lit("TITULAR").alias("rol"),
    )
)

# COMMAND ----------
# RAMA BENEFICIARIO
# Persona beneficiaria: bh_sa_member.per_ncode (no del contrato)
# Institucion:          bh_sa_affiliation_contract.ins_ncode

df_beneficiario = (
    member
    .join(
        affiliation_contract.select(
            "aco_ncode",
            F.col("ins_ncode").alias("ins_ncode_contrato"),
        ),
        on="aco_ncode",
        how="inner",
    )
    .join(
        person.select(
            "per_ncode",
            "per_cidentificationnumber",
            "per_cfirstname",
            "per_cmiddlename",
            "per_clastname",
            "per_cemail",
            "per_cmobilephone",
            "per_dbirthdate",
            "per_cgender",
            "tid_ncode",
        ),
        on="per_ncode",
        how="left",
    )
    .join(
        institution.select(
            F.col("ins_ncode").alias("ins_ncode_contrato"),
            "ins_cidentificationnumber",
            "ins_cname",
            "ins_cemail",
            "ins_cmobil_phone",
        ),
        on="ins_ncode_contrato",
        how="left",
    )
    .join(
        df_residencial.select(
            "per_ncode",
            "dir_res",
            "ciu_res_codigo",
        ),
        on="per_ncode",
        how="left",
    )
    .join(df_ciudad_nombre, on="ciu_res_codigo", how="left")
    .select(
        "per_ncode",
        "per_cidentificationnumber",
        "per_cfirstname",
        "per_cmiddlename",
        "per_clastname",
        "per_cemail",
        "per_cmobilephone",
        "per_dbirthdate",
        "per_cgender",
        "ins_cidentificationnumber",
        "ins_cname",
        "ins_cemail",
        "ins_cmobil_phone",
        "dir_res",
        "ciudad_residencia",
        "mem_ncode",
        "aco_ncode",
        F.lit("BENEFICIARIO").alias("rol"),
    )
)

# COMMAND ----------
# Union titular + beneficiario

df_resultado = df_titular.unionByName(df_beneficiario)

print("filas titular:     ", df_titular.count())
print("filas beneficiario:", df_beneficiario.count())
print("filas total:       ", df_resultado.count())
df_resultado.show(5, truncate=False)
