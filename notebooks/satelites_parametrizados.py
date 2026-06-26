# Databricks notebook source
# NTT DATA 2026
# Motor parametrizado de satelites — Data Vault (PySpark puro, sin SQL)
#
#  Implementa los lineamientos de Julian:
#  - Una sola tabla centralizada de parametros (DIM_PARAMETROS), formato
#    grupo_parametros | nombre | valor | valor_homologado.
#  - El notebook NO tiene "if satelite == 'sat1': ..." quemado: carga
#    DIM_PARAMETROS UNA SOLA VEZ en Spark y opera sobre ese DataFrame.
#  - Cada satelite es un grupo_parametros con estado activo/inactivo,
#    catalogo/esquema/destino parametrizables, y un "constructor" (nombre
#    de funcion PySpark registrada) en vez de una query SQL en texto —
#    esto evita ejecutar SQL dinamico (riesgo de inyeccion) y a la vez
#    evita el if/elif rigido: agregar un satelite = agregar filas de
#    parametros + registrar su funcion en CONSTRUCTORES, sin tocar el
#    bucle principal.
#
#  Catalogo de parametrizacion (DIM_PARAMETROS), nombres reservados por
#  grupo_parametros = sat_<nombre>:
#    estado          1=activo, 0=inactivo
#    constructor     nombre de funcion registrada en CONSTRUCTORES
#    catalogo        catalogo destino (ej. uc_axa_cli)
#    esquema         esquema destino (ej. silver)
#    tabla_destino   nombre de tabla destino (ej. sat_beyond_health)
#    id_columna_pk   nombre de la PK del satelite (ej. id_sat_beyond_health)
#    catalogo_fuente catalogo de las tablas origen (ej. axa_col_dv)
#    esquema_fuente  esquema de las tablas origen (ej. core_bh)
#  Homologaciones (opcionales, libres): nombre = "homologacion_<campo>",
#  valor = valor crudo, valor_homologado = valor estandar de negocio.

# COMMAND ----------
#
#   ## Bloque 1 — Carga de parametros (una sola vez)

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from datetime import datetime, timezone

spark = SparkSession.builder.getOrCreate()

PARAMS_TABLE = "`uc_axa_cli`.`silver`.`dim_parametros`"
LOAD_TS = datetime.now(timezone.utc).isoformat(timespec="seconds")


def cargar_parametros():
    """Carga DIM_PARAMETROS UNA SOLA VEZ y cachea el resultado: todos los
    filtros posteriores se hacen sobre este DataFrame en memoria, nunca
    con nuevas consultas a la tabla (lineamiento explicito de Julian)."""
    return spark.table(PARAMS_TABLE).cache()


def get_param(df_params, grupo: str, nombre: str, default=None):
    row = (
        df_params
        .filter((F.col("grupo_parametros") == grupo) & (F.col("nombre") == nombre))
        .select("valor")
        .first()
    )
    return row[0] if row else default


def satelites_activos(df_params) -> list:
    return [
        r["grupo_parametros"]
        for r in (
            df_params
            .filter(
                F.col("grupo_parametros").startswith("sat")
                & (F.col("nombre") == "estado")
                & (F.col("valor") == "1")
            )
            .select("grupo_parametros")
            .distinct()
            .collect()
        )
    ]


def fuente(df_params, grupo: str, tabla_fisica: str) -> "DataFrame":
    """Lee una tabla origen usando catalogo_fuente/esquema_fuente parametrizados
    para este grupo (satelite). No hay nombres de catalogo/esquema quemados."""
    catalogo = get_param(df_params, grupo, "catalogo_fuente")
    esquema = get_param(df_params, grupo, "esquema_fuente")
    return spark.table(f"`{catalogo}`.`{esquema}`.`{tabla_fisica}`")

# COMMAND ----------
#
#   ## Bloque 2 — Constructor: sat_beyond_health (Titulares + Beneficiarios)
#
#   Traducido 1:1 desde el script SQL de origen (sin usar spark.sql ni
#   texto SQL en ningun punto): los CTE (#temp) del SQL pasan a ser
#   funciones que devuelven DataFrames, y los JOIN/UNION ALL/WHERE se
#   expresan con la API de DataFrame de PySpark.

def _bh_novelty_administration(df_params, grupo):
    df = fuente(df_params, grupo, "bh_novelty_administration")
    return (
        df.filter(F.col("NAD_CMODIFIEDFIELD").isin(
            "Autorización tratamiento de datos",
            "Autorizo el tratamiento de los datos Personales",
        ))
        .groupBy("NAD_NPPALOBJECTCODE")
        .agg(F.max("NAD_DPROCESSDATE").alias("NAD_DPROCESSDATE"))
    )


def _bh_member_status_latest(df_params, grupo):
    """Equivalente a: where mst_ncode = (select max(mst_ncode) ... group by mem_ncode)."""
    df = fuente(df_params, grupo, "bh_member_status_history")
    w = Window.partitionBy("MEM_NCODE")
    return (
        df.withColumn("_max_mst", F.max("MST_NCODE").over(w))
        .filter(F.col("MST_NCODE") == F.col("_max_mst"))
        .drop("_max_mst")
    )


def _bh_residencial(df_params, grupo):
    ad = fuente(df_params, grupo, "bh_address").filter(F.col("LTY_NCODE") == 1)
    tel = fuente(df_params, grupo, "bh_address_telephone_number")
    ciu = fuente(df_params, grupo, "bh_city")
    joined = (
        ad.join(tel, tel["ADD_NCODE"] == ad["ADD_NCODE"], "left")
        .join(ciu, ciu["CIT_NCODE"] == ad["CIT_NCODE"], "left")
    )
    return (
        joined.groupBy(ad["PER_NCODE"].alias("per_ncode"))
        .agg(
            F.max(tel["ATN_CNUMBER"]).alias("tel_res"),
            F.max(ad["ADD_CADDRESS"]).alias("dir_res"),
            F.max(ciu["CIT_CLEGALCODE"]).alias("ciu_res"),
        )
    )


def _bh_titulares(df_params, grupo):
    m = fuente(df_params, grupo, "bh_member")
    a = fuente(df_params, grupo, "bh_affiliation_contract")
    pln = fuente(df_params, grupo, "bh_plan")
    prod = fuente(df_params, grupo, "bh_product")
    p = fuente(df_params, grupo, "bh_person")
    t = fuente(df_params, grupo, "bh_identification_type")
    i = fuente(df_params, grupo, "bh_institution")
    t0 = fuente(df_params, grupo, "bh_identification_type")
    h = _bh_member_status_latest(df_params, grupo)
    re_ = _bh_residencial(df_params, grupo)
    cit_res = fuente(df_params, grupo, "bh_city")
    ad_nov = _bh_novelty_administration(df_params, grupo)

    df = (
        m.join(a, a["ACO_NCODE"] == m["ACO_NCODE"], "inner")
        .join(pln, pln["PLA_NCODE"] == a["PLA_NCODE"], "left")
        .join(prod, prod["PRO_NCODE"] == pln["PRO_NCODE"], "left")
        .join(p, p["PER_NCODE"] == a["PER_NCODE"], "left")
        .join(t, t["ITY_NCODE"] == p["TID_NCODE"], "left")
        .join(i, i["INS_NCODE"] == a["INS_NCODE"], "left")
        .join(t0.alias("t0"), F.col("t0.ITY_NCODE") == i["IDT_NCODE"], "left")
        .join(h, h["MEM_NCODE"] == m["MEM_NCODE"], "left")
        .join(re_, re_["per_ncode"] == a["PER_NCODE"], "left")
        .join(cit_res.alias("cit_res"), F.col("cit_res.CIT_CLEGALCODE") == re_["ciu_res"], "left")
        .join(ad_nov, ad_nov["NAD_NPPALOBJECTCODE"] == p["PER_NCODE"], "left")
        .filter(
            (a["CTY_NCODE"] != 5)
            & (m["MEM_DSTARTINGDATE"] <= F.current_date())
            & (m["MEM_DENDINGDATE"] >= F.current_date())
            & h["MSH_DFINALDATE"].isNull()
        )
    )
    es_institucion = i["INS_NCODE"].isNotNull()
    return df.select(
        F.when(~es_institucion, t["ITY_CSHORTNAME"]).otherwise(F.col("t0.ITY_CSHORTNAME")).alias("TIPO_DE_DOCUMENTO"),
        F.when(~es_institucion, p["PER_CIDENTIFICATIONNUMBER"]).otherwise(i["INS_CIDENTIFICATIONNUMBER"]).alias("DOCUMENTO_DE_IDENTIFICACION"),
        F.when(~es_institucion, F.concat_ws(" ", p["PER_CFIRSTNAME"], F.coalesce(p["PER_CMIDDLENAME"], F.lit("")))).otherwise(F.lit("")).alias("NOMBRE"),
        F.when(~es_institucion, p["PER_CLASTNAME"]).otherwise(i["INS_CNAME"]).alias("PRIMER_APELLIDO"),
        F.when(~es_institucion, p["PER_CEMAIL"]).otherwise(i["INS_CEMAIL"]).alias("EMAIL"),
        F.when(~es_institucion, p["PER_CMOBILEPHONE"]).otherwise(i["INS_CMOBIL_PHONE"]).alias("CELULAR"),
        re_["dir_res"].alias("DIRECCION_RESIDENCIA"),
        F.col("cit_res.CIT_CNAME").alias("CIUDAD_RESIDENCIA"),
        p["PER_DBIRTHDATE"].alias("FECHA_NACIMIENTO"),
        p["PER_CGENDER"].alias("GENERO"),
        F.lit("").alias("EPS"),
        p["PER_BAUTHPERSONALINFO"].alias("ATDP_TITULAR"),
        ad_nov["NAD_DPROCESSDATE"].alias("FECHA_ATDP"),
        F.lit("TITULAR").alias("ROL"),
        m["MEM_DSTARTINGDATE"].alias("FEC_INI_VIGENCIA"),
    ).distinct()


def _bh_beneficiarios(df_params, grupo):
    m = fuente(df_params, grupo, "bh_member")
    a = fuente(df_params, grupo, "bh_affiliation_contract")
    pln = fuente(df_params, grupo, "bh_plan")
    prod = fuente(df_params, grupo, "bh_product")
    p = fuente(df_params, grupo, "bh_person")
    t = fuente(df_params, grupo, "bh_identification_type")
    mp = fuente(df_params, grupo, "bh_member_product_mpp")
    i2 = fuente(df_params, grupo, "bh_institution")
    h = _bh_member_status_latest(df_params, grupo)
    re_ = _bh_residencial(df_params, grupo)
    cit_res = fuente(df_params, grupo, "bh_city")
    ad_nov = _bh_novelty_administration(df_params, grupo)

    df = (
        m.join(a, a["ACO_NCODE"] == m["ACO_NCODE"], "inner")
        .join(pln, pln["PLA_NCODE"] == a["PLA_NCODE"], "left")
        .join(prod, prod["PRO_NCODE"] == pln["PRO_NCODE"], "left")
        .join(p, p["PER_NCODE"] == m["PER_NCODE"], "left")
        .join(t, t["ITY_NCODE"] == p["TID_NCODE"], "left")
        .join(mp, mp["MEM_NCODE"] == m["MEM_NCODE"], "left")
        .join(i2, i2["INS_NCODE"] == mp["INS_NCODEEPS"], "left")
        .join(h, h["MEM_NCODE"] == m["MEM_NCODE"], "left")
        .join(re_, re_["per_ncode"] == m["PER_NCODE"], "left")
        .join(cit_res.alias("cit_res"), F.col("cit_res.CIT_CLEGALCODE") == re_["ciu_res"], "left")
        .join(ad_nov, ad_nov["NAD_NPPALOBJECTCODE"] == p["PER_NCODE"], "left")
        .filter(
            (a["CTY_NCODE"] != 5)
            & (m["MEM_DSTARTINGDATE"] <= F.current_date())
            & (m["MEM_DENDINGDATE"] >= F.current_date())
            & h["MSH_DFINALDATE"].isNull()
        )
    )
    es_institucion = a["INS_NCODE"].isNotNull()
    return df.select(
        t["ITY_CSHORTNAME"].alias("TIPO_DE_DOCUMENTO"),
        p["PER_CIDENTIFICATIONNUMBER"].alias("DOCUMENTO_DE_IDENTIFICACION"),
        F.concat_ws(" ", p["PER_CFIRSTNAME"], F.coalesce(p["PER_CMIDDLENAME"], F.lit(""))).alias("NOMBRE"),
        p["PER_CLASTNAME"].alias("PRIMER_APELLIDO"),
        F.when(~es_institucion, p["PER_CEMAIL"]).otherwise(i2["INS_CEMAIL"]).alias("EMAIL"),
        F.when(~es_institucion, p["PER_CMOBILEPHONE"]).otherwise(i2["INS_CMOBIL_PHONE"]).alias("CELULAR"),
        re_["dir_res"].alias("DIRECCION_RESIDENCIA"),
        F.col("cit_res.CIT_CNAME").alias("CIUDAD_RESIDENCIA"),
        p["PER_DBIRTHDATE"].alias("FECHA_NACIMIENTO"),
        p["PER_CGENDER"].alias("GENERO"),
        i2["INS_CLEGALCODE"].alias("EPS"),
        p["PER_BAUTHPERSONALINFO"].alias("ATDP_TITULAR"),
        ad_nov["NAD_DPROCESSDATE"].alias("FECHA_ATDP"),
        F.lit("BENEFICIARIO").alias("ROL"),
        m["MEM_DSTARTINGDATE"].alias("FEC_INI_VIGENCIA"),
    ).distinct()


def build_sat_beyond_health(df_params, grupo):
    """SELECT * FROM #titulares UNION ALL SELECT * FROM #Beneficiario, ordenado
    por tipo y numero de documento."""
    titulares = _bh_titulares(df_params, grupo)
    beneficiarios = _bh_beneficiarios(df_params, grupo)
    return (
        titulares.unionByName(beneficiarios)
        .orderBy("TIPO_DE_DOCUMENTO", "DOCUMENTO_DE_IDENTIFICACION")
    )

# COMMAND ----------
#
#   ## Bloque 3 — Constructor: sat_pyc (SISE) — PRIMER BLOQUE: dedup "ultima
#   carga" (equivalente a las tablas #1ss_* del script "Generales" SQL).
#
#   PENDIENTE DE VALIDACION: este es solo el primer bloque (deduplicacion),
#   acordado por partes. Falta traducir: estado de poliza (#Estados_polizas),
#   producto por ramo (#Producto_Autos/#Producto_ext/#producto_vida_col),
#   email/celular/telefono deduplicados y los 3 roles (Asegurado/Tomador/
#   Beneficiario) con su UNION final filtrado por Estado='Vigente'. Se
#   agregan en un siguiente paso una vez se valide este bloque en Databricks.

def _sise_ultima_carga(df, claves: list):
    """Equivalente a: where a.fecha_cargue = (select max(fecha_cargue) from
    tabla b where <claves coinciden>) — quedarse con el registro mas
    reciente por combinacion de columnas clave."""
    w = Window.partitionBy(*claves)
    return (
        df.withColumn("_max_fc", F.max("fecha_cargue").over(w))
        .filter(F.col("fecha_cargue") == F.col("_max_fc"))
        .drop("_max_fc")
    )


def _sise_maseg_header(df_params, grupo):
    df = fuente(df_params, grupo, "ss_maseg_header")
    df = _sise_ultima_carga(df, ["id_persona", "cod_aseg"])
    return df.select("id_persona", "cod_aseg", "fec_baja", "cod_ocupacion", "cod_ciiu", "cod_tipo_aseg")


def _sise_mpersona_telef(df_params, grupo):
    df = fuente(df_params, grupo, "ss_mpersona_telef")
    df = _sise_ultima_carga(df, ["id_persona", "cod_tipo_telef"])
    return df.select("id_persona", "cod_tipo_telef", "txt_telefono")


def _sise_mpersona_dir(df_params, grupo):
    df = fuente(df_params, grupo, "ss_mpersona_dir")
    df = _sise_ultima_carga(df, ["id_persona", "cod_tipo_dir"])
    return df.select("id_persona", "cod_tipo_dir", "txt_direccion", "cod_pais", "cod_dpto", "cod_municipio")


def _sise_mpersona(df_params, grupo):
    df = fuente(df_params, grupo, "ss_mpersona")
    df = _sise_ultima_carga(df, ["id_persona"])
    return df.select(
        "id_persona", "nro_nit", "nro_doc", "txt_apellido1", "txt_apellido2",
        "txt_nombre", "fec_nac", "cod_tipo_doc", "cod_tipo_persona", "txt_sexo", "cod_est_civil",
    )


def _sise_ttipo_doc(df_params, grupo):
    df = fuente(df_params, grupo, "ss_ttipo_doc")
    df = _sise_ultima_carga(df, ["cod_tipo_doc"])
    return df.select("cod_tipo_doc", "txt_desc", "txt_desc_redu").distinct()


def _sise_sg_tramo(df_params, grupo):
    df = fuente(df_params, grupo, "ss_sg_tramo")
    df = _sise_ultima_carga(df, ["cod_ramo"])
    return df.select("cod_ramo", "txt_desc", "cod_ttipo_ramo", "txt_desc_redu")


def _sise_sg_product(df_params, grupo):
    df = fuente(df_params, grupo, "ss_sg_product")
    df = _sise_ultima_carga(df, ["product_id"])
    return df.select("product_id", "description")


def _sise_sg_tau_coberturas(df_params, grupo):
    df = fuente(df_params, grupo, "ss_sg_tau_coberturas")
    df = _sise_ultima_carga(df, ["cod_cobertura"])
    return df.select("cod_cobertura", "txt_desc")


def _sise_sg_pv_informacion_adic(df_params, grupo):
    df = fuente(df_params, grupo, "ss_sg_pv_informacion_adic")
    df = _sise_ultima_carga(df, ["id_pv"])
    return df.select("id_pv", "cod_producto")


def _sise_sg_di_datos_au(df_params, grupo):
    df = fuente(df_params, grupo, "ss_sg_di_datos_au")
    df = _sise_ultima_carga(df, ["id_pv", "cod_item"])
    return df.select("id_pv", "cod_item", "cod_plan_cober")


def _sise_tdpto(df_params, grupo):
    df = fuente(df_params, grupo, "ss_tdpto")
    df = _sise_ultima_carga(df, ["cod_pais", "cod_dpto"])
    return df.select("cod_pais", "cod_dpto", "txt_desc")


def _sise_tmunicipio(df_params, grupo):
    df = fuente(df_params, grupo, "ss_tmunicipio")
    df = _sise_ultima_carga(df, ["cod_pais", "cod_dpto", "cod_municipio"])
    return df.select("cod_pais", "cod_dpto", "cod_municipio", "txt_desc")


def _sise_sg_di_benef(df_params, grupo):
    """select distinct id_pv, cod_benef ... where cod_ind_benef = 1"""
    df = fuente(df_params, grupo, "ss_sg_di_benef").filter(F.col("cod_ind_benef") == 1)
    df = _sise_ultima_carga(df, ["id_pv", "cod_benef"])
    return df.select("id_pv", "cod_benef").distinct()


def _sise_sg_pv_header(df_params, grupo):
    """Dedup por (cod_suc, cod_ramo, nro_pol) a la ultima carga, y dentro de
    eso al ultimo nro_endoso excluyendo cod_grupo_endo = 15."""
    df = fuente(df_params, grupo, "ss_sg_pv_header")
    df = _sise_ultima_carga(df, ["cod_suc", "cod_ramo", "nro_pol"])
    w_endoso = Window.partitionBy("cod_suc", "cod_ramo", "nro_pol")
    df_validos = df.filter(F.col("cod_grupo_endo") != 15)
    df = (
        df.join(
            df_validos.groupBy("cod_suc", "cod_ramo", "nro_pol")
            .agg(F.max("nro_endoso").alias("_max_endoso")),
            ["cod_suc", "cod_ramo", "nro_pol"], "inner",
        )
        .filter(F.col("nro_endoso") == F.col("_max_endoso"))
        .drop("_max_endoso")
    )
    return df.select(
        "id_pv", "cod_suc", "cod_ramo", "nro_pol", "aaaa_endoso", "nro_endoso",
        "cod_aseg", "fec_emi", "fec_vig_desde", "fec_vig_hasta", "fec_hora_desde",
        "cod_grupo_endo", "cod_sistema",
    )


def _sise_sg_di_header(df_params, grupo):
    df = fuente(df_params, grupo, "ss_sg_di_header")
    df = _sise_ultima_carga(df, ["id_pv", "cod_item", "cod_aseg"])
    return df.select("id_pv", "cod_producto", "cod_aseg", "cod_item").distinct()


def _sise_sg_pv_varios(df_params, grupo):
    df = fuente(df_params, grupo, "ss_sg_pv_varios")
    df = _sise_ultima_carga(df, ["id_pv"])
    return df.select("id_pv", "cod_producto", "cod_tipo_poliza").distinct()


def _sise_sg_ttipo_poliza(df_params, grupo):
    df = fuente(df_params, grupo, "ss_sg_ttipo_poliza")
    df = _sise_ultima_carga(df, ["cod_ramo", "cod_tipo_poliza"])
    return df.select("cod_ramo", "cod_tipo_poliza", "txt_desc").distinct()


def _sise_sg_tvprod_header(df_params, grupo):
    df = fuente(df_params, grupo, "ss_sg_tvprod_header")
    df = _sise_ultima_carga(df, ["cod_ramo", "cod_producto"])
    return df.select("cod_ramo", "cod_producto", "txt_desc_producto").distinct()


def _sise_sg_pv_col_categ(df_params, grupo):
    """select distinct id_pv, cod_producto — sin dedup por fecha_cargue (el
    SQL original no lo aplica para esta tabla)."""
    df = fuente(df_params, grupo, "ss_sg_pv_col_categ")
    return df.select("id_pv", "cod_producto").distinct()


def staging_sat_pyc_generales(df_params, grupo) -> dict:
    """Devuelve el diccionario de DataFrames de deduplicacion 'ultima carga'
    del script SISE Generales — equivalente a las tablas #1ss_* antes de
    calcular estado de poliza, producto y roles. Para validar contra el SQL
    original: comparar conteos de filas de cada DataFrame con su tabla
    temporal #1ss_* correspondiente en una corrida de prueba en SQL Server."""
    return {
        "maseg_header": _sise_maseg_header(df_params, grupo),
        "mpersona_telef": _sise_mpersona_telef(df_params, grupo),
        "mpersona_dir": _sise_mpersona_dir(df_params, grupo),
        "mpersona": _sise_mpersona(df_params, grupo),
        "ttipo_doc": _sise_ttipo_doc(df_params, grupo),
        "sg_tramo": _sise_sg_tramo(df_params, grupo),
        "sg_product": _sise_sg_product(df_params, grupo),
        "sg_tau_coberturas": _sise_sg_tau_coberturas(df_params, grupo),
        "sg_pv_informacion_adic": _sise_sg_pv_informacion_adic(df_params, grupo),
        "sg_di_datos_au": _sise_sg_di_datos_au(df_params, grupo),
        "tdpto": _sise_tdpto(df_params, grupo),
        "tmunicipio": _sise_tmunicipio(df_params, grupo),
        "sg_di_benef": _sise_sg_di_benef(df_params, grupo),
        "sg_pv_header": _sise_sg_pv_header(df_params, grupo),
        "sg_di_header": _sise_sg_di_header(df_params, grupo),
        "sg_pv_varios": _sise_sg_pv_varios(df_params, grupo),
        "sg_ttipo_poliza": _sise_sg_ttipo_poliza(df_params, grupo),
        "sg_tvprod_header": _sise_sg_tvprod_header(df_params, grupo),
        "sg_pv_col_categ": _sise_sg_pv_col_categ(df_params, grupo),
    }

# COMMAND ----------
#
#   ## Bloque 3b — sat_pyc / Generales: estado de poliza, producto por ramo,
#   contactos deduplicados y union de los 3 roles (Asegurado/Tomador/
#   Beneficiario), filtrado a Estado='Vigente' — traduccion 1:1 del script
#   SQL "Generales".

def _estados_polizas_pv_tramo_endo(df_params, grupo, st, ttipo_ramo: list):
    pv = st["sg_pv_header"]
    tramo = st["sg_tramo"].filter(F.col("cod_ttipo_ramo").isin(*ttipo_ramo))
    ge = fuente(df_params, grupo, "ss_sg_tgrupo_endo")
    return pv.join(tramo, "cod_ramo", "inner").join(
        ge, pv["cod_grupo_endo"] == ge["cod_grupo_endo"], "inner"
    )


def sise_estados_polizas_generales(df_params, grupo, st):
    autos = _estados_polizas_pv_tramo_endo(df_params, grupo, st, [2, 11])
    autos = autos.select(
        autos["id_pv"],
        F.when(
            (F.col("sn_cancelacion") == 0) & (F.col("sn_cancelacion_automatica") == 0)
            & (autos["fec_vig_hasta"] > F.current_date()),
            F.lit(1),
        ).otherwise(F.lit(2)).alias("estado"),
        F.lit("Autos").alias("ramos"),
    )

    ge_excl = (
        fuente(df_params, grupo, "ss_sg_tgrupo_endo")
        .filter(
            (F.col("sn_poliza") == -1)
            | (F.col("sn_renovacion_automatica") == -1)
            | (F.col("sn_renovacion_manual") == -1)
        )
        .select("cod_grupo_endo")
    )
    pv_all = st["sg_pv_header"]
    conteo_excl = (
        pv_all.join(ge_excl, "cod_grupo_endo", "inner")
        .groupBy("cod_suc", "cod_ramo", "nro_pol")
        .agg(F.count(F.lit(1)).alias("_cnt_excl"))
    )

    gen_base = _estados_polizas_pv_tramo_endo(
        df_params, grupo, st, [1, 5, 6, 8, 12, 14, 15, 16, 7]
    ).join(conteo_excl, ["cod_suc", "cod_ramo", "nro_pol"], "left")

    generales_1 = (
        gen_base.filter((F.coalesce(F.col("_cnt_excl"), F.lit(0)) <= 1))
        .select(
            F.col("id_pv"),
            F.when(
                (F.col("sn_cancelacion") == 0)
                & (F.col("fec_vig_hasta") > F.current_date())
                & (F.col("fec_vig_desde") <= F.current_date()),
                F.lit(1),
            ).otherwise(F.lit(2)).alias("estado"),
            F.lit("Generales").alias("ramos"),
        )
    )

    generales_2 = (
        _estados_polizas_pv_tramo_endo(df_params, grupo, st, [1, 5, 6, 8, 12, 14, 15, 16])
        .join(conteo_excl, ["cod_suc", "cod_ramo", "nro_pol"], "left")
        .filter(F.coalesce(F.col("_cnt_excl"), F.lit(0)) > 1)
        .select(
            F.col("id_pv"),
            F.when(
                (F.col("sn_cancelacion") == 0) & (F.col("fec_vig_hasta") > F.current_date()),
                F.lit(1),
            ).otherwise(F.lit(2)).alias("estado"),
            F.lit("Generales").alias("ramos"),
        )
    )

    vida_col = _estados_polizas_pv_tramo_endo(df_params, grupo, st, [3, 10, 13, 18])
    vida_col = vida_col.select(
        vida_col["id_pv"],
        F.when(
            (F.col("sn_cancelacion") == 0) & (F.col("sn_cancelacion_automatica") == 0)
            & (vida_col["fec_vig_hasta"] > F.current_date()),
            F.lit(1),
        ).otherwise(F.lit(2)).alias("estado"),
        F.lit("Vida Colectiva").alias("ramos"),
    )

    return autos.unionByName(generales_1).unionByName(generales_2).unionByName(vida_col)


def sise_producto_generales(df_params, grupo, st):
    """#Producto_Autos + #Producto_ext + #producto_vida_col, cada uno
    devuelto por separado (se usan en distintos joins en los roles)."""
    pv = st["sg_pv_header"]
    di = st["sg_di_header"]
    dau = st["sg_di_datos_au"]
    cob = st["sg_tau_coberturas"]
    autos = (
        pv.filter(F.col("cod_ramo").isin(10, 11, 12, 15))
        .join(di, "id_pv", "inner")
        .join(dau, (pv["id_pv"] == dau["id_pv"]) & (di["cod_item"] == dau["cod_item"]), "inner")
        .join(cob, dau["cod_plan_cober"] == cob["cod_cobertura"], "inner")
        .select(
            pv["id_pv"], pv["cod_ramo"], di["cod_item"],
            dau["cod_plan_cober"].alias("cod_producto"), cob["txt_desc"].alias("producto"),
        )
        .distinct()
    )

    varios = st["sg_pv_varios"]
    tpol = st["sg_ttipo_poliza"]
    ext = (
        pv.filter(~F.col("cod_ramo").isin(10, 11, 12, 15))
        .join(varios, "id_pv", "inner")
        .join(
            tpol,
            (pv["cod_ramo"] == tpol["cod_ramo"]) & (varios["cod_tipo_poliza"] == tpol["cod_tipo_poliza"]),
            "inner",
        )
        .select(
            pv["id_pv"], pv["cod_ramo"],
            tpol["cod_tipo_poliza"].alias("cod_producto"), tpol["txt_desc"].alias("producto"),
        )
        .distinct()
    )

    col_categ = st["sg_pv_col_categ"]
    tvprod = st["sg_tvprod_header"]
    vida_col = (
        pv.filter(F.col("cod_sistema").isin(3, 10))
        .join(col_categ, "id_pv", "inner")
        .join(
            tvprod,
            (col_categ["cod_producto"] == tvprod["cod_producto"]) & (pv["cod_ramo"] == tvprod["cod_ramo"]),
            "inner",
        )
        .select(
            pv["id_pv"], pv["cod_ramo"],
            tvprod["cod_producto"], tvprod["txt_desc_producto"].alias("producto"),
        )
        .distinct()
    )
    w = Window.partitionBy("id_pv", "cod_ramo").orderBy(F.col("cod_producto").desc())
    vida_col = (
        vida_col.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    return autos, ext, vida_col


def _sise_contactos(st):
    """#Email/#celular/#telfono/#direccion: maximo de los 2 primeros valores
    por persona (rank descendente), separados por tipo de contacto."""
    dir_ = st["mpersona_dir"]
    telef = st["mpersona_telef"]
    tdpto = st["tdpto"]
    tmun = st["tmunicipio"]

    def top2(df, claves, valor_col, salida):
        w = Window.partitionBy(*claves).orderBy(F.col(valor_col).desc())
        ranked = df.filter(F.col(valor_col) != "").withColumn("_rnk", F.rank().over(w))
        return (
            ranked.groupBy("id_persona")
            .agg(
                F.max(F.when(F.col("_rnk") == 1, F.col(valor_col))).alias(f"{salida}_1"),
                F.max(F.when(F.col("_rnk") == 2, F.col(valor_col))).alias(f"{salida}_2"),
            )
        )

    email = top2(dir_.filter(F.col("cod_tipo_dir").isin(15, 13)), ["id_persona", "cod_tipo_dir"], "txt_direccion", "email")
    celular = top2(telef.filter(F.col("cod_tipo_telef").isin(4, 10)), ["id_persona", "cod_tipo_telef"], "txt_telefono", "celular")
    telefono = top2(telef.filter(~F.col("cod_tipo_telef").isin(4, 10)), ["id_persona", "cod_tipo_telef"], "txt_telefono", "telefono")

    dir_base = dir_.filter(~F.col("cod_tipo_dir").isin(15, 13) & (F.col("txt_direccion") != ""))
    w_min = Window.partitionBy("id_persona")
    dir_base = dir_base.withColumn("_min_tipo", F.min("cod_tipo_dir").over(w_min)).filter(
        F.col("cod_tipo_dir") == F.col("_min_tipo")
    )
    direccion = (
        dir_base.join(tdpto, ["cod_pais", "cod_dpto"], "inner")
        .join(tmun, ["cod_pais", "cod_dpto", "cod_municipio"], "inner")
        .select(
            dir_base["id_persona"], dir_base["txt_direccion"],
            tdpto["txt_desc"].alias("departamento"), tmun["txt_desc"].alias("muncipio"),
        )
    )
    return email, celular, telefono, direccion


def _edad(fec_nac_col):
    return F.floor(F.months_between(F.current_date(), fec_nac_col) / 12).cast("int")


def _rol_select(pv, tramo, tpd, persona, autos, ext, vida_col, tpol, estados, email, celular, telefono, direccion, rol_nombre):
    return [
        pv["nro_pol"], pv["cod_ramo"], pv["cod_suc"], pv["nro_endoso"], pv["aaaa_endoso"],
        tramo["txt_desc"].alias("ramo"),
        F.coalesce(F.col("aut.cod_producto"), F.col("aut_ext.cod_producto"), F.col("p_vid_col.cod_producto"), tpol["cod_tipo_poliza"]).alias("cod_producto"),
        F.coalesce(F.col("aut.producto"), F.col("aut_ext.producto"), F.col("p_vid_col.producto"), tpol["txt_desc"]).alias("producto"),
        tpd["txt_desc_redu"].alias("tipo_documento"),
        tpd["txt_desc"].alias("desc_tipo_documento"),
        persona["nro_doc"].alias("numero_documento"),
        persona["txt_apellido1"], persona["txt_apellido2"], persona["txt_nombre"],
        F.lit(rol_nombre).alias("Rol"),
        _edad(persona["fec_nac"]).alias("edad"),
        persona["txt_sexo"].alias("Genero"),
        persona["fec_nac"].alias("Fecha_Nacimiento"),
        F.col("email_1"), F.col("email_2"), F.col("celular_1"), F.col("celular_2"),
        F.when(estados["estado"] == 1, F.lit("Vigente")).when(estados["estado"] == 2, F.lit("Cancelado")).alias("Estado"),
        F.col("telefono_1"), F.col("telefono_2"),
        direccion["txt_direccion"], direccion["departamento"], direccion["muncipio"],
    ]


def _rol_asegurado_generales(pv, st, autos, ext, vida_col, estados):
    """from pv inner join di_header inner join maseg_header(cod_aseg=b.cod_aseg)
    inner join mpersona, producto via di_header.cod_item."""
    di = st["sg_di_header"]
    maseg = st["maseg_header"]
    persona = st["mpersona"]
    tpd = st["ttipo_doc"]
    tramo = st["sg_tramo"]
    pvv = st["sg_pv_varios"]
    tpol = st["sg_ttipo_poliza"]
    email, celular, telefono, direccion = _sise_contactos(st)

    base = (
        pv.join(di, "id_pv", "inner")
        .join(maseg, di["cod_aseg"] == maseg["cod_aseg"], "inner")
        .join(persona, maseg["id_persona"] == persona["id_persona"], "inner")
        .join(email, persona["id_persona"] == email["id_persona"], "left")
        .join(celular, persona["id_persona"] == celular["id_persona"], "left")
        .join(telefono, persona["id_persona"] == telefono["id_persona"], "left")
        .join(direccion, persona["id_persona"] == direccion["id_persona"], "left")
        .join(tpd, persona["cod_tipo_doc"] == tpd["cod_tipo_doc"], "inner")
        .join(tramo, pv["cod_ramo"] == tramo["cod_ramo"], "inner")
        .join(autos.alias("aut"), (pv["id_pv"] == F.col("aut.id_pv")) & (di["cod_item"] == F.col("aut.cod_item")), "left")
        .join(ext.alias("aut_ext"), pv["id_pv"] == F.col("aut_ext.id_pv"), "left")
        .join(vida_col.alias("p_vid_col"), pv["id_pv"] == F.col("p_vid_col.id_pv"), "left")
        .join(pvv, pv["id_pv"] == pvv["id_pv"], "left")
        .join(tpol, (pv["cod_ramo"] == tpol["cod_ramo"]) & (pvv["cod_tipo_poliza"] == tpol["cod_tipo_poliza"]), "left")
        .join(estados, pv["id_pv"] == estados["id_pv"], "left")
    )
    return base.select(*_rol_select(pv, tramo, tpd, persona, autos, ext, vida_col, tpol, estados, email, celular, telefono, direccion, "Asegurado")).distinct()


def _rol_tomador_generales(pv, st, autos, ext, vida_col, estados):
    """from pv inner join maseg_header(cod_aseg=pv.cod_aseg) inner join
    mpersona, producto via #1ss_sg_di_header_aux (max cod_item por id_pv)."""
    maseg = st["maseg_header"]
    persona = st["mpersona"]
    tpd = st["ttipo_doc"]
    tramo = st["sg_tramo"]
    pvv = st["sg_pv_varios"]
    tpol = st["sg_ttipo_poliza"]
    email, celular, telefono, direccion = _sise_contactos(st)
    di_aux = st["sg_di_header"].groupBy("id_pv").agg(F.max("cod_item").alias("cod_item"))

    base = (
        pv.join(maseg, pv["cod_aseg"] == maseg["cod_aseg"], "inner")
        .join(persona, maseg["id_persona"] == persona["id_persona"], "inner")
        .join(email, persona["id_persona"] == email["id_persona"], "left")
        .join(celular, persona["id_persona"] == celular["id_persona"], "left")
        .join(telefono, persona["id_persona"] == telefono["id_persona"], "left")
        .join(direccion, persona["id_persona"] == direccion["id_persona"], "left")
        .join(tpd, persona["cod_tipo_doc"] == tpd["cod_tipo_doc"], "inner")
        .join(tramo, pv["cod_ramo"] == tramo["cod_ramo"], "inner")
        .join(di_aux, pv["id_pv"] == di_aux["id_pv"], "left")
        .join(autos.alias("aut"), (pv["id_pv"] == F.col("aut.id_pv")) & (di_aux["cod_item"] == F.col("aut.cod_item")), "left")
        .join(ext.alias("aut_ext"), pv["id_pv"] == F.col("aut_ext.id_pv"), "left")
        .join(vida_col.alias("p_vid_col"), pv["id_pv"] == F.col("p_vid_col.id_pv"), "left")
        .join(pvv, pv["id_pv"] == pvv["id_pv"], "left")
        .join(tpol, (pv["cod_ramo"] == tpol["cod_ramo"]) & (pvv["cod_tipo_poliza"] == tpol["cod_tipo_poliza"]), "left")
        .join(estados, pv["id_pv"] == estados["id_pv"], "left")
    )
    return base.select(*_rol_select(pv, tramo, tpd, persona, autos, ext, vida_col, tpol, estados, email, celular, telefono, direccion, "Tomador")).distinct()


def _rol_beneficiario_generales(pv, st, autos, ext, vida_col, estados):
    """from pv inner join di_benef(cod_ind_benef=1) inner join
    mpersona(cod_benef=id_persona)."""
    di_benef = st["sg_di_benef"]
    persona = st["mpersona"]
    tpd = st["ttipo_doc"]
    tramo = st["sg_tramo"]
    pvv = st["sg_pv_varios"]
    tpol = st["sg_ttipo_poliza"]
    email, celular, telefono, direccion = _sise_contactos(st)

    base = (
        pv.join(di_benef, "id_pv", "inner")
        .join(persona, di_benef["cod_benef"] == persona["id_persona"], "inner")
        .join(email, persona["id_persona"] == email["id_persona"], "left")
        .join(celular, persona["id_persona"] == celular["id_persona"], "left")
        .join(telefono, persona["id_persona"] == telefono["id_persona"], "left")
        .join(direccion, persona["id_persona"] == direccion["id_persona"], "left")
        .join(tpd, persona["cod_tipo_doc"] == tpd["cod_tipo_doc"], "inner")
        .join(tramo, pv["cod_ramo"] == tramo["cod_ramo"], "inner")
        .join(autos.alias("aut"), pv["id_pv"] == F.col("aut.id_pv"), "left")
        .join(ext.alias("aut_ext"), pv["id_pv"] == F.col("aut_ext.id_pv"), "left")
        .join(vida_col.alias("p_vid_col"), pv["id_pv"] == F.col("p_vid_col.id_pv"), "left")
        .join(pvv, pv["id_pv"] == pvv["id_pv"], "left")
        .join(tpol, (pv["cod_ramo"] == tpol["cod_ramo"]) & (pvv["cod_tipo_poliza"] == tpol["cod_tipo_poliza"]), "left")
        .join(estados, pv["id_pv"] == estados["id_pv"], "left")
    )
    return base.select(*_rol_select(pv, tramo, tpd, persona, autos, ext, vida_col, tpol, estados, email, celular, telefono, direccion, "Beneficiario")).distinct()


def build_sat_pyc_generales(df_params, grupo):
    st = staging_sat_pyc_generales(df_params, grupo)
    pv = st["sg_pv_header"]
    estados = sise_estados_polizas_generales(df_params, grupo, st)
    autos, ext, vida_col = sise_producto_generales(df_params, grupo, st)

    rol_asegurado = _rol_asegurado_generales(pv, st, autos, ext, vida_col, estados)
    rol_tomador = _rol_tomador_generales(pv, st, autos, ext, vida_col, estados)
    rol_beneficiario = _rol_beneficiario_generales(pv, st, autos, ext, vida_col, estados)

    clientes = rol_asegurado.unionByName(rol_tomador).unionByName(rol_beneficiario).distinct()
    return clientes.filter(F.col("Estado") == "Vigente")


def build_sat_pyc(df_params, grupo):
    """Por ahora solo implementa el script 'Generales'. El script 'Vida'
    (tablas ss_sv_*, logica de producto individual/colectivo distinta)
    queda pendiente como build_sat_pyc_vida — se valida primero Generales
    antes de traducir Vida, segun lo acordado."""
    return build_sat_pyc_generales(df_params, grupo)

# COMMAND ----------
#
#   ## Bloque 4 — Constructor: sat_arl — PENDIENTE
#
#   No se cuenta aun con la logica de negocio/joins entre las 6 tablas
#   AS400 (AAEMPAF0, AAAFAAF0, AAAFNAF0, AACIUAF0, AAICOAF0, AACENAF0).
#   Se deja registrado el grupo de parametros pero el constructor lanza
#   error explicito hasta tener esa logica.

def build_sat_arl(df_params, grupo):
    raise NotImplementedError(
        "sat_arl: pendiente — falta el script/logica de negocio que define "
        "como se relacionan AAEMPAF0, AAAFAAF0, AAAFNAF0, AACIUAF0, "
        "AAICOAF0 y AACENAF0 entre si."
    )

# COMMAND ----------
#
#   ## Bloque 5 — Motor generico: lee parametros, despacha por nombre de
#   constructor y escribe cada satelite activo. SIN if/elif por satelite.

CONSTRUCTORES = {
    "build_sat_beyond_health": build_sat_beyond_health,
    "build_sat_pyc": build_sat_pyc,
    "build_sat_arl": build_sat_arl,
}


def escribir_satelite(df_params, grupo: str, df_resultado):
    catalogo = get_param(df_params, grupo, "catalogo")
    esquema = get_param(df_params, grupo, "esquema")
    tabla = get_param(df_params, grupo, "tabla_destino")
    id_col = get_param(df_params, grupo, "id_columna_pk")
    destino = f"`{catalogo}`.`{esquema}`.`{tabla}`"

    df_resultado = df_resultado.withColumn("dv_load_date", F.lit(LOAD_TS))
    if id_col:
        df_resultado = df_resultado.withColumn(id_col, F.monotonically_increasing_id().cast("long"))

    df_resultado.writeTo(destino).using("delta").createOrReplace()
    print(f"  ✓ {grupo} → {destino}  ({spark.table(destino).count():,} filas)")


def ejecutar_satelites_activos():
    df_params = cargar_parametros()
    activos = satelites_activos(df_params)
    print(f"Satelites activos en DIM_PARAMETROS: {activos}")

    for grupo in activos:
        constructor_name = get_param(df_params, grupo, "constructor")
        builder = CONSTRUCTORES.get(constructor_name)
        if builder is None:
            print(f"  ✗ {grupo}: constructor '{constructor_name}' no registrado en CONSTRUCTORES")
            continue
        try:
            df_resultado = builder(df_params, grupo)
            escribir_satelite(df_params, grupo, df_resultado)
        except NotImplementedError as e:
            print(f"  ⏸ {grupo}: {e}")
        except Exception as e:
            print(f"  ✗ {grupo}: ERROR — {e}")


ejecutar_satelites_activos()
