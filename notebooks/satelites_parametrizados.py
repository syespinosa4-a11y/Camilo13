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


def build_sat_pyc(df_params, grupo):
    raise NotImplementedError(
        "sat_pyc: pendiente — falta traducir estado de poliza, producto por "
        "ramo y los 3 roles (Asegurado/Tomador/Beneficiario) sobre el "
        "staging de staging_sat_pyc_generales(). Validar primero el "
        "staging (conteos vs #1ss_* del SQL) antes de continuar."
    )

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
