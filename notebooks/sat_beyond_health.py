# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from datetime import datetime, timezone

spark = SparkSession.builder.getOrCreate()

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", str(64 * 1024 * 1024))
spark.conf.set("spark.sql.shuffle.partitions", "400")

MODO_PRUEBA = True
_CKPT_BASE = "dbfs:/tmp/sat_checkpoints"

def _materializar(df, nombre: str):
    ruta = f"{_CKPT_BASE}/{nombre}"
    if MODO_PRUEBA:
        df = df.coalesce(4)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(ruta)
    return spark.read.format("delta").load(ruta)

CONFIG = {
    "catalogo_fuente": "axa_col_slv_dv",
    "esquema_fuente": "core_bh",
    "catalogo_destino": "uc_axa_cli",
    "esquema_destino": "silver",
    "tabla_destino": "sat_beyond_health",
    "id_columna_pk": "id_sat_beyond_health",
}

LOAD_TS = datetime.now(timezone.utc).isoformat(timespec="seconds")

TABLAS_BH = [
    "bh_sa_person",
    "bh_sa_address",
    "bh_sa_institution",
    "bh_sa_city",
    "bh_sa_affiliation_contract",
    "bh_sa_member",
]

# COMMAND ----------
# Lectura generica de tablas fuente

def fuente(tabla_fisica: str):
    catalogo = CONFIG["catalogo_fuente"]
    esquema = CONFIG["esquema_fuente"]
    df = spark.table(f"`{catalogo}`.`{esquema}`.`{tabla_fisica}`")
    if MODO_PRUEBA:
        df = df.sample(withReplacement=False, fraction=0.005, seed=42)
    return df


def cargar_tablas_bh() -> dict:
    return {t: fuente(t) for t in TABLAS_BH}

# COMMAND ----------
# Llave de entidad: coalesce(PER_NCODE, INS_NCODE) detectado por columnas
# presentes en cada tabla, no por nombre de tabla.

def _mapa_columnas(df):
    return {c.upper(): c for c in df.columns}


def llave_entidad(df):
    mapa = _mapa_columnas(df)
    col_per = mapa.get("PER_NCODE")
    col_ins = mapa.get("INS_NCODE")
    if col_per and col_ins:
        return F.coalesce(F.col(col_per).cast("string"), F.col(col_ins).cast("string"))
    if col_per:
        return F.col(col_per).cast("string")
    if col_ins:
        return F.col(col_ins).cast("string")
    return None

def llave_comun(izq, der):
    mapa_izq = _mapa_columnas(izq)
    mapa_der = _mapa_columnas(der)
    comunes = [k for k in mapa_izq if k in mapa_der and k.endswith("NCODE")]
    if not comunes:
        raise ValueError("No se encontro llave comun (*_NCODE) entre las dos tablas")
    # PER_NCODE/INS_NCODE son llaves de entidad (ya resueltas via llave_entidad);
    # si hay otra *_NCODE comun mas especifica de la tabla, se prefiere esa.
    especificas = [k for k in comunes if k not in ("PER_NCODE", "INS_NCODE")]
    clave = especificas[0] if especificas else comunes[0]
    return mapa_izq[clave], mapa_der[clave]

# COMMAND ----------
# Universo base de entidades: union de persona e institucion, cada una con
# su ID_ENTIDAD_HUB ya resuelto.

def base_entidades(tablas: dict):
    p = tablas["bh_sa_person"]
    i = tablas["bh_sa_institution"]
    p = p.withColumn("ID_ENTIDAD_HUB", llave_entidad(p)).withColumn("TIPO_ENTIDAD", F.lit("PERSONA"))
    i = i.withColumn("ID_ENTIDAD_HUB", llave_entidad(i)).withColumn("TIPO_ENTIDAD", F.lit("INSTITUCION"))
    return p.unionByName(i, allowMissingColumns=True)

# COMMAND ----------
# Join generico de una tabla "satelite de entidad" (address, affiliation_contract)
# contra el universo base, por ID_ENTIDAD_HUB.

def unir_por_entidad(base, tabla, alias: str):
    llave = llave_entidad(tabla)
    if llave is None:
        raise ValueError(f"{alias}: no tiene PER_NCODE ni INS_NCODE, no se puede unir por entidad")
    tabla_alias = tabla.withColumn("ID_ENTIDAD_HUB", llave).alias(alias)
    unido = base.join(
        tabla_alias,
        base["ID_ENTIDAD_HUB"] == tabla_alias["ID_ENTIDAD_HUB"],
        "left",
    )
    return unido.drop(tabla_alias["ID_ENTIDAD_HUB"])

# COMMAND ----------
# Join por puente: para tablas que no tienen PER_NCODE/INS_NCODE directo
# (ej. bh_sa_member), se cruzan contra otra tabla ya unida (ej. affiliation_contract)
# usando la llave *_NCODE que tengan en comun (detectada dinamicamente, no quemada).

def unir_por_puente(df, alias_referencia: str, tabla_referencia_original, tabla_nueva, alias_nueva: str, broadcast: bool = False):
    llave_izq, llave_der = llave_comun(tabla_referencia_original, tabla_nueva)
    tabla_alias = tabla_nueva.alias(alias_nueva)
    if broadcast:
        tabla_alias = F.broadcast(tabla_alias)
    return df.join(
        tabla_alias,
        df[f"{alias_referencia}.{llave_izq}"] == tabla_alias[llave_der],
        "left",
    )

# COMMAND ----------
# Construccion del "big dataframe" de sat_beyond_health.

def build_sat_beyond_health():
    tablas = cargar_tablas_bh()

    base = _materializar(base_entidades(tablas), "bh_base_entidades")
    df = unir_por_entidad(base, tablas["bh_sa_address"], "addr")
    df = unir_por_entidad(df, tablas["bh_sa_affiliation_contract"], "aco")
    df = unir_por_puente(df, "aco", tablas["bh_sa_affiliation_contract"], tablas["bh_sa_member"], "mem", broadcast=True)

    # bh_sa_city es catalogo de referencia (pocas filas): se fuerza broadcast
    # para evitar una baraja innecesaria contra el universo ya unido, que es
    # mucho mas grande.
    ciu = tablas["bh_sa_city"]
    llave_addr, llave_ciu_addr = llave_comun(tablas["bh_sa_address"], ciu)

    df = df.join(
        F.broadcast(ciu.alias("ciu")),
        df[f"addr.{llave_addr}"] == F.col(f"ciu.{llave_ciu_addr}"),
        "left",
    )
    return df

# COMMAND ----------
# El join trae columnas repetidas (mismo nombre en mas de una tabla origen,
# ej. PER_NCODE, COMPANIA, FEC_CARGUE): Delta no permite columnas duplicadas
# al escribir. Se desambiguan sufijando "_n" a partir de la segunda aparicion,
# conservando la primera con su nombre original.

def deduplicar_columnas(df):
    # Delta/Spark tratan los nombres de columna sin distinguir mayuscula de
    # minuscula, asi
    # que la deteccion de duplicados tambien debe ser case-insensitive.
    usados = {c.upper() for c in df.columns}
    vistos = {}
    nuevos = []
    for c in df.columns:
        clave = c.upper()
        if clave not in vistos:
            vistos[clave] = 0
            nuevos.append(c)
        else:
            # El sufijo "_n" generado puede coincidir con una columna que ya
            # existe literalmente en el origen (ej. COD_AGENTE_1 real),
            # asi que se sigue incrementando hasta encontrar un nombre que
            # no este en uso, en vez de asumir que el primer intento es libre.
            vistos[clave] += 1
            candidato = f"{c}_{vistos[clave]}"
            while candidato.upper() in usados:
                vistos[clave] += 1
                candidato = f"{c}_{vistos[clave]}"
            nuevos.append(candidato)
            usados.add(candidato.upper())
    return df.toDF(*nuevos)

def minusculizar_columnas(df):
    return df.toDF(*[c.lower() for c in df.columns])

df_resultado = deduplicar_columnas(build_sat_beyond_health())
df_resultado = df_resultado.withColumn(CONFIG["id_columna_pk"], F.monotonically_increasing_id())
df_resultado = df_resultado.withColumn("dv_load_date", F.lit(LOAD_TS))
df_resultado = df_resultado.withColumn("fecha_creacion", F.to_date(F.lit(LOAD_TS)))
df_resultado = minusculizar_columnas(df_resultado)

df_resultado = _materializar(df_resultado, "bh_resultado_final")

df_resultado.printSchema()

# COMMAND ----------
# Propiedades de la tabla Delta destino (CDF, auto-optimize, compatibilidad
# Iceberg). Se aplican via opciones del writer, sin usar SQL.

TBLPROPERTIES = {
    "delta.enableChangeDataFeed": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.feature.allowColumnDefaults": "supported",
    "delta.enableDeletionVectors": "false",
    "delta.enableIcebergCompatV2": "true",
    "delta.universalFormat.enabledFormats": "iceberg",
    # la tabla es muy ancha; sin esto Delta solo indexa stats de las primeras
    # 32 columnas y fecha_creacion (la columna de clustering) queda afuera.
    "delta.dataSkippingNumIndexedCols": "-1",
}

# COMMAND ----------
# Mantenimiento (no es logica del satelite): se elimina la tabla destino si
# ya existia con Deletion Vectors fisicamente escritos de corridas previas,
# para poder recrearla limpia con IcebergCompatV2 habilitado.

destino = f"`{CONFIG['catalogo_destino']}`.`{CONFIG['esquema_destino']}`.`{CONFIG['tabla_destino']}`"
spark.sql(f"DROP TABLE IF EXISTS {destino}")

# COMMAND ----------
# Escritura a Delta: sobrescribe completo (son pruebas, la tabla destino
# tenia la version vieja por union, con filas repetidas por tabla origen).
# Cluster by fecha_creacion en lugar de particionar.

(
    df_resultado.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .options(**TBLPROPERTIES)
    .clusterBy("fecha_creacion")
    .saveAsTable(destino)
)
# Conteo barato DESPUES del write: lee de la tabla Delta ya escrita,
# no recalcula el pipeline (cero costo adicional de computo).
print("filas sat_beyond_health:", spark.table(destino).count())

CONFIG_SISE = {
    "catalogo_fuente": "axa_col_slv_dv",
    "esquema_fuente": "core_sise",
    "catalogo_destino": "uc_axa_cli",
    "esquema_destino": "silver",
    "tabla_destino": "sat_sise_pyc",
    "id_columna_pk": "id_sat_sise_pyc",
}

TABLAS_SISE = [
    "ss_mpersona",
    "ss_mpersona_dir",
    "ss_mpersona_telef",
    "ss_sg_mpersona_aut_datos",
    "ss_magente",
    "ss_maseg_header",
    "ss_tmunicipio",
    "ss_tpais",
    "ss_tdpto",
    "ss_sv_pv_header",
    "ss_sv_tramo",
    "ss_sv_di_header",
    "ss_sg_pv_header",
    "ss_sg_tramo",
    "ss_sg_di_header",
    "ss_sg_di_benef",
    # "ss_tciiu" excluida a peticion del usuario (pendiente confirmar).
]

# COMMAND ----------
# Lectura generica de tablas SISE (mismo patron que fuente()/cargar_tablas_bh()).

def fuente_sise(tabla_fisica: str):
    catalogo = CONFIG_SISE["catalogo_fuente"]
    esquema = CONFIG_SISE["esquema_fuente"]
    df = spark.table(f"`{catalogo}`.`{esquema}`.`{tabla_fisica}`")
    if MODO_PRUEBA:
        df = df.sample(withReplacement=False, fraction=0.005, seed=42)
    return df


def cargar_tablas_sise() -> dict:
    return {t: fuente_sise(t) for t in TABLAS_SISE}


def unir_por_llave_compuesta(izq, der, columnas: list, alias_der: str, tipo: str = "left", alias_izq: str = None, broadcast: bool = False):
    mapa_der = _mapa_columnas(der)
    der_alias = der.alias(alias_der)
    if broadcast:
        der_alias = F.broadcast(der_alias)
    condicion = None
    for columna in columnas:
        col_izq = izq[f"{alias_izq}.{columna}"] if alias_izq else izq[columna]
        columna_der = mapa_der.get(columna.upper(), columna)
        cond_col = col_izq == der_alias[columna_der]
        condicion = cond_col if condicion is None else (condicion & cond_col)
    unido = izq.join(der_alias, condicion, tipo)
    for columna in columnas:
        columna_der = mapa_der.get(columna.upper(), columna)
        unido = unido.drop(der_alias[columna_der])
    return unido

# COMMAND ----------
# Universo persona: ss_mpersona como base, todo lo que se une por ID_PERSONA
# directo (incluye ss_mpersona_telef SIN deduplicar: si una persona tiene
# varios telefonos, debe quedar una fila por telefono, repitiendo el resto de
# columnas). Luego se le pega a ss_mpersona_dir su puente geografico
# (municipio/pais/dpto) por llave compuesta.

def universo_persona_sise(tablas: dict):
    base = tablas["ss_mpersona"].alias("per")

    df = unir_por_llave_compuesta(base, tablas["ss_mpersona_dir"], ["ID_PERSONA"], "dir")
    df = unir_por_llave_compuesta(df, tablas["ss_mpersona_telef"], ["ID_PERSONA"], "tel")
    df = unir_por_llave_compuesta(df, tablas["ss_sg_mpersona_aut_datos"], ["ID_PERSONA"], "aut")
    df = unir_por_llave_compuesta(df, tablas["ss_magente"], ["ID_PERSONA"], "age")
    df = unir_por_llave_compuesta(df, tablas["ss_maseg_header"], ["ID_PERSONA"], "ase")

    df = _materializar(deduplicar_columnas(df), "sise_per_sin_geo")

    df = unir_por_llave_compuesta(
        df, tablas["ss_tmunicipio"],
        ["COD_MUNICIPIO", "FEC_ACTUALIZACION", "FECHA_CARGUE"], "mun",
        broadcast=True,
    )
    df = deduplicar_columnas(df)
    df = unir_por_llave_compuesta(
        df, tablas["ss_tpais"],
        ["COD_PAIS", "FEC_ACTUALIZACION", "FECHA_CARGUE"], "pai",
        broadcast=True,
    )
    df = deduplicar_columnas(df)
    df = unir_por_llave_compuesta(
        df, tablas["ss_tdpto"],
        ["COD_DPTO", "FEC_MOVIMIENTO", "PERIODO"], "dpt",
        broadcast=True,
    )
    return df

# COMMAND ----------
# Universo poliza: rama SV y rama SG, cada una con su propio sub-join interno
# por COD_RAMO/ID_PV, unidas entre si por UNION con discriminador TIPO_POLIZA
# (mismo patron que PERSONA/INSTITUCION en beyond_health). INFERIDO: el
# usuario no confirmo si SV/SG deben unirse por UNION o por JOIN; se elige
# UNION porque cada poliza es de un solo tipo (SV o SG), nunca ambos a la vez.

def universo_poliza_sise(tablas: dict):
    sv = tablas["ss_sv_pv_header"].alias("sv_pv")
    sv = unir_por_llave_compuesta(sv, tablas["ss_sv_tramo"], ["COD_RAMO"], "sv_tr", broadcast=True)
    sv = unir_por_llave_compuesta(sv, tablas["ss_sv_di_header"], ["ID_PV"], "sv_di")
    sv = sv.withColumn("TIPO_POLIZA", F.lit("SV"))

    sg = tablas["ss_sg_pv_header"].alias("sg_pv")
    sg = unir_por_llave_compuesta(sg, tablas["ss_sg_tramo"], ["COD_RAMO"], "sg_tr", broadcast=True)
    sg = unir_por_llave_compuesta(sg, tablas["ss_sg_di_header"], ["ID_PV"], "sg_di")
    sg = unir_por_llave_compuesta(sg, tablas["ss_sg_di_benef"], ["ID_PV"], "sg_be")
    sg = sg.withColumn("TIPO_POLIZA", F.lit("SG"))

    sv = deduplicar_columnas(sv)
    sg = deduplicar_columnas(sg)

    sv = _materializar(sv, "sise_sv")
    sg = _materializar(sg, "sise_sg")
    return sv.unionByName(sg, allowMissingColumns=True)

# COMMAND ----------
# Construccion final: puente persona <-> poliza por COD_ASEG. INFERIDO del
# precedente del script SQL legado de vida/SISE (no confirmado por el
# usuario); es el primer punto a revisar si los conteos no calzan.

def build_sat_sise_pyc():
    tablas = cargar_tablas_sise()

    personas = _materializar(deduplicar_columnas(universo_persona_sise(tablas)), "sise_personas")
    polizas = _materializar(deduplicar_columnas(universo_poliza_sise(tablas)), "sise_polizas")
    return unir_por_llave_compuesta(personas, polizas, ["COD_ASEG"], "pol")

df_resultado_sise = deduplicar_columnas(build_sat_sise_pyc())
df_resultado_sise = df_resultado_sise.withColumn(
    CONFIG_SISE["id_columna_pk"], F.monotonically_increasing_id()
)
df_resultado_sise = df_resultado_sise.withColumn("dv_load_date", F.lit(LOAD_TS))
df_resultado_sise = df_resultado_sise.withColumn("fecha_creacion", F.to_date(F.lit(LOAD_TS)))
df_resultado_sise = minusculizar_columnas(df_resultado_sise)

if not MODO_PRUEBA:
    df_resultado_sise = df_resultado_sise.repartition(400)
df_resultado_sise = _materializar(df_resultado_sise, "sise_resultado_final")

df_resultado_sise.printSchema()

# COMMAND ----------
# Mantenimiento (no es logica del satelite): se elimina la tabla destino si
# ya existia con Deletion Vectors fisicamente escritos de corridas previas.

destino_sise = f"`{CONFIG_SISE['catalogo_destino']}`.`{CONFIG_SISE['esquema_destino']}`.`{CONFIG_SISE['tabla_destino']}`"
spark.sql(f"DROP TABLE IF EXISTS {destino_sise}")

# COMMAND ----------
# Escritura a Delta: mismo patron de propiedades y liquid clustering que
# sat_beyond_health.

(
    df_resultado_sise.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .options(**TBLPROPERTIES)
    .clusterBy("fecha_creacion")
    .saveAsTable(destino_sise)
)
print("filas sat_sise_pyc:", spark.table(destino_sise).count())
