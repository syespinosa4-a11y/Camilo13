# Databricks notebook source
# sat_beyond_health — primera version funcional (PySpark puro, sin SQL).
#
# DIM_PARAMETROS todavia no existe como tabla Delta (esta reservada para
# nuevos satelites y homologaciones a futuro). Mientras tanto, la
# configuracion de catalogo/esquema queda centralizada en CONFIG: es el
# UNICO lugar a editar cuando DIM_PARAMETROS exista (solo hay que cambiar
# como se llena CONFIG, el resto del script no cambia).
#
# Estructura: join en estrella. bh_sa_address, bh_sa_affiliation_contract y
# bh_sa_member se conectan directo contra la entidad (persona o institucion)
# usando PER_NCODE o INS_NCODE — la que exista en cada tabla, detectada
# dinamicamente por columnas (nada de "if tabla == 'x'" quemado). bh_sa_city
# es catalogo de referencia: se cruza contra address por la llave *_NCODE
# que tengan en comun, tambien detectada dinamicamente. bh_sa_country no se
# incluye: no existe tabla de departamento que conecte city con country.

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from datetime import datetime, timezone

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# Configuracion (unico punto a editar; futuro reemplazo = leer de DIM_PARAMETROS)

CONFIG = {
    "catalogo_fuente": "axa_col_dv",
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
    return spark.table(f"`{catalogo}`.`{esquema}`.`{tabla_fisica}`")


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

# COMMAND ----------
# Llave comun entre dos tablas de catalogo (cualquier columna *_NCODE que
# exista en ambas, sin importar mayuscula/minuscula), para no quemar el
# nombre puntual de columna. Devuelve el nombre real de cada lado, porque
# el casing puede diferir entre tablas (ej. ACO_NCODE vs aco_ncode).

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
    # se descarta el ID_ENTIDAD_HUB de tabla_alias: queda duplicado con el de
    # base y vuelve ambigua cualquier referencia siguiente a la columna.
    return unido.drop(tabla_alias["ID_ENTIDAD_HUB"])

# COMMAND ----------
# Join por puente: para tablas que no tienen PER_NCODE/INS_NCODE directo
# (ej. bh_sa_member), se cruzan contra otra tabla ya unida (ej. affiliation_contract)
# usando la llave *_NCODE que tengan en comun (detectada dinamicamente, no quemada).

def unir_por_puente(df, alias_referencia: str, tabla_referencia_original, tabla_nueva, alias_nueva: str):
    llave_izq, llave_der = llave_comun(tabla_referencia_original, tabla_nueva)
    tabla_alias = tabla_nueva.alias(alias_nueva)
    return df.join(
        tabla_alias,
        df[f"{alias_referencia}.{llave_izq}"] == tabla_alias[llave_der],
        "left",
    )

# COMMAND ----------
# Construccion del "big dataframe" de sat_beyond_health.

def build_sat_beyond_health():
    tablas = cargar_tablas_bh()

    base = base_entidades(tablas)
    df = unir_por_entidad(base, tablas["bh_sa_address"], "addr")
    df = unir_por_entidad(df, tablas["bh_sa_affiliation_contract"], "aco")
    df = unir_por_puente(df, "aco", tablas["bh_sa_affiliation_contract"], tablas["bh_sa_member"], "mem")

    ciu = tablas["bh_sa_city"]
    llave_addr, llave_ciu_addr = llave_comun(tablas["bh_sa_address"], ciu)

    df = df.join(
        ciu.alias("ciu"),
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
    # minuscula (ACO_NCODE y aco_ncode son la misma columna al guardar), asi
    # que la deteccion de duplicados tambien debe ser case-insensitive.
    vistos = {}
    nuevos = []
    for c in df.columns:
        clave = c.upper()
        if clave not in vistos:
            vistos[clave] = 0
            nuevos.append(c)
        else:
            vistos[clave] += 1
            nuevos.append(f"{c}_{vistos[clave]}")
    return df.toDF(*nuevos)

# COMMAND ----------
# Construccion final: una fila por entidad (no una fila por tabla origen),
# con id y fecha de carga, lista para sobrescribir el destino.

df_resultado = deduplicar_columnas(build_sat_beyond_health())
df_resultado = df_resultado.withColumn(CONFIG["id_columna_pk"], F.monotonically_increasing_id())
df_resultado = df_resultado.withColumn("dv_load_date", F.lit(LOAD_TS))
df_resultado = df_resultado.withColumn("fecha_creacion", F.to_date(F.lit(LOAD_TS)))

print("filas:", df_resultado.count())
df_resultado.printSchema()

# COMMAND ----------
# Propiedades de la tabla Delta destino (CDF, auto-optimize, compatibilidad
# Iceberg). Se aplican via opciones del writer, sin usar SQL.

TBLPROPERTIES = {
    "delta.enableChangeDataFeed": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.feature.allowColumnDefaults": "supported",
    # IcebergCompatV2 exige Deletion Vectors deshabilitados.
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
