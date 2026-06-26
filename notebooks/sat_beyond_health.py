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
# y bh_sa_country son catalogos de referencia: se cruzan contra address/city
# por la llave *_NCODE que tengan en comun, tambien detectada dinamicamente.

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
    "bh_sa_country",
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

def llave_entidad(df):
    cols = set(df.columns)
    tiene_per = "PER_NCODE" in cols
    tiene_ins = "INS_NCODE" in cols
    if tiene_per and tiene_ins:
        return F.coalesce(F.col("PER_NCODE").cast("string"), F.col("INS_NCODE").cast("string"))
    if tiene_per:
        return F.col("PER_NCODE").cast("string")
    if tiene_ins:
        return F.col("INS_NCODE").cast("string")
    return None

# COMMAND ----------
# Llave comun entre dos tablas de catalogo (cualquier columna *_NCODE que
# exista en ambas), para no quemar el nombre puntual de columna.

def llave_comun(izq, der):
    comunes = [c for c in izq.columns if c in der.columns and c.endswith("NCODE")]
    if not comunes:
        raise ValueError("No se encontro llave comun (*_NCODE) entre las dos tablas")
    return comunes[0]

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
# Join generico de una tabla "satelite de entidad" (address, affiliation_contract,
# member) contra el universo base, por ID_ENTIDAD_HUB.

def unir_por_entidad(base, tabla, alias: str):
    llave = llave_entidad(tabla)
    if llave is None:
        raise ValueError(f"{alias}: no tiene PER_NCODE ni INS_NCODE, no se puede unir por entidad")
    tabla_alias = tabla.withColumn("ID_ENTIDAD_HUB", llave).alias(alias)
    return base.join(
        tabla_alias,
        base["ID_ENTIDAD_HUB"] == tabla_alias["ID_ENTIDAD_HUB"],
        "left",
    )

# COMMAND ----------
# Construccion del "big dataframe" de sat_beyond_health.

def build_sat_beyond_health():
    tablas = cargar_tablas_bh()

    base = base_entidades(tablas)
    df = unir_por_entidad(base, tablas["bh_sa_address"], "addr")
    df = unir_por_entidad(df, tablas["bh_sa_affiliation_contract"], "aco")
    df = unir_por_entidad(df, tablas["bh_sa_member"], "mem")

    ciu = tablas["bh_sa_city"]
    pai = tablas["bh_sa_country"]
    llave_addr_ciu = llave_comun(tablas["bh_sa_address"], ciu)
    llave_ciu_pai = llave_comun(ciu, pai)

    df = df.join(
        ciu.alias("ciu"),
        df[f"addr.{llave_addr_ciu}"] == F.col(f"ciu.{llave_addr_ciu}"),
        "left",
    )
    df = df.join(
        pai.alias("pai"),
        F.col(f"ciu.{llave_ciu_pai}") == F.col(f"pai.{llave_ciu_pai}"),
        "left",
    )
    return df

# COMMAND ----------
# Ejecucion: solo construye y muestra conteo/esquema (todavia no escribe a
# Delta — eso se agrega cuando se valide que el join cruza correctamente).

df_resultado = build_sat_beyond_health()
print("filas:", df_resultado.count())
df_resultado.printSchema()
