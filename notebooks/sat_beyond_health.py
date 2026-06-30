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

# AQF (Adaptive Query Framework): ajusta el plan en tiempo de ejecucion
# segun estadisticas reales (particiones, skew, broadcast dinamico).
# Suele estar activo en Databricks >= 10.x, pero se fuerza para garantizar
# que aplica sin importar la config del cluster.
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# Directorio de checkpoints: trunca el linaje del DAG en puntos clave
# para que Spark no recalcule todo el pipeline desde cero en cada accion.
spark.sparkContext.setCheckpointDir("dbfs:/tmp/checkpoints/satelites/")

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

def unir_por_puente(df, alias_referencia: str, tabla_referencia_original, tabla_nueva, alias_nueva: str, broadcast: bool = False):
    llave_izq, llave_der = llave_comun(tabla_referencia_original, tabla_nueva)
    tabla_alias = tabla_nueva.alias(alias_nueva)
    if broadcast:
        # bh_sa_member es una tabla de afiliacion chica frente al universo de
        # entidades; forzar broadcast evita un shuffle costoso de ambos lados
        # cuando Unity Catalog no tiene stats que permitan a Spark decidirlo
        # solo (broadcast join automatico).
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

    # Checkpoint tras la union PERSONA/INSTITUCION: materializa el universo
    # base en disco y trunca el linaje, para que los joins siguientes no
    # arrastren el plan de la union en cada etapa.
    base = base_entidades(tablas).checkpoint()
    df = unir_por_entidad(base, tablas["bh_sa_address"], "addr")
    df = unir_por_entidad(df, tablas["bh_sa_affiliation_contract"], "aco")
    df = unir_por_puente(df, "aco", tablas["bh_sa_affiliation_contract"], tablas["bh_sa_member"], "mem", broadcast=True)

    # bh_sa_city es catalogo de referencia (pocas filas): se fuerza broadcast
    # para evitar un shuffle innecesario contra el universo ya unido, que es
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
    # minuscula (ACO_NCODE y aco_ncode son la misma columna al guardar), asi
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

# COMMAND ----------
# Resultado final en minuscula sostenida: se aplica como ultimo paso, despues
# de deduplicar y de agregar el id/fechas, para que TODAS las columnas del
# satelite (de cualquier tabla origen) queden homogeneas sin importar el
# casing original de cada fuente.

def minusculizar_columnas(df):
    return df.toDF(*[c.lower() for c in df.columns])

# COMMAND ----------
# Construccion final: una fila por entidad (no una fila por tabla origen),
# con id y fecha de carga, lista para sobrescribir el destino.

df_resultado = deduplicar_columnas(build_sat_beyond_health())
df_resultado = df_resultado.withColumn(CONFIG["id_columna_pk"], F.monotonically_increasing_id())
df_resultado = df_resultado.withColumn("dv_load_date", F.lit(LOAD_TS))
df_resultado = df_resultado.withColumn("fecha_creacion", F.to_date(F.lit(LOAD_TS)))
df_resultado = minusculizar_columnas(df_resultado)

# Checkpoint final: trunca el linaje acumulado por deduplicar + withColumns.
# A diferencia de cache(), escribe a disco (no falla si el cluster no tiene
# RAM suficiente) y garantiza que el write Delta no recalcula desde cero.
df_resultado = df_resultado.checkpoint()

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
# Conteo barato DESPUES del write: lee de la tabla Delta ya escrita,
# no recalcula el pipeline (cero costo adicional de computo).
print("filas sat_beyond_health:", spark.table(destino).count())

# COMMAND ----------
# ============================================================
# sat_sise_pyc — segundo satelite en este mismo scrip (mismas
# caracteristicas que sat_beyond_health: CONFIG centralizado,
# PySpark puro sin SQL salvo el DROP TABLE de mantenimiento,
# llaves dinamicas en lo posible).
#
# NOTA IMPORTANTE (pendiente de validar con datos reales):
# SISE no usa el patron *_NCODE de beyond_health, las llaves
# vienen explicitas por columna segun lo indicado. Dos puentes
# fueron INFERIDOS por mi (no confirmados por el usuario), y
# deben revisarse contra resultados reales en Databricks:
#   1) Puente persona <-> poliza: se usa COD_ASEG (precedente
#      del script SQL legado de vida/SISE).
#   2) SV y SG se combinan por UNION (unionByName con
#      allowMissingColumns=True) con un discriminador
#      TIPO_POLIZA in ('SV','SG'), igual patron que
#      TIPO_ENTIDAD en beyond_health (persona/institucion).
# Si estas dos inferencias no calzan con la realidad de los
# datos, son los dos puntos a corregir primero.
#
# ss_tciiu queda EXCLUIDA por instruccion explicita del usuario
# (pendiente de confirmacion del equipo tecnico).
# ============================================================

CONFIG_SISE = {
    "catalogo_fuente": "axa_col_dv",
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
    return spark.table(f"`{catalogo}`.`{esquema}`.`{tabla_fisica}`")


def cargar_tablas_sise() -> dict:
    return {t: fuente_sise(t) for t in TABLAS_SISE}

# COMMAND ----------
# Join por llave compuesta explicita: ANDea igualdad de una lista de columnas
# (mismo nombre en ambos lados, segun lo indicado por el usuario). Se descartan
# las columnas de la llave del lado derecho tras el join, para no dejar
# columnas duplicadas que vuelvan ambigua cualquier referencia siguiente
# (mismo problema y misma solucion que en unir_por_entidad).

def unir_por_llave_compuesta(izq, der, columnas: list, alias_der: str, tipo: str = "left", alias_izq: str = None, broadcast: bool = False):
    # El nombre real de cada columna se resuelve case-insensitive (mismo
    # motivo que en llave_comun): las tablas SISE no garantizan la misma
    # mayuscula/minuscula para una misma columna logica entre dos tablas
    # (ej. COD_ASEG vs cod_aseg), y comparar/dropear por el nombre literal
    # tal cual se escribio en la lista deja columnas duplicadas que Spark
    # no logra resolver mas adelante ([COLUMN_ALREADY_EXISTS]).
    mapa_der = _mapa_columnas(der)
    der_alias = der.alias(alias_der)
    if broadcast:
        # Catalogos de referencia (municipio, pais, dpto, tramo): muchas
        # menos filas que el universo persona/poliza ya unido, forzar
        # broadcast evita un shuffle costoso de ambos lados.
        der_alias = F.broadcast(der_alias)
    condicion = None
    for columna in columnas:
        # alias_izq se usa cuando el dataframe izquierdo ya acumulo varias
        # tablas con columnas del mismo nombre (ej. FEC_ACTUALIZACION en mas
        # de una tabla origen): hay que anclar a que alias especifico
        # pertenece la columna del puente, si no la referencia es ambigua.
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

    # Puente geografico de ss_mpersona_dir: cada catalogo se une por su propia
    # llave compuesta indicada por el usuario.
    df = unir_por_llave_compuesta(
        df, tablas["ss_tmunicipio"],
        ["COD_MUNICIPIO", "FEC_ACTUALIZACION", "FECHA_CARGUE"], "mun",
        alias_izq="dir", broadcast=True,
    )
    df = unir_por_llave_compuesta(
        df, tablas["ss_tpais"],
        ["COD_PAIS", "FEC_ACTUALIZACION", "FECHA_CARGUE"], "pai",
        alias_izq="dir", broadcast=True,
    )
    df = unir_por_llave_compuesta(
        df, tablas["ss_tdpto"],
        ["COD_DPTO", "FEC_MOVIMIENTO", "PERIODO"], "dpt",
        alias_izq="dir", broadcast=True,
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

    # Se dedupica cada rama ANTES del union: dentro de sv y de sg se
    # acumulan columnas no-llave duplicadas (ej. COD_ASEG presente en mas
    # de una tabla de la misma rama), y unionByName falla al alinear
    # esquemas si alguna rama trae nombres repetidos.
    sv = deduplicar_columnas(sv)
    sg = deduplicar_columnas(sg)
    return sv.unionByName(sg, allowMissingColumns=True)

# COMMAND ----------
# Construccion final: puente persona <-> poliza por COD_ASEG. INFERIDO del
# precedente del script SQL legado de vida/SISE (no confirmado por el
# usuario); es el primer punto a revisar si los conteos no calzan.

def build_sat_sise_pyc():
    tablas = cargar_tablas_sise()
    # Checkpoint de cada universo por separado: materializa personas en disco
    # ANTES de computar polizas, y viceversa. Esto impide que Spark construya
    # un plan gigante (personas * polizas en un solo DAG) y le permite a AQF
    # optimizar cada etapa con estadisticas reales. Sin checkpoint, el join
    # final arrastra todo el linaje de ambos universos y el plan se vuelve
    # inmanejable para tablas de millones de filas.
    personas = deduplicar_columnas(universo_persona_sise(tablas)).checkpoint()
    polizas = deduplicar_columnas(universo_poliza_sise(tablas)).checkpoint()
    return unir_por_llave_compuesta(personas, polizas, ["COD_ASEG"], "pol")

# COMMAND ----------
# Construccion final: una fila por (persona, telefono, poliza) segun los joins
# anteriores, con id y fecha de carga, lista para sobrescribir el destino.
# Reusa deduplicar_columnas/TBLPROPERTIES/LOAD_TS definidos para beyond_health.

df_resultado_sise = deduplicar_columnas(build_sat_sise_pyc())
df_resultado_sise = df_resultado_sise.withColumn(
    CONFIG_SISE["id_columna_pk"], F.monotonically_increasing_id()
)
df_resultado_sise = df_resultado_sise.withColumn("dv_load_date", F.lit(LOAD_TS))
df_resultado_sise = df_resultado_sise.withColumn("fecha_creacion", F.to_date(F.lit(LOAD_TS)))
df_resultado_sise = minusculizar_columnas(df_resultado_sise)
df_resultado_sise = df_resultado_sise.checkpoint()

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
