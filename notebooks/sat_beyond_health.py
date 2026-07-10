# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from collections import defaultdict

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

TABLAS_BH = [
    "bh_sa_person",
    "bh_sa_address",
    "bh_sa_institution",
    "bh_sa_city",
    "bh_sa_affiliation_contract",
    "bh_sa_member",
]

# Prefijo corto por tabla: se usa para renombrar columnas de datos duplicadas
PREFIJOS_TABLA = {
    "bh_sa_member":               "mem",
    "bh_sa_affiliation_contract": "aco",
    "bh_sa_person":               "per",
    "bh_sa_institution":          "ins",
    "bh_sa_address":              "add",
    "bh_sa_city":                 "cit",
}

# Columnas que son llaves de join y tienen el mismo nombre en mas de una tabla.
# Se renombran ANTES del join para que PySpark no genere ambiguedad.
# Formato: { nombre_tabla: { col_original: col_nueva } }
RENOMBRES = {
    "bh_sa_affiliation_contract": {
        "per_ncode": "aco_per_ncode",   # titular viene del contrato
        "ins_ncode": "aco_ins_ncode",   # institucion del contrato
    },
    "bh_sa_address": {
        "per_ncode": "add_per_ncode",   # llave hacia persona en address
        "cit_ncode": "add_cit_ncode",   # llave hacia city en address
    },
    "bh_sa_member": {
        "per_ncode": "mem_per_ncode",   # beneficiario viene del member
        "ins_ncode": "mem_ins_ncode",   # ins_ncode de member choca con institution.ins_ncode
    },
}

# Llaves de join entre tablas (izquierda, derecha)
LLAVES = {
    # join comun a ambas ramas
    "member_contrato":          ("aco_ncode",      "aco_ncode"),
    "residencial_ciudad":       ("ciu_res_codigo",  "ciu_res_codigo"),
    # rama TITULAR: persona e institucion vienen del contrato
    "titular_persona":          ("aco_per_ncode",  "per_ncode"),
    "titular_institucion":      ("aco_ins_ncode",  "ins_ncode"),
    "titular_residencial":      ("aco_per_ncode",  "res_per_ncode"),
    # rama BENEFICIARIO: persona viene del member (mem_per_ncode tras renombre)
    "beneficiario_persona":     ("mem_per_ncode",  "per_ncode"),
    "beneficiario_institucion": ("aco_ins_ncode",  "ins_ncode"),
    "beneficiario_residencial": ("mem_per_ncode",  "res_per_ncode"),
}

# Parametros del pre-agregado residencial
RESIDENCIAL = {
    "filtro_col":        "lty_ncode",
    "filtro_val":        1,
    "llave_persona":     "add_per_ncode",
    "col_direccion":     "add_caddress",
    "join_ciudad_llave": ("add_cit_ncode", "cit_ncode"),
    "col_ciudad_codigo": "cit_clegalcode",
    "col_ciudad_nombre": "cit_cname",
    "alias_dir":         "dir_res",
    "alias_ciu_codigo":  "ciu_res_codigo",
    "alias_ciu_nombre":  "ciudad_residencia",
    "alias_llave":       "res_per_ncode",
}

# COMMAND ----------
# Helpers

def fuente(tabla: str):
    cat = CONFIG["catalogo_fuente"]
    esq = CONFIG["esquema_fuente"]
    return spark.table(f"`{cat}`.`{esq}`.`{tabla}`")


def aplicar_renombres(df, nombre_tabla: str):
    """Renombra las llaves de join conflictivas segun RENOMBRES."""
    for col_orig, col_nueva in RENOMBRES.get(nombre_tabla, {}).items():
        df = df.withColumnRenamed(col_orig, col_nueva)
    return df


def dedup_data_cols(tbls: dict) -> dict:
    """
    Detecta columnas de DATOS (no llaves) que aparecen en mas de una tabla
    y las renombra con el prefijo de la tabla para evitar COLUMN_ALREADY_EXISTS
    en el join wide. Las columnas ya tratadas por RENOMBRES o usadas en LLAVES
    se excluyen automaticamente.
    """
    # columnas ya gestionadas: no tocar
    gestionadas = set()
    for renames in RENOMBRES.values():
        gestionadas.update(v.lower() for v in renames.values())
    for izq, der in LLAVES.values():
        gestionadas.add(izq.lower())
        gestionadas.add(der.lower())
    gestionadas.add(RESIDENCIAL["alias_llave"].lower())

    # mapear columna -> lista de (nombre_tabla, nombre_exacto_col)
    col_a_tablas = defaultdict(list)
    for nombre, df in tbls.items():
        for col in df.columns:
            if col.lower() not in gestionadas:
                col_a_tablas[col.lower()].append((nombre, col))

    # renombrar solo las que aparecen en mas de una tabla
    duplicadas = {col: ocurr for col, ocurr in col_a_tablas.items() if len(ocurr) > 1}
    result = dict(tbls)
    for col_lower, ocurrencias in duplicadas.items():
        for nombre_tabla, col_exact in ocurrencias:
            prefijo = PREFIJOS_TABLA.get(nombre_tabla, nombre_tabla[:3])
            nuevo = f"{prefijo}_{col_exact}"
            result[nombre_tabla] = result[nombre_tabla].withColumnRenamed(col_exact, nuevo)

    if duplicadas:
        print("Columnas de datos renombradas automaticamente por duplicado:")
        for col, ocurr in duplicadas.items():
            tablas = [t for t, _ in ocurr]
            print(f"  '{col}' encontrada en: {tablas}")

    return result


def llave(nombre: str):
    return LLAVES[nombre]

# COMMAND ----------
# Lectura, desambiguacion de llaves y dedup de columnas de datos

tbls = {}
for t in TABLAS_BH:
    tbls[t] = aplicar_renombres(fuente(t), t)

tbls = dedup_data_cols(tbls)

# Alias por tabla: permite referenciar columnas con F.col("alias.col")
# en joins encadenados sin que el optimizer de PySpark pierda columnas.
member               = tbls["bh_sa_member"].alias("mem")
affiliation_contract = tbls["bh_sa_affiliation_contract"].alias("aco")
person               = tbls["bh_sa_person"].alias("per")
institution          = tbls["bh_sa_institution"].alias("ins")
address              = tbls["bh_sa_address"].alias("add")
city                 = tbls["bh_sa_city"].alias("cit")

# COMMAND ----------
# Pre-agregado: residencial
# bh_sa_address (lty_ncode = 1) + bh_sa_city agrupado por per_ncode.
# Produce una fila por persona con su direccion y codigo de ciudad.

R = RESIDENCIAL
_add_llave_izq, _add_llave_der = R["join_ciudad_llave"]

df_residencial = (
    address.filter(F.col(R["filtro_col"]) == R["filtro_val"])
    .join(
        city.select(F.col(_add_llave_der), F.col(R["col_ciudad_codigo"])),
        F.col(f"add.{_add_llave_izq}") == F.col(f"cit.{_add_llave_der}"),
        how="left",
    )
    .groupBy(f"add.{R['llave_persona']}")
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
# Lista de columnas a seleccionar de cada tabla para el join wide.
# Se excluye aco_ncode de affiliation_contract (ya viene de member via on= string join).
# Las llaves de condicion (per_ncode, ins_ncode) se toman de la tabla duena (person, institution).

_l = llave

_llave_join_str = _l("member_contrato")[0]   # "aco_ncode"

# Construir el select como dict ordenado: primer alias gana, duplicados se omiten.
# Esto es robusto frente a cualquier colision que dedup_data_cols no haya anticipado.
def _build_select(tabla_alias_pares, extras):
    seen = {}
    for alias_tabla, cols in tabla_alias_pares:
        for c in cols:
            if c not in seen:
                seen[c] = F.col(f"{alias_tabla}.{c}").alias(c)
    result = list(seen.values())
    result.extend(extras)
    return result

_tabla_cols = [
    ("mem", tbls["bh_sa_member"].columns),
    ("aco", tbls["bh_sa_affiliation_contract"].columns),
    ("per", tbls["bh_sa_person"].columns),
    ("ins", tbls["bh_sa_institution"].columns),
]
_extras = [
    F.col(R["alias_dir"]),
    F.col(R["alias_ciu_codigo"]),
    F.col(R["alias_ciu_nombre"]),
]

select_cols_titular     = _build_select(_tabla_cols, _extras + [F.lit("TITULAR").alias("rol")])
select_cols_beneficiario = _build_select(_tabla_cols, _extras + [F.lit("BENEFICIARIO").alias("rol")])

# COMMAND ----------
# RAMA TITULAR
# En este fragmento, se crea un DataFrame df_titular que contiene la informacion de los titulares.
# Se une la tabla member con la tabla affiliation_contract para obtener el codigo de la afiliacion
# y el codigo de la persona titular. Luego, se une con la tabla person para obtener la informacion
# de la persona titular. Tambien se une con la tabla institution para obtener la informacion de la
# institucion. Finalmente, se une con los DataFrames df_residencial y df_ciudad_nombre para obtener
# la direccion residencial y el nombre de la ciudad.

df_titular = (
    member
    .join(affiliation_contract,
          on=_llave_join_str,
          how="inner")
    .join(person,
          F.col(f"aco.{_l('titular_persona')[0]}") == F.col(f"per.{_l('titular_persona')[1]}"),
          how="left")
    .join(institution,
          F.col(f"aco.{_l('titular_institucion')[0]}") == F.col(f"ins.{_l('titular_institucion')[1]}"),
          how="left")
    .join(df_residencial.alias("res"),
          F.col(f"aco.{_l('titular_residencial')[0]}") == F.col(f"res.{_l('titular_residencial')[1]}"),
          how="left")
    .join(df_ciudad_nombre.alias("ciudad"),
          on=_l("residencial_ciudad")[0],
          how="left")
    .select(*select_cols_titular)
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
          on=_llave_join_str,
          how="inner")
    .join(person,
          F.col(f"mem.{_l('beneficiario_persona')[0]}") == F.col(f"per.{_l('beneficiario_persona')[1]}"),
          how="left")
    .join(institution,
          F.col(f"aco.{_l('beneficiario_institucion')[0]}") == F.col(f"ins.{_l('beneficiario_institucion')[1]}"),
          how="left")
    .join(df_residencial.alias("res"),
          F.col(f"mem.{_l('beneficiario_residencial')[0]}") == F.col(f"res.{_l('beneficiario_residencial')[1]}"),
          how="left")
    .join(df_ciudad_nombre.alias("ciudad"),
          on=_l("residencial_ciudad")[0],
          how="left")
    .select(*select_cols_beneficiario)
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
