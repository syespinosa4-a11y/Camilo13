# Databricks notebook source
# 01_setup — AXA Clientes 360
#
# Crea el catálogo, los schemas, el volumen y todas las tablas Delta
# del pipeline. Ejecutar una sola vez por entorno (DEV / QA / PRD).
#
# Estructura de tablas por schema:
#   bronze    : parametros, reglas_calidad, satelites
#   silver    : dim_parametros, dim_regla_calidad, dim_tiempo, hub_cliente
#   gold      : dim_parametros, dim_reglas_calidad, dim_tiempo,
#               fact_reporte_calidad, fact_resumen_reporte_calidad, hub_cliente
#   auditoria : log_ejecuciones, log_reglas_calidad

# COMMAND ----------
# %run ../utils/00_utils

# COMMAND ----------

import json
print("[OK] 01_setup iniciado")

# COMMAND ----------
# MAGIC ## 1. Parámetros

# COMMAND ----------

CATALOGO         = "uc_axa_cli"
MANAGED_LOCATION = "abfss://unity-catalog@zacodatainlakedvue2sto01.dfs.core.windows.net/uc_axa_cli"

SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD   = "gold"
SCHEMA_AUDIT  = "auditoria"

VOLUMEN = "vl_axa_cli"

TBLPROPERTIES = """
    TBLPROPERTIES (
        'delta.enableChangeDataFeed'           = 'true',
        'delta.autoOptimize.optimizeWrite'     = 'true',
        'delta.feature.allowColumnDefaults'    = 'supported',
        'delta.enableIcebergCompatV2'          = 'true',
        'delta.universalFormat.enabledFormats' = 'iceberg'
    )
"""

schemas = {
    SCHEMA_BRONZE : "Capa de ingesta — archivos Excel crudos",
    SCHEMA_SILVER : "Capa de transformación — dimensiones y hub Data Vault",
    SCHEMA_GOLD   : "Capa analítica — dimensiones y hechos",
    SCHEMA_AUDIT  : "Logs de ejecución y calidad",
}

log_seccion("PARÁMETROS")
print(f"  catálogo         : {CATALOGO}")
print(f"  managed location : {MANAGED_LOCATION}")
print(f"  volumen          : {VOLUMEN}")

# COMMAND ----------
# MAGIC ## 2. Catálogo

# COMMAND ----------

# Descomentar la primera vez que se cree el catálogo en el entorno.
# En ejecuciones siguientes dejar comentado para no sobreescribir permisos.
# log_seccion("CATÁLOGO")
# spark.sql(f"""
#     CREATE CATALOG IF NOT EXISTS {CATALOGO}
#     MANAGED LOCATION '{MANAGED_LOCATION}'
#     COMMENT 'Catálogo Unity Catalog para el proyecto AXA Clientes 360'
# """)
# print(f"  [OK] Catálogo {CATALOGO}")

# COMMAND ----------
# MAGIC ## 3. Schemas

# COMMAND ----------

log_seccion("SCHEMAS")
for schema, comentario in schemas.items():
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{schema}
        MANAGED LOCATION '{MANAGED_LOCATION}/{schema}/tablas'
        COMMENT '{comentario}'
    """)
    print(f"  [OK] {CATALOGO}.{schema}")

# COMMAND ----------
# MAGIC ## 4. Volumen Bronze

# COMMAND ----------

log_seccion("VOLUMEN BRONZE")
spark.sql(f"""
    CREATE EXTERNAL VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA_BRONZE}.{VOLUMEN}
    LOCATION '{MANAGED_LOCATION}/{SCHEMA_BRONZE}/volumen'
    COMMENT 'Volumen para archivos Excel del pipeline de calidad'
""")

for sub in ["nuevo", "exitoso", "error"]:
    dbutils.fs.mkdirs(f"{MANAGED_LOCATION}/{SCHEMA_BRONZE}/volumen/{sub}")
    print(f"  [OK] subcarpeta volumen/{sub}")

print(f"  [OK] Volumen {VOLUMEN}")

# COMMAND ----------
# MAGIC ## 5. Tablas — Bronze

# COMMAND ----------

log_seccion("TABLAS BRONZE")

# bronze.parametros — hoja de parámetros del Excel
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOGO}.{SCHEMA_BRONZE}.parametros (
        grupo_parametros  STRING    COMMENT 'Grupo funcional: homologacion_tipo_doc, homologacion_genero, umbral_calidad',
        nombre            STRING    COMMENT 'Nombre del parámetro; con grupo_parametros forma la llave natural',
        descripcion       STRING    COMMENT 'Descripción legible del parámetro',
        valor             STRING    COMMENT 'Valor configurado',
        valor_homologado  STRING    COMMENT 'Valor normalizado para homologaciones',
        columnas_llave    STRING    COMMENT 'Columnas llave separadas por coma (para grupo=satelite)',
        estado            STRING    COMMENT 'Estado: activo o inactivo',
        fecha_carga       TIMESTAMP COMMENT 'Timestamp de carga desde el archivo Excel'
    )
    USING DELTA
    COMMENT 'Parámetros de configuración del pipeline — Bronze'
    {TBLPROPERTIES}
""")
print("  [OK] bronze.parametros")

# bronze.reglas_calidad — hoja de reglas del Excel
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOGO}.{SCHEMA_BRONZE}.reglas_calidad (
        tabla             STRING    COMMENT 'Satélite objetivo; * aplica a todos',
        atributo          STRING    COMMENT 'Campo del satélite que se evalúa',
        regla             STRING    COMMENT 'Expresión SQL booleana evaluada con F.expr()',
        descripcion_regla STRING    COMMENT 'Descripción legible de la regla',
        grupo_regla       STRING    COMMENT 'Dimensión de calidad: completitud, formato, unicidad, consistencia',
        prioridad_regla   INT       COMMENT 'Prioridad: 1=alta, 2=media, 3=baja',
        estado_regla      STRING    COMMENT 'Estado: activo o inactivo',
        fecha_carga       TIMESTAMP COMMENT 'Timestamp de carga desde el archivo Excel'
    )
    USING DELTA
    COMMENT 'Reglas de calidad de datos — Bronze'
    {TBLPROPERTIES}
""")
print("  [OK] bronze.reglas_calidad")

# bronze.satelites — hoja de satélites del Excel
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOGO}.{SCHEMA_BRONZE}.satelites (
        nombre          STRING    COMMENT 'Nombre del satélite (ej: asegurados, polizas)',
        descripcion     STRING    COMMENT 'Descripción del satélite',
        expresion_sql   STRING    COMMENT 'Query SQL parametrizado que extrae los registros del satélite',
        columnas_llave  STRING    COMMENT 'Columnas llave del satélite separadas por coma',
        estado          STRING    COMMENT 'Estado: activo o inactivo',
        fecha_carga     TIMESTAMP COMMENT 'Timestamp de carga desde el archivo Excel'
    )
    USING DELTA
    COMMENT 'Configuración de satélites del pipeline — Bronze'
    {TBLPROPERTIES}
""")
print("  [OK] bronze.satelites")

# COMMAND ----------
# MAGIC ## 6. Tablas — Auditoría

# COMMAND ----------

log_seccion("TABLAS AUDITORÍA")

# auditoria.log_ejecuciones — traza de cada notebook del pipeline
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOGO}.{SCHEMA_AUDIT}.log_ejecuciones (
        id_log          BIGINT    NOT NULL GENERATED ALWAYS AS IDENTITY COMMENT 'Identificador autoincremental del evento',
        id_ejecucion    STRING    NOT NULL COMMENT 'UUID del pipeline que generó el evento',
        fecha_ejecucion TIMESTAMP          COMMENT 'Timestamp del inicio de la ejecución del proceso',
        esquema         STRING             COMMENT 'Schema de la tabla procesada',
        tabla           STRING             COMMENT 'Tabla procesada',
        proceso         STRING    NOT NULL COMMENT 'Nombre del proceso o notebook ejecutado',
        estado          STRING    NOT NULL COMMENT 'Estado del proceso: exitoso o error',
        detalle_error   STRING             COMMENT 'Mensaje de error; NULL cuando el estado es exitoso',
        fecha_creacion  TIMESTAMP          COMMENT 'Timestamp de inserción del registro de log'
    )
    USING DELTA
    COMMENT 'Log de ejecuciones del pipeline — Auditoría'
    {TBLPROPERTIES}
""")
print("  [OK] auditoria.log_ejecuciones")

# auditoria.log_reglas_calidad — detalle de reglas evaluadas
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOGO}.{SCHEMA_AUDIT}.log_reglas_calidad (
        id_log          BIGINT    NOT NULL GENERATED ALWAYS AS IDENTITY COMMENT 'Identificador autoincremental del evento',
        id_ejecucion    STRING    NOT NULL COMMENT 'UUID del pipeline que generó el evento',
        fecha_ejecucion TIMESTAMP          COMMENT 'Timestamp del inicio de la evaluación',
        esquema         STRING             COMMENT 'Schema de la tabla evaluada',
        tabla           STRING             COMMENT 'Tabla evaluada',
        regla           STRING             COMMENT 'Expresión SQL booleana evaluada',
        estado          STRING             COMMENT 'Resultado de la evaluación: exitoso o fallido',
        detalle_error   STRING             COMMENT 'Detalle del hallazgo; NULL cuando el resultado es exitoso',
        fecha_creacion  TIMESTAMP          COMMENT 'Timestamp de inserción del registro de log'
    )
    USING DELTA
    COMMENT 'Log de evaluación de reglas de calidad — Auditoría'
    {TBLPROPERTIES}
""")
print("  [OK] auditoria.log_reglas_calidad")

# COMMAND ----------
# MAGIC ## 7. Tablas — Silver

# COMMAND ----------

log_seccion("TABLAS SILVER")

# silver.dim_parametros — SCD1; llave natural (grupo_parametros, nombre)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOGO}.{SCHEMA_SILVER}.dim_parametros (
        pk_parametro        BIGINT    NOT NULL COMMENT 'Surrogate key generada con monotonically_increasing_id()',
        grupo_parametros    STRING    NOT NULL COMMENT 'Grupo funcional: homologacion_tipo_doc, homologacion_genero, umbral_calidad, satelite',
        nombre              STRING    NOT NULL COMMENT 'Nombre del parámetro; con grupo_parametros forma la llave natural',
        descripcion         STRING             COMMENT 'Descripción legible del parámetro',
        valor               STRING             COMMENT 'Valor configurado; para grupo=satelite es el query SQL parametrizado',
        valor_homologado    STRING             COMMENT 'Valor normalizado para homologaciones',
        columnas_llave      STRING             COMMENT 'Columnas llave del satélite separadas por coma (para grupo=satelite)',
        estado              STRING             COMMENT 'Estado: activo o inactivo',
        fecha_creacion      TIMESTAMP          COMMENT 'Timestamp de primera inserción (SCD1)',
        fecha_actualizacion TIMESTAMP          COMMENT 'Timestamp de última actualización; SCD1 sobrescribe'
    )
    USING DELTA
    COMMENT 'Parámetros y configuración de satélites — Silver SCD1'
    {TBLPROPERTIES}
""")
print("  [OK] silver.dim_parametros")

# silver.dim_tiempo
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOGO}.{SCHEMA_SILVER}.dim_tiempo (
        pk_tiempo     INT     NOT NULL COMMENT 'Surrogate key = YYYYMMDD; coincide con ano_mes_dia',
        fecha         DATE    NOT NULL COMMENT 'Fecha calendario',
        ano_mes_dia   INT              COMMENT 'Fecha en formato YYYYMMDD',
        ano_mes       INT              COMMENT 'Año y mes en formato YYYYMM',
        ano           INT              COMMENT 'Año',
        mes           INT              COMMENT 'Mes (1-12)',
        dia           INT              COMMENT 'Día del mes',
        trimestre     INT              COMMENT 'Trimestre del año (1-4)',
        semestre      INT              COMMENT 'Semestre del año (1-2)',
        es_fin_semana BOOLEAN          COMMENT 'true si la fecha cae en sábado o domingo'
    )
    USING DELTA
    COMMENT 'Dimensión de tiempo — Silver'
    {TBLPROPERTIES}
""")
print("  [OK] silver.dim_tiempo")

# silver.dim_reglas_calidad — SCD2; llave natural (tabla, atributo, regla)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOGO}.{SCHEMA_SILVER}.dim_reglas_calidad (
        pk_regla_calidad      BIGINT    NOT NULL COMMENT 'Surrogate key de la versión del registro (SCD2)',
        tabla                 STRING             COMMENT 'Satélite objetivo; * aplica a todos',
        atributo              STRING             COMMENT 'Campo del satélite que se evalúa',
        regla                 STRING             COMMENT 'Expresión SQL booleana evaluada con F.expr()',
        descripcion_regla     STRING             COMMENT 'Descripción legible de la regla',
        grupo_regla           STRING             COMMENT 'Dimensión de calidad: completitud, formato, unicidad, consistencia',
        prioridad_regla       INT                COMMENT 'Prioridad: 1=alta, 2=media, 3=baja',
        estado_regla          STRING             COMMENT 'Estado: activo o inactivo',
        vigencia_activa       BOOLEAN            COMMENT 'true si es la versión vigente (SCD2)',
        fecha_inicio_vigencia TIMESTAMP          COMMENT 'Inicio de vigencia de esta versión',
        fecha_fin_vigencia    TIMESTAMP          COMMENT 'Fin de vigencia; NULL cuando vigencia_activa = true',
        fecha_creacion        TIMESTAMP          COMMENT 'Timestamp de primera inserción',
        fecha_actualizacion   TIMESTAMP          COMMENT 'Timestamp de última actualización'
    )
    USING DELTA
    COMMENT 'Reglas de calidad con historial de versiones — Silver SCD2'
    {TBLPROPERTIES}
""")
print("  [OK] silver.dim_reglas_calidad")

# silver.hub_cliente — Hub Data Vault 2.0
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOGO}.{SCHEMA_SILVER}.hub_cliente (
        pk_hub_cliente      BIGINT    NOT NULL COMMENT 'Surrogate key del hub; identifica (id_cliente, satelite) unívocamente',
        id_cliente          STRING    NOT NULL COMMENT 'Llave natural — concatenación tipo_documento + _ + numero_documento',
        id_satelite         BIGINT             COMMENT 'Referencia al surrogate key del registro en la tabla satélite fuente',
        satelite            STRING    NOT NULL COMMENT 'Nombre de la tabla satélite (asegurados, polizas, contacto)',
        fecha_creacion      TIMESTAMP          COMMENT 'Timestamp de primera detección del cliente en esta fuente',
        fecha_actualizacion TIMESTAMP          COMMENT 'Timestamp de última actualización'
    )
    USING DELTA
    COMMENT 'Hub de clientes Data Vault 2.0 — Silver'
    {TBLPROPERTIES}
""")
print("  [OK] silver.hub_cliente")

# COMMAND ----------
# MAGIC ## 8. Tablas — Gold

# COMMAND ----------

log_seccion("TABLAS GOLD")

# gold.dim_parametros — réplica de silver.dim_parametros
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOGO}.{SCHEMA_GOLD}.dim_parametros (
        pk_parametro        BIGINT    NOT NULL COMMENT 'Surrogate key del parámetro',
        grupo_parametros    STRING    NOT NULL COMMENT 'Grupo funcional: homologacion_tipo_doc, homologacion_genero, umbral_calidad, satelite',
        nombre              STRING    NOT NULL COMMENT 'Nombre del parámetro; con grupo_parametros forma la llave natural',
        descripcion         STRING             COMMENT 'Descripción legible del parámetro',
        valor               STRING             COMMENT 'Valor configurado',
        valor_homologado    STRING             COMMENT 'Valor normalizado para homologaciones',
        columnas_llave      STRING             COMMENT 'Columnas llave del satélite separadas por coma',
        estado              STRING             COMMENT 'Estado: activo o inactivo',
        fecha_creacion      TIMESTAMP          COMMENT 'Timestamp de primera inserción',
        fecha_actualizacion TIMESTAMP          COMMENT 'Timestamp de última actualización'
    )
    USING DELTA
    COMMENT 'Dimensión de parámetros — Gold'
    {TBLPROPERTIES}
""")
print("  [OK] gold.dim_parametros")

# gold.dim_reglas_calidad — réplica de silver.dim_reglas_calidad
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOGO}.{SCHEMA_GOLD}.dim_reglas_calidad (
        pk_regla_calidad      BIGINT    NOT NULL COMMENT 'Surrogate key de la versión de la regla (SCD2)',
        tabla                 STRING             COMMENT 'Satélite objetivo; * aplica a todos',
        atributo              STRING             COMMENT 'Campo del satélite que se evalúa',
        regla                 STRING             COMMENT 'Expresión SQL booleana evaluada con F.expr()',
        descripcion_regla     STRING             COMMENT 'Descripción legible de la regla',
        grupo_regla           STRING             COMMENT 'Dimensión de calidad: completitud, formato, unicidad, consistencia',
        prioridad_regla       INT                COMMENT 'Prioridad: 1=alta, 2=media, 3=baja',
        estado_regla          STRING             COMMENT 'Estado: activo o inactivo',
        vigencia_activa       BOOLEAN            COMMENT 'true si es la versión vigente',
        fecha_inicio_vigencia TIMESTAMP          COMMENT 'Inicio de vigencia de esta versión',
        fecha_fin_vigencia    TIMESTAMP          COMMENT 'Fin de vigencia; NULL cuando vigencia_activa = true',
        fecha_creacion        TIMESTAMP          COMMENT 'Timestamp de primera inserción',
        fecha_actualizacion   TIMESTAMP          COMMENT 'Timestamp de última actualización'
    )
    USING DELTA
    COMMENT 'Dimensión de reglas de calidad — Gold'
    {TBLPROPERTIES}
""")
print("  [OK] gold.dim_reglas_calidad")

# gold.dim_tiempo — réplica de silver.dim_tiempo
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOGO}.{SCHEMA_GOLD}.dim_tiempo (
        pk_tiempo     INT     NOT NULL COMMENT 'Surrogate key = YYYYMMDD; coincide con ano_mes_dia',
        fecha         DATE    NOT NULL COMMENT 'Fecha calendario',
        ano_mes_dia   INT              COMMENT 'Fecha en formato YYYYMMDD',
        ano_mes       INT              COMMENT 'Año y mes en formato YYYYMM',
        ano           INT              COMMENT 'Año',
        mes           INT              COMMENT 'Mes (1-12)',
        dia           INT              COMMENT 'Día del mes',
        trimestre     INT              COMMENT 'Trimestre del año (1-4)',
        semestre      INT              COMMENT 'Semestre del año (1-2)',
        es_fin_semana BOOLEAN          COMMENT 'true si la fecha cae en sábado o domingo'
    )
    USING DELTA
    COMMENT 'Dimensión de tiempo — Gold'
    {TBLPROPERTIES}
""")
print("  [OK] gold.dim_tiempo")

# gold.hub_cliente — réplica de silver.hub_cliente
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOGO}.{SCHEMA_GOLD}.hub_cliente (
        pk_hub_cliente      BIGINT    NOT NULL COMMENT 'Surrogate key del hub cliente',
        id_cliente          STRING    NOT NULL COMMENT 'Llave natural — concatenación tipo_documento + _ + numero_documento',
        id_satelite         BIGINT             COMMENT 'Referencia al surrogate key del registro en la tabla satélite fuente',
        satelite            STRING    NOT NULL COMMENT 'Nombre de la tabla satélite o fuente asociada',
        fecha_creacion      TIMESTAMP          COMMENT 'Timestamp de primera detección del cliente en esta fuente',
        fecha_actualizacion TIMESTAMP          COMMENT 'Timestamp de última actualización del registro'
    )
    USING DELTA
    COMMENT 'Hub de clientes por satélite/fuente — Gold'
    {TBLPROPERTIES}
""")
print("  [OK] gold.hub_cliente")

# gold.fact_reporte_calidad — grano: (fk_hub_cliente, fk_regla_calidad, atributo)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOGO}.{SCHEMA_GOLD}.fact_reporte_calidad (
        fk_hub_cliente      BIGINT    NOT NULL COMMENT 'FK a silver.hub_cliente.pk_hub_cliente',
        fk_regla_calidad    BIGINT    NOT NULL COMMENT 'FK a gold.dim_reglas_calidad.pk_regla_calidad',
        atributo            STRING    NOT NULL COMMENT 'Campo evaluado del satélite',
        valor_atributo      STRING             COMMENT 'Valor del atributo en el momento de la evaluación',
        fecha_ini_deteccion TIMESTAMP          COMMENT 'Primera vez que se detectó el hallazgo',
        fecha_fin_deteccion TIMESTAMP          COMMENT 'Última vez confirmada como inválido; NULL si persiste',
        flag_remediado      INT                COMMENT '1 si el hallazgo fue corregido, 0 si persiste',
        pivot               INT                COMMENT 'Campo auxiliar para matrices de calidad',
        flag_resultado      INT                COMMENT '1 = válido, 0 = inválido',
        fecha_creacion      TIMESTAMP          COMMENT 'Timestamp de primera inserción del hallazgo',
        fecha_actualizacion TIMESTAMP          COMMENT 'Timestamp de la última actualización del registro'
    )
    USING DELTA
    COMMENT 'Hecho de reporte de calidad por cliente y regla — Gold'
    {TBLPROPERTIES}
""")
print("  [OK] gold.fact_reporte_calidad")

# gold.fact_resumen_reporte_calidad — grano: (pk_tiempo, ambito, descripcion_ambito)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOGO}.{SCHEMA_GOLD}.fact_resumen_reporte_calidad (
        pk_tiempo           INT     NOT NULL COMMENT 'FK a gold.dim_tiempo.pk_tiempo (= YYYYMMDD)',
        ambito              STRING  NOT NULL COMMENT 'Nivel de agrupación: global, satelite, dimension, regla',
        descripcion_ambito  STRING  NOT NULL COMMENT 'Valor del ámbito: nombre del satélite, dimensión o regla',
        cant_evaluados      BIGINT           COMMENT 'Cantidad total de registros evaluados',
        cant_validos        BIGINT           COMMENT 'Cantidad de registros que pasaron todas las reglas',
        cant_invalidos      BIGINT           COMMENT 'Cantidad de registros con al menos un hallazgo',
        cant_remediados     BIGINT           COMMENT 'Cantidad de hallazgos marcados como remediados',
        porcentaje_calidad  DOUBLE           COMMENT 'Porcentaje de registros válidos sobre el total evaluado'
    )
    USING DELTA
    COMMENT 'Hecho de resumen de calidad por tiempo y ámbito — Gold'
    {TBLPROPERTIES}
""")
print("  [OK] gold.fact_resumen_reporte_calidad")

# COMMAND ----------
# MAGIC ## 9. Resumen

# COMMAND ----------

log_seccion("RESUMEN 01_setup")

resumen = {
    "catalogo" : CATALOGO,
    "schemas"  : list(schemas.keys()),
    "volumen"  : VOLUMEN,
    "tablas"   : {
        "bronze"   : ["parametros", "reglas_calidad", "satelites"],
        "silver"   : ["dim_parametros", "dim_reglas_calidad", "dim_tiempo", "hub_cliente"],
        "gold"     : ["dim_parametros", "dim_reglas_calidad", "dim_tiempo",
                      "fact_reporte_calidad", "fact_resumen_reporte_calidad", "hub_cliente"],
        "auditoria": ["log_ejecuciones", "log_reglas_calidad"],
    },
}

for schema, tablas in resumen["tablas"].items():
    for t in tablas:
        print(f"  [OK] {CATALOGO}.{schema}.{t}")

print(f"\n  Total tablas : {sum(len(v) for v in resumen['tablas'].values())}")
print(f"  Volumen      : {CATALOGO}.{SCHEMA_BRONZE}.{VOLUMEN}")
