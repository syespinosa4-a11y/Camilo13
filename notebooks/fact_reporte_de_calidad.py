# Databricks notebook source
# NTT DATA 2026
# MAGIC %md
# MAGIC # FACT_REPORTE_DE_CALIDAD — Evaluación de Reglas de Calidad
# MAGIC
# MAGIC Evalúa las 85 reglas de calidad definidas en el consolidado DQ v2
# MAGIC y carga los resultados en `uc-axa-cli.gold.fact_reporte_de_calidad`.
# MAGIC
# MAGIC | Dimensión      | Reglas |
# MAGIC |----------------|--------|
# MAGIC | Completitud    | 20     |
# MAGIC | Validez        | 22     |
# MAGIC | Consistencia   | 13     |
# MAGIC | Unicidad       | 1      |
# MAGIC | Razonabilidad  | 6      |
# MAGIC | **Total**      | **85** |

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 1 — Configuración

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timezone

spark = SparkSession.builder.getOrCreate()

# Satélites fuente (contienen los atributos a evaluar)
SAT_ARL   = "`uc-axa-cli`.`silver`.`sv_sat_arl`"
SAT_BH    = "`uc-axa-cli`.`silver`.`sv_sat_beyond_health`"
SAT_PYC   = "`uc-axa-cli`.`silver`.`sv_sat_pyc`"

# HUB: relaciona cada satélite con su pk_hub_cliente
HUB_TABLE = "`uc-axa-cli`.`silver`.`sv_hub_clientes`"

# Tabla destino donde se registran los hallazgos de calidad
FACT_TABLE = "`uc-axa-cli`.`gold`.`fact_reporte_de_calidad`"

LOAD_TS = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# Valores que representan "campo vacío" (aplica a todas las reglas de completitud)
EMPTY_VALUES = ("NULL", "0", "NE", "NO ESPECIFICA", "NO TIENE", "NA", "", " ")
EMPTY_SQL    = "NULL, 'NULL', '0', 'NE', 'NO ESPECIFICA', 'NO TIENE', 'NA', '', ' '"

print(f"Inicio  : {LOAD_TS}")
print(f"Satélites: {SAT_ARL}, {SAT_BH}, {SAT_PYC}")
print(f"Destino : {FACT_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 2 — Catálogo de reglas
# MAGIC
# MAGIC Cada entrada define:
# MAGIC - `pk_regla_calidad` : código único de la regla
# MAGIC - `atributo`         : campo evaluado
# MAGIC - `dimension`        : Completitud / Validez / Consistencia / Unicidad / Razonabilidad
# MAGIC - `tipo_regla`       : Campo / Tabla / Relación entre campos
# MAGIC - `dominio`          : Cliente / Contrato
# MAGIC - `descripcion`      : descripción de negocio
# MAGIC - `sql_expr`         : condición SQL que devuelve TRUE cuando el registro **falla** la regla
# MAGIC - `spark_col`        : expresión PySpark equivalente (Column) — True = falla

RULES_CATALOG = [

    # ══════════════════════════════════════════════════════════════════════
    # TIPO DE DOCUMENTO
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "TD-COM-1",
        "atributo":  "tipo_documento",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Tipo de documento es obligatorio para todas las personas.",
        "sql_expr": f"""
            tipo_documento IS NULL
            OR TRIM(UPPER(tipo_documento)) IN ({EMPTY_SQL})
        """,
        "spark_col": (
            F.col("tipo_documento").isNull() |
            F.trim(F.upper(F.col("tipo_documento"))).isin(*EMPTY_VALUES)
        ),
    },
    {
        "pk_regla_calidad": "TD-VAL-1",
        "atributo":  "tipo_documento",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Tipo de documento debe pertenecer a la lista de referencia: CC, TI, RC, NIT, CE, PS, PPT.",
        "sql_expr": """
            tipo_documento IS NOT NULL
            AND UPPER(TRIM(tipo_documento)) NOT IN ('CC','TI','RC','NIT','CE','PS','PPT')
        """,
        "spark_col": (
            F.col("tipo_documento").isNotNull() &
            ~F.trim(F.upper(F.col("tipo_documento"))).isin("CC","TI","RC","NIT","CE","PS","PPT")
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # NÚMERO DOCUMENTO
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "ND-COM-1",
        "atributo":  "num_documento",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Número de documento es obligatorio para todas las personas.",
        "sql_expr": f"""
            num_documento IS NULL
            OR TRIM(UPPER(num_documento)) IN ({EMPTY_SQL})
        """,
        "spark_col": (
            F.col("num_documento").isNull() |
            F.trim(F.upper(F.col("num_documento"))).isin(*EMPTY_VALUES)
        ),
    },
    {
        "pk_regla_calidad": "ND-VAL-1",
        "atributo":  "num_documento",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Número de documento debe cumplir formato según tipo: TI/RC numérico 10 dígitos, CC 6-10 dígitos, NIT 9-10 dígitos, CE 4-7 dígitos, PS/PPT alfanumérico.",
        "sql_expr": """
            num_documento IS NOT NULL
            AND CASE
                WHEN UPPER(tipo_documento) IN ('TI','RC') THEN
                    NOT (num_documento RLIKE '^[0-9]{10}$' AND LEFT(num_documento,1) = '1')
                WHEN UPPER(tipo_documento) = 'CC' THEN
                    NOT num_documento RLIKE '^[0-9]{6,10}$'
                WHEN UPPER(tipo_documento) = 'NIT' THEN
                    NOT num_documento RLIKE '^[0-9]{9,10}$'
                WHEN UPPER(tipo_documento) = 'CE' THEN
                    NOT num_documento RLIKE '^[0-9]{4,7}$'
                WHEN UPPER(tipo_documento) IN ('PS','PPT') THEN
                    NOT num_documento RLIKE '^[a-zA-Z0-9]{1,20}$'
                ELSE FALSE
            END
        """,
        "spark_col": (
            F.col("num_documento").isNotNull() &
            ~(
                F.when(
                    F.upper(F.col("tipo_documento")).isin("TI","RC"),
                    F.col("num_documento").rlike(r"^[0-9]{10}$") & (F.col("num_documento").substr(1,1) == "1")
                ).when(
                    F.upper(F.col("tipo_documento")) == "CC",
                    F.col("num_documento").rlike(r"^[0-9]{6,10}$")
                ).when(
                    F.upper(F.col("tipo_documento")) == "NIT",
                    F.col("num_documento").rlike(r"^[0-9]{9,10}$")
                ).when(
                    F.upper(F.col("tipo_documento")) == "CE",
                    F.col("num_documento").rlike(r"^[0-9]{4,7}$")
                ).when(
                    F.upper(F.col("tipo_documento")).isin("PS","PPT"),
                    F.col("num_documento").rlike(r"^[a-zA-Z0-9]{1,20}$")
                ).otherwise(F.lit(True))
            )
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # LLAVE ÚNICA CLIENTE
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "LL-UNI-1",
        "atributo":  "num_documento",
        "dimension": "Unicidad",
        "tipo_regla": "Tabla",
        "dominio":   "Cliente",
        "descripcion": "El número de documento debe ser único por tipo de documento. No se permiten duplicados.",
        "sql_expr": """
            (tipo_documento, num_documento) IN (
                SELECT tipo_documento, num_documento
                FROM {SOURCE}
                WHERE num_documento IS NOT NULL
                GROUP BY tipo_documento, num_documento
                HAVING COUNT(*) > 1
            )
        """,
        "spark_col": None,  # se evalúa con lógica especial de ventana (ver Bloque 4)
    },

    # ══════════════════════════════════════════════════════════════════════
    # PRIMER NOMBRE
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "PN-COM-1",
        "atributo":  "primer_nombre",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Primer nombre es obligatorio para personas naturales.",
        "sql_expr": f"""
            primer_nombre IS NULL
            OR TRIM(UPPER(primer_nombre)) IN ({EMPTY_SQL})
        """,
        "spark_col": (
            F.col("primer_nombre").isNull() |
            F.trim(F.upper(F.col("primer_nombre"))).isin(*EMPTY_VALUES)
        ),
    },
    {
        "pk_regla_calidad": "PN-VAL-1",
        "atributo":  "primer_nombre",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Primer nombre solo debe contener letras del alfabeto español (incluyendo acentos y ñ). Sin números ni caracteres especiales.",
        "sql_expr": r"""
            primer_nombre IS NOT NULL
            AND NOT TRIM(primer_nombre) RLIKE '^[a-zA-ZáéíóúÁÉÍÓÚüÜñÑ ]+$'
        """,
        "spark_col": (
            F.col("primer_nombre").isNotNull() &
            ~F.trim(F.col("primer_nombre")).rlike(r"^[a-zA-ZáéíóúÁÉÍÓÚüÜñÑ ]+$")
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # PRIMER APELLIDO
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "PA-COM-1",
        "atributo":  "primer_apellido",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Primer apellido es obligatorio para personas naturales (tipo_documento != NIT).",
        "sql_expr": f"""
            UPPER(tipo_documento) <> 'NIT'
            AND (primer_apellido IS NULL OR TRIM(UPPER(primer_apellido)) IN ({EMPTY_SQL}))
        """,
        "spark_col": (
            (F.upper(F.col("tipo_documento")) != "NIT") &
            (F.col("primer_apellido").isNull() | F.trim(F.upper(F.col("primer_apellido"))).isin(*EMPTY_VALUES))
        ),
    },
    {
        "pk_regla_calidad": "PA-VAL-1",
        "atributo":  "primer_apellido",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Primer apellido solo debe contener letras del alfabeto español. Sin números ni caracteres especiales (aplica para tipo_documento != NIT).",
        "sql_expr": r"""
            UPPER(tipo_documento) <> 'NIT'
            AND primer_apellido IS NOT NULL
            AND NOT TRIM(primer_apellido) RLIKE '^[a-zA-ZáéíóúÁÉÍÓÚüÜñÑ ]+$'
        """,
        "spark_col": (
            (F.upper(F.col("tipo_documento")) != "NIT") &
            F.col("primer_apellido").isNotNull() &
            ~F.trim(F.col("primer_apellido")).rlike(r"^[a-zA-ZáéíóúÁÉÍÓÚüÜñÑ ]+$")
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # EMAIL
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "EM-COM-1",
        "atributo":  "email",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Email es obligatorio para todos los clientes.",
        "sql_expr": f"""
            email IS NULL
            OR TRIM(UPPER(email)) IN ({EMPTY_SQL})
        """,
        "spark_col": (
            F.col("email").isNull() |
            F.trim(F.upper(F.col("email"))).isin(*EMPTY_VALUES)
        ),
    },
    {
        "pk_regla_calidad": "EM-COM-2",
        "atributo":  "email",
        "dimension": "Completitud",
        "tipo_regla": "Relación entre campos",
        "dominio":   "Cliente",
        "descripcion": "Para menores de edad (TI, RC o edad < 18), el email debe corresponder al del responsable legal.",
        "sql_expr": """
            UPPER(tipo_documento) IN ('TI','RC')
            AND (email IS NULL OR email = '')
        """,
        "spark_col": (
            F.upper(F.col("tipo_documento")).isin("TI","RC") &
            (F.col("email").isNull() | (F.col("email") == ""))
        ),
    },
    {
        "pk_regla_calidad": "EM-VAL-1",
        "atributo":  "email",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Email debe cumplir formato estándar: usuario@dominio.TLD. Un solo @, sin espacios.",
        "sql_expr": r"""
            email IS NOT NULL
            AND NOT email RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'
        """,
        "spark_col": (
            F.col("email").isNotNull() &
            ~F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
        ),
    },
    {
        "pk_regla_calidad": "EM-VAL-2",
        "atributo":  "email",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Usuario del email: letras/números/puntos/guiones/guion bajo, sin iniciar ni terminar en punto, 1-64 caracteres.",
        "sql_expr": r"""
            email IS NOT NULL
            AND LOCATE('@', email) > 0
            AND NOT SUBSTRING_INDEX(email, '@', 1) RLIKE '^[a-zA-Z0-9][a-zA-Z0-9._\\-]{0,62}[a-zA-Z0-9]$|^[a-zA-Z0-9]$'
        """,
        "spark_col": (
            F.col("email").isNotNull() &
            (F.locate("@", F.col("email")) > 0) &
            ~F.split(F.col("email"), "@").getItem(0)
             .rlike(r"^[a-zA-Z0-9][a-zA-Z0-9._\-]{0,62}[a-zA-Z0-9]$|^[a-zA-Z0-9]$")
        ),
    },
    {
        "pk_regla_calidad": "EM-VAL-3",
        "atributo":  "email",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Dominio del email: letras/números/guiones, al menos un punto separando TLD, longitud 4-255 caracteres.",
        "sql_expr": r"""
            email IS NOT NULL
            AND LOCATE('@', email) > 0
            AND NOT SUBSTRING_INDEX(email, '@', -1) RLIKE '^[a-zA-Z0-9\\-]+(\\.[a-zA-Z0-9\\-]+)+$'
        """,
        "spark_col": (
            F.col("email").isNotNull() &
            (F.locate("@", F.col("email")) > 0) &
            ~F.split(F.col("email"), "@").getItem(1)
             .rlike(r"^[a-zA-Z0-9\-]+(\.[a-zA-Z0-9\-]+)+$")
        ),
    },
    {
        "pk_regla_calidad": "EM-VAL-4",
        "atributo":  "email",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Longitud total del email no debe exceder 320 caracteres.",
        "sql_expr": """
            email IS NOT NULL AND LENGTH(email) > 320
        """,
        "spark_col": (
            F.col("email").isNotNull() & (F.length(F.col("email")) > 320)
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # CELULAR
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "CEL-COM-1",
        "atributo":  "celular",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Celular es obligatorio para todos los clientes.",
        "sql_expr": f"""
            celular IS NULL
            OR TRIM(UPPER(celular)) IN ({EMPTY_SQL})
        """,
        "spark_col": (
            F.col("celular").isNull() |
            F.trim(F.upper(F.col("celular"))).isin(*EMPTY_VALUES)
        ),
    },
    {
        "pk_regla_calidad": "CEL-COM-2",
        "atributo":  "celular",
        "dimension": "Completitud",
        "tipo_regla": "Relación entre campos",
        "dominio":   "Cliente",
        "descripcion": "Para menores de edad (TI, RC), el celular debe corresponder al del responsable legal.",
        "sql_expr": """
            UPPER(tipo_documento) IN ('TI','RC')
            AND (celular IS NULL OR celular = '')
        """,
        "spark_col": (
            F.upper(F.col("tipo_documento")).isin("TI","RC") &
            (F.col("celular").isNull() | (F.col("celular") == ""))
        ),
    },
    {
        "pk_regla_calidad": "CEL-VAL-1",
        "atributo":  "celular",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Celular debe ser numérico, 10 dígitos, iniciar por 3, sin espacios ni caracteres especiales.",
        "sql_expr": r"""
            celular IS NOT NULL
            AND NOT celular RLIKE '^3[0-9]{9}$'
        """,
        "spark_col": (
            F.col("celular").isNotNull() &
            ~F.col("celular").rlike(r"^3[0-9]{9}$")
        ),
    },
    {
        "pk_regla_calidad": "CEL-VAL-2",
        "atributo":  "celular",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Los tres primeros dígitos del celular deben estar en el rango 300 a 399.",
        "sql_expr": """
            celular IS NOT NULL
            AND celular RLIKE '^[0-9]{10}$'
            AND CAST(LEFT(celular, 3) AS INT) NOT BETWEEN 300 AND 399
        """,
        "spark_col": (
            F.col("celular").isNotNull() &
            F.col("celular").rlike(r"^[0-9]{10}$") &
            ~F.col("celular").substr(1,3).cast("int").between(300, 399)
        ),
    },
    {
        "pk_regla_calidad": "CEL-VAL-3",
        "atributo":  "celular",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Longitud del celular debe ser exactamente 10 dígitos.",
        "sql_expr": """
            celular IS NOT NULL AND LENGTH(TRIM(celular)) <> 10
        """,
        "spark_col": (
            F.col("celular").isNotNull() &
            (F.length(F.trim(F.col("celular"))) != 10)
        ),
    },
    {
        "pk_regla_calidad": "CEL-VAL-4",
        "atributo":  "celular",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Celular no debe contener caracteres no numéricos ni espacios.",
        "sql_expr": r"""
            celular IS NOT NULL
            AND (celular LIKE '% %' OR NOT celular RLIKE '^[0-9]+$')
        """,
        "spark_col": (
            F.col("celular").isNotNull() &
            (F.col("celular").contains(" ") | ~F.col("celular").rlike(r"^[0-9]+$"))
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # DIRECCIÓN DE RESIDENCIA
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "DR-COM-1",
        "atributo":  "direccion_residencia",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Dirección de residencia es obligatoria para personas naturales mayores de edad y jurídicas.",
        "sql_expr": f"""
            direccion_residencia IS NULL
            OR TRIM(UPPER(direccion_residencia)) IN ({EMPTY_SQL})
        """,
        "spark_col": (
            F.col("direccion_residencia").isNull() |
            F.trim(F.upper(F.col("direccion_residencia"))).isin(*EMPTY_VALUES)
        ),
    },
    {
        "pk_regla_calidad": "DR-VAL-1",
        "atributo":  "direccion_residencia",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Dirección debe ser alfanumérica, no solo números, contener al menos una letra.",
        "sql_expr": r"""
            direccion_residencia IS NOT NULL
            AND (
                NOT direccion_residencia RLIKE '^[a-zA-Z0-9áéíóúÁÉÍÓÚñÑ #\\-\\.°]+$'
                OR NOT direccion_residencia RLIKE '[a-zA-ZáéíóúÁÉÍÓÚñÑ]'
            )
        """,
        "spark_col": (
            F.col("direccion_residencia").isNotNull() &
            (
                ~F.col("direccion_residencia").rlike(r"^[a-zA-Z0-9áéíóúÁÉÍÓÚñÑ #\-\.°]+$") |
                ~F.col("direccion_residencia").rlike(r"[a-zA-ZáéíóúÁÉÍÓÚñÑ]")
            )
        ),
    },
    {
        "pk_regla_calidad": "DR-CON-1",
        "atributo":  "direccion_residencia",
        "dimension": "Consistencia",
        "tipo_regla": "Relación entre campos",
        "dominio":   "Cliente",
        "descripcion": "Si ciudad y departamento están diligenciados, la dirección no debe ser nula.",
        "sql_expr": f"""
            ciudad_residencia IS NOT NULL
            AND departamento IS NOT NULL
            AND (direccion_residencia IS NULL OR TRIM(UPPER(direccion_residencia)) IN ({EMPTY_SQL}))
        """,
        "spark_col": (
            F.col("ciudad_residencia").isNotNull() &
            F.col("departamento").isNotNull() &
            (F.col("direccion_residencia").isNull() | F.trim(F.upper(F.col("direccion_residencia"))).isin(*EMPTY_VALUES))
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # CIUDAD DE RESIDENCIA
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "CR-COM-1",
        "atributo":  "ciudad_residencia",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Ciudad de residencia es obligatoria.",
        "sql_expr": f"""
            ciudad_residencia IS NULL
            OR TRIM(UPPER(ciudad_residencia)) IN ({EMPTY_SQL})
        """,
        "spark_col": (
            F.col("ciudad_residencia").isNull() |
            F.trim(F.upper(F.col("ciudad_residencia"))).isin(*EMPTY_VALUES)
        ),
    },
    {
        "pk_regla_calidad": "CR-VAL-1",
        "atributo":  "ciudad_residencia",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Ciudad de residencia debe contener solo letras del alfabeto español, sin números. Debe pertenecer a DIVIPOLA.",
        "sql_expr": r"""
            ciudad_residencia IS NOT NULL
            AND NOT TRIM(ciudad_residencia) RLIKE '^[a-zA-ZáéíóúÁÉÍÓÚüÜñÑ \\-]+$'
        """,
        "spark_col": (
            F.col("ciudad_residencia").isNotNull() &
            ~F.trim(F.col("ciudad_residencia")).rlike(r"^[a-zA-ZáéíóúÁÉÍÓÚüÜñÑ \-]+$")
        ),
    },
    {
        "pk_regla_calidad": "CR-CON-1",
        "atributo":  "ciudad_residencia",
        "dimension": "Consistencia",
        "tipo_regla": "Relación entre campos",
        "dominio":   "Cliente",
        "descripcion": "Ciudad de residencia debe ser coherente con el departamento (DIVIPOLA).",
        "sql_expr": """
            departamento IS NOT NULL
            AND ciudad_residencia IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM referencia_divipola d
                WHERE d.departamento = departamento
                  AND d.municipio    = ciudad_residencia
            )
        """,
        "spark_col": None,  # requiere join con tabla de referencia DIVIPOLA (ver Bloque 5)
    },

    # ══════════════════════════════════════════════════════════════════════
    # FECHA DE NACIMIENTO
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "FN-COM-1",
        "atributo":  "fecha_nacimiento",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Fecha de nacimiento es obligatoria para personas naturales (tipo_documento != NIT).",
        "sql_expr": f"""
            UPPER(tipo_documento) <> 'NIT'
            AND (fecha_nacimiento IS NULL OR TRIM(CAST(fecha_nacimiento AS STRING)) IN ({EMPTY_SQL}))
        """,
        "spark_col": (
            (F.upper(F.col("tipo_documento")) != "NIT") &
            (F.col("fecha_nacimiento").isNull() | F.trim(F.col("fecha_nacimiento").cast("string")).isin(*EMPTY_VALUES))
        ),
    },
    {
        "pk_regla_calidad": "FN-VAL-1",
        "atributo":  "fecha_nacimiento",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Fecha de nacimiento debe ser una fecha válida, no futura y con edad máxima de 108 años.",
        "sql_expr": """
            fecha_nacimiento IS NOT NULL
            AND (
                TRY_CAST(fecha_nacimiento AS DATE) IS NULL
                OR CAST(fecha_nacimiento AS DATE) > CURRENT_DATE()
                OR DATEDIFF(CURRENT_DATE(), CAST(fecha_nacimiento AS DATE)) / 365.25 > 108
            )
        """,
        "spark_col": (
            F.col("fecha_nacimiento").isNotNull() &
            (
                F.to_date(F.col("fecha_nacimiento")).isNull() |
                (F.to_date(F.col("fecha_nacimiento")) > F.current_date()) |
                (F.datediff(F.current_date(), F.to_date(F.col("fecha_nacimiento"))) / 365.25 > 108)
            )
        ),
    },
    {
        "pk_regla_calidad": "FN-RAZ-1",
        "atributo":  "fecha_nacimiento",
        "dimension": "Razonabilidad",
        "tipo_regla": "Relación entre campos",
        "dominio":   "Cliente",
        "descripcion": "Si la edad < 18, tipo_documento debe ser TI, RC, PS, CE o PPT. CC con menor de edad es error.",
        "sql_expr": """
            fecha_nacimiento IS NOT NULL
            AND DATEDIFF(CURRENT_DATE(), CAST(fecha_nacimiento AS DATE)) / 365.25 < 18
            AND UPPER(tipo_documento) = 'CC'
        """,
        "spark_col": (
            F.col("fecha_nacimiento").isNotNull() &
            ((F.datediff(F.current_date(), F.to_date(F.col("fecha_nacimiento"))) / 365.25) < 18) &
            (F.upper(F.col("tipo_documento")) == "CC")
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # GÉNERO
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "GEN-COM-1",
        "atributo":  "genero",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Género es obligatorio para personas naturales (tipo_documento != NIT).",
        "sql_expr": f"""
            UPPER(tipo_documento) <> 'NIT'
            AND (genero IS NULL OR TRIM(UPPER(genero)) IN ({EMPTY_SQL}))
        """,
        "spark_col": (
            (F.upper(F.col("tipo_documento")) != "NIT") &
            (F.col("genero").isNull() | F.trim(F.upper(F.col("genero"))).isin(*EMPTY_VALUES))
        ),
    },
    {
        "pk_regla_calidad": "GEN-VAL-1",
        "atributo":  "genero",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Género debe ser F (Femenino), M (Masculino) o N (No binario).",
        "sql_expr": """
            genero IS NOT NULL
            AND UPPER(TRIM(genero)) NOT IN ('F','M','N')
        """,
        "spark_col": (
            F.col("genero").isNotNull() &
            ~F.upper(F.trim(F.col("genero"))).isin("F","M","N")
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # EPS ACTUAL
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "EPS-COM-1",
        "atributo":  "eps_actual",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "EPS actual es obligatoria para personas naturales (tipo_documento != NIT).",
        "sql_expr": f"""
            UPPER(tipo_documento) <> 'NIT'
            AND (eps_actual IS NULL OR TRIM(UPPER(eps_actual)) IN ({EMPTY_SQL}))
        """,
        "spark_col": (
            (F.upper(F.col("tipo_documento")) != "NIT") &
            (F.col("eps_actual").isNull() | F.trim(F.upper(F.col("eps_actual"))).isin(*EMPTY_VALUES))
        ),
    },
    {
        "pk_regla_calidad": "EPS-VAL-1",
        "atributo":  "eps_actual",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "EPS debe pertenecer al listado oficial vigente del Ministerio de Salud de Colombia.",
        "sql_expr": """
            eps_actual IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM referencia_eps r WHERE r.nombre_eps = eps_actual AND r.vigente = 'SI'
            )
        """,
        "spark_col": None,  # requiere join con tabla de referencia EPS (ver Bloque 5)
    },

    # ══════════════════════════════════════════════════════════════════════
    # FECHA EXPEDICIÓN DOCUMENTO
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "FED-COM-1",
        "atributo":  "fecha_expedicion_documento",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Fecha de expedición del documento es obligatoria para tipo_documento != NIT.",
        "sql_expr": f"""
            UPPER(tipo_documento) <> 'NIT'
            AND (fecha_expedicion_documento IS NULL OR TRIM(CAST(fecha_expedicion_documento AS STRING)) IN ({EMPTY_SQL}))
        """,
        "spark_col": (
            (F.upper(F.col("tipo_documento")) != "NIT") &
            (F.col("fecha_expedicion_documento").isNull() |
             F.trim(F.col("fecha_expedicion_documento").cast("string")).isin(*EMPTY_VALUES))
        ),
    },
    {
        "pk_regla_calidad": "FED-VAL-1",
        "atributo":  "fecha_expedicion_documento",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Fecha de expedición debe ser válida, no futura y no mayor a 101 años de antigüedad.",
        "sql_expr": """
            fecha_expedicion_documento IS NOT NULL
            AND (
                TRY_CAST(fecha_expedicion_documento AS DATE) IS NULL
                OR CAST(fecha_expedicion_documento AS DATE) > CURRENT_DATE()
                OR CAST(fecha_expedicion_documento AS DATE) < ADD_MONTHS(CURRENT_DATE(), -101*12)
            )
        """,
        "spark_col": (
            F.col("fecha_expedicion_documento").isNotNull() &
            (
                F.to_date(F.col("fecha_expedicion_documento")).isNull() |
                (F.to_date(F.col("fecha_expedicion_documento")) > F.current_date()) |
                (F.datediff(F.current_date(), F.to_date(F.col("fecha_expedicion_documento"))) / 365.25 > 101)
            )
        ),
    },
    {
        "pk_regla_calidad": "FED-RAZ-1",
        "atributo":  "fecha_expedicion_documento",
        "dimension": "Razonabilidad",
        "tipo_regla": "Relación entre campos",
        "dominio":   "Cliente",
        "descripcion": "Fecha de expedición no puede ser anterior a la fecha de nacimiento.",
        "sql_expr": """
            fecha_expedicion_documento IS NOT NULL
            AND fecha_nacimiento IS NOT NULL
            AND CAST(fecha_expedicion_documento AS DATE) < CAST(fecha_nacimiento AS DATE)
        """,
        "spark_col": (
            F.col("fecha_expedicion_documento").isNotNull() &
            F.col("fecha_nacimiento").isNotNull() &
            (F.to_date(F.col("fecha_expedicion_documento")) < F.to_date(F.col("fecha_nacimiento")))
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # ATDP
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "ATDP-COM-1",
        "atributo":  "atdp",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "ATDP es obligatorio para personas naturales (tipo_documento != NIT).",
        "sql_expr": f"""
            UPPER(tipo_documento) <> 'NIT'
            AND (atdp IS NULL OR TRIM(UPPER(atdp)) IN ({EMPTY_SQL}))
        """,
        "spark_col": (
            (F.upper(F.col("tipo_documento")) != "NIT") &
            (F.col("atdp").isNull() | F.trim(F.upper(F.col("atdp"))).isin(*EMPTY_VALUES))
        ),
    },
    {
        "pk_regla_calidad": "ATDP-VAL-1",
        "atributo":  "atdp",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "ATDP solo acepta los valores SI o NO.",
        "sql_expr": """
            atdp IS NOT NULL
            AND UPPER(TRIM(atdp)) NOT IN ('SI','NO')
        """,
        "spark_col": (
            F.col("atdp").isNotNull() &
            ~F.upper(F.trim(F.col("atdp"))).isin("SI","NO")
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # FECHA ACTUALIZACIÓN ATDP
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "FATDP-COM-1",
        "atributo":  "fecha_atdp",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Fecha de actualización ATDP es obligatoria para personas naturales si ATDP está diligenciado.",
        "sql_expr": f"""
            UPPER(tipo_documento) <> 'NIT'
            AND atdp IS NOT NULL AND TRIM(atdp) <> ''
            AND (fecha_atdp IS NULL OR TRIM(CAST(fecha_atdp AS STRING)) IN ({EMPTY_SQL}))
        """,
        "spark_col": (
            (F.upper(F.col("tipo_documento")) != "NIT") &
            F.col("atdp").isNotNull() & (F.trim(F.col("atdp")) != "") &
            (F.col("fecha_atdp").isNull() | F.trim(F.col("fecha_atdp").cast("string")).isin(*EMPTY_VALUES))
        ),
    },
    {
        "pk_regla_calidad": "FATDP-COM-2",
        "atributo":  "fecha_atdp",
        "dimension": "Completitud",
        "tipo_regla": "Relación entre campos",
        "dominio":   "Cliente",
        "descripcion": "Si ATDP no es nulo → fecha_atdp obligatoria. Si ATDP es nulo → fecha_atdp debe ser nula.",
        "sql_expr": """
            (atdp IS NOT NULL AND atdp <> '' AND fecha_atdp IS NULL)
            OR (atdp IS NULL AND fecha_atdp IS NOT NULL)
        """,
        "spark_col": (
            (F.col("atdp").isNotNull() & (F.col("atdp") != "") & F.col("fecha_atdp").isNull()) |
            (F.col("atdp").isNull() & F.col("fecha_atdp").isNotNull())
        ),
    },
    {
        "pk_regla_calidad": "FATDP-VAL-1",
        "atributo":  "fecha_atdp",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Fecha ATDP debe ser válida y no futura.",
        "sql_expr": """
            fecha_atdp IS NOT NULL
            AND (
                TRY_CAST(fecha_atdp AS DATE) IS NULL
                OR CAST(fecha_atdp AS DATE) > CURRENT_DATE()
            )
        """,
        "spark_col": (
            F.col("fecha_atdp").isNotNull() &
            (
                F.to_date(F.col("fecha_atdp")).isNull() |
                (F.to_date(F.col("fecha_atdp")) > F.current_date())
            )
        ),
    },
    {
        "pk_regla_calidad": "FATDP-RAZ-1",
        "atributo":  "fecha_atdp",
        "dimension": "Razonabilidad",
        "tipo_regla": "Relación entre campos",
        "dominio":   "Cliente",
        "descripcion": "Fecha ATDP no puede ser anterior a la fecha de nacimiento.",
        "sql_expr": """
            fecha_atdp IS NOT NULL AND fecha_nacimiento IS NOT NULL
            AND CAST(fecha_atdp AS DATE) < CAST(fecha_nacimiento AS DATE)
        """,
        "spark_col": (
            F.col("fecha_atdp").isNotNull() & F.col("fecha_nacimiento").isNotNull() &
            (F.to_date(F.col("fecha_atdp")) < F.to_date(F.col("fecha_nacimiento")))
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # PREFERENCIA DE CONTACTO (LEY 2300)
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "PC-COM-1",
        "atributo":  "preferencia_contacto",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Preferencia de contacto es obligatoria para personas naturales (Ley 2300 de 2023).",
        "sql_expr": f"""
            UPPER(tipo_documento) <> 'NIT'
            AND (preferencia_contacto IS NULL OR TRIM(UPPER(preferencia_contacto)) IN ({EMPTY_SQL}))
        """,
        "spark_col": (
            (F.upper(F.col("tipo_documento")) != "NIT") &
            (F.col("preferencia_contacto").isNull() | F.trim(F.upper(F.col("preferencia_contacto"))).isin(*EMPTY_VALUES))
        ),
    },
    {
        "pk_regla_calidad": "PC-VAL-1",
        "atributo":  "preferencia_contacto",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Preferencia de contacto debe ser: EMAIL, CELULAR, TELEFONO, CORREO_POSTAL o NINGUNO.",
        "sql_expr": """
            preferencia_contacto IS NOT NULL
            AND UPPER(TRIM(preferencia_contacto)) NOT IN ('EMAIL','CELULAR','TELEFONO','CORREO_POSTAL','NINGUNO')
        """,
        "spark_col": (
            F.col("preferencia_contacto").isNotNull() &
            ~F.upper(F.trim(F.col("preferencia_contacto"))).isin("EMAIL","CELULAR","TELEFONO","CORREO_POSTAL","NINGUNO")
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # FECHA MEDIO DE CONTACTO
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "FMC-COM-1",
        "atributo":  "fecha_medio_contacto",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Fecha medio de contacto es obligatoria si preferencia_contacto está diligenciada.",
        "sql_expr": f"""
            preferencia_contacto IS NOT NULL AND TRIM(preferencia_contacto) <> ''
            AND (fecha_medio_contacto IS NULL OR TRIM(CAST(fecha_medio_contacto AS STRING)) IN ({EMPTY_SQL}))
        """,
        "spark_col": (
            F.col("preferencia_contacto").isNotNull() & (F.trim(F.col("preferencia_contacto")) != "") &
            (F.col("fecha_medio_contacto").isNull() | F.trim(F.col("fecha_medio_contacto").cast("string")).isin(*EMPTY_VALUES))
        ),
    },
    {
        "pk_regla_calidad": "FMC-COM-2",
        "atributo":  "fecha_medio_contacto",
        "dimension": "Completitud",
        "tipo_regla": "Relación entre campos",
        "dominio":   "Cliente",
        "descripcion": "Si preferencia_contacto está diligenciada, fecha_medio_contacto es obligatoria.",
        "sql_expr": """
            preferencia_contacto IS NOT NULL AND preferencia_contacto <> ''
            AND fecha_medio_contacto IS NULL
        """,
        "spark_col": (
            F.col("preferencia_contacto").isNotNull() & (F.col("preferencia_contacto") != "") &
            F.col("fecha_medio_contacto").isNull()
        ),
    },
    {
        "pk_regla_calidad": "FMC-VAL-1",
        "atributo":  "fecha_medio_contacto",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Fecha medio de contacto debe ser válida y no futura.",
        "sql_expr": """
            fecha_medio_contacto IS NOT NULL
            AND (
                TRY_CAST(fecha_medio_contacto AS DATE) IS NULL
                OR CAST(fecha_medio_contacto AS DATE) > CURRENT_DATE()
            )
        """,
        "spark_col": (
            F.col("fecha_medio_contacto").isNotNull() &
            (
                F.to_date(F.col("fecha_medio_contacto")).isNull() |
                (F.to_date(F.col("fecha_medio_contacto")) > F.current_date())
            )
        ),
    },
    {
        "pk_regla_calidad": "FMC-RAZ-1",
        "atributo":  "fecha_medio_contacto",
        "dimension": "Razonabilidad",
        "tipo_regla": "Relación entre campos",
        "dominio":   "Cliente",
        "descripcion": "Fecha de medio de contacto no puede ser anterior a la fecha de nacimiento.",
        "sql_expr": """
            fecha_medio_contacto IS NOT NULL AND fecha_nacimiento IS NOT NULL
            AND CAST(fecha_medio_contacto AS DATE) < CAST(fecha_nacimiento AS DATE)
        """,
        "spark_col": (
            F.col("fecha_medio_contacto").isNotNull() & F.col("fecha_nacimiento").isNotNull() &
            (F.to_date(F.col("fecha_medio_contacto")) < F.to_date(F.col("fecha_nacimiento")))
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # ATDP COMERCIAL
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "ATDPC-COM-1",
        "atributo":  "atdp_comercial",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "ATDP Comercial es obligatorio para personas naturales (Ley 2300).",
        "sql_expr": f"""
            UPPER(tipo_documento) <> 'NIT'
            AND (atdp_comercial IS NULL OR TRIM(UPPER(atdp_comercial)) IN ({EMPTY_SQL}))
        """,
        "spark_col": (
            (F.upper(F.col("tipo_documento")) != "NIT") &
            (F.col("atdp_comercial").isNull() | F.trim(F.upper(F.col("atdp_comercial"))).isin(*EMPTY_VALUES))
        ),
    },
    {
        "pk_regla_calidad": "ATDPC-VAL-1",
        "atributo":  "atdp_comercial",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "ATDP Comercial solo acepta los valores SI o NO.",
        "sql_expr": """
            atdp_comercial IS NOT NULL
            AND UPPER(TRIM(atdp_comercial)) NOT IN ('SI','NO')
        """,
        "spark_col": (
            F.col("atdp_comercial").isNotNull() &
            ~F.upper(F.trim(F.col("atdp_comercial"))).isin("SI","NO")
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # FECHA ACTUALIZACIÓN ATDP COMERCIAL
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "FATDPC-COM-1",
        "atributo":  "fecha_atdp_comercial",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Fecha ATDP Comercial es obligatoria si atdp_comercial está diligenciado.",
        "sql_expr": f"""
            atdp_comercial IS NOT NULL AND TRIM(atdp_comercial) <> ''
            AND (fecha_atdp_comercial IS NULL OR TRIM(CAST(fecha_atdp_comercial AS STRING)) IN ({EMPTY_SQL}))
        """,
        "spark_col": (
            F.col("atdp_comercial").isNotNull() & (F.trim(F.col("atdp_comercial")) != "") &
            (F.col("fecha_atdp_comercial").isNull() | F.trim(F.col("fecha_atdp_comercial").cast("string")).isin(*EMPTY_VALUES))
        ),
    },
    {
        "pk_regla_calidad": "FATDPC-COM-2",
        "atributo":  "fecha_atdp_comercial",
        "dimension": "Completitud",
        "tipo_regla": "Relación entre campos",
        "dominio":   "Cliente",
        "descripcion": "Si atdp_comercial no es nulo → fecha_atdp_comercial obligatoria. Si es nulo → debe ser nula.",
        "sql_expr": """
            (atdp_comercial IS NOT NULL AND atdp_comercial <> '' AND fecha_atdp_comercial IS NULL)
            OR (atdp_comercial IS NULL AND fecha_atdp_comercial IS NOT NULL)
        """,
        "spark_col": (
            (F.col("atdp_comercial").isNotNull() & (F.col("atdp_comercial") != "") & F.col("fecha_atdp_comercial").isNull()) |
            (F.col("atdp_comercial").isNull() & F.col("fecha_atdp_comercial").isNotNull())
        ),
    },
    {
        "pk_regla_calidad": "FATDPC-VAL-1",
        "atributo":  "fecha_atdp_comercial",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Fecha ATDP Comercial debe ser válida y no futura.",
        "sql_expr": """
            fecha_atdp_comercial IS NOT NULL
            AND (
                TRY_CAST(fecha_atdp_comercial AS DATE) IS NULL
                OR CAST(fecha_atdp_comercial AS DATE) > CURRENT_DATE()
            )
        """,
        "spark_col": (
            F.col("fecha_atdp_comercial").isNotNull() &
            (
                F.to_date(F.col("fecha_atdp_comercial")).isNull() |
                (F.to_date(F.col("fecha_atdp_comercial")) > F.current_date())
            )
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # TIPO DE PERSONA
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "TP-COM-1",
        "atributo":  "tipo_persona",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Tipo de persona es obligatorio para todos los registros.",
        "sql_expr": f"""
            tipo_persona IS NULL
            OR TRIM(UPPER(tipo_persona)) IN ('NULL','NE','NO ESPECIFICA','',' ')
        """,
        "spark_col": (
            F.col("tipo_persona").isNull() |
            F.trim(F.upper(F.col("tipo_persona"))).isin("NULL","NE","NO ESPECIFICA",""," ")
        ),
    },
    {
        "pk_regla_calidad": "TP-VAL-1",
        "atributo":  "tipo_persona",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Tipo de persona debe ser NATURAL o JURIDICA (o sus abreviaciones N/J).",
        "sql_expr": """
            tipo_persona IS NOT NULL
            AND UPPER(TRIM(tipo_persona)) NOT IN ('NATURAL','JURIDICA','N','J')
        """,
        "spark_col": (
            F.col("tipo_persona").isNotNull() &
            ~F.upper(F.trim(F.col("tipo_persona"))).isin("NATURAL","JURIDICA","N","J")
        ),
    },
    {
        "pk_regla_calidad": "TP-RAZ-1",
        "atributo":  "tipo_persona",
        "dimension": "Razonabilidad",
        "tipo_regla": "Relación entre campos",
        "dominio":   "Cliente",
        "descripcion": "Persona JURIDICA debe tener NIT. Persona NATURAL no debe tener NIT.",
        "sql_expr": """
            (UPPER(tipo_persona) IN ('JURIDICA','J') AND UPPER(tipo_documento) <> 'NIT')
            OR (UPPER(tipo_persona) IN ('NATURAL','N')  AND UPPER(tipo_documento) = 'NIT')
        """,
        "spark_col": (
            (F.upper(F.col("tipo_persona")).isin("JURIDICA","J") & (F.upper(F.col("tipo_documento")) != "NIT")) |
            (F.upper(F.col("tipo_persona")).isin("NATURAL","N")  & (F.upper(F.col("tipo_documento")) == "NIT"))
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # ESTADO CIVIL
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "EC-COM-1",
        "atributo":  "estado_civil",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Estado civil es obligatorio para personas con tipo_documento = CC (mayor de edad).",
        "sql_expr": f"""
            UPPER(tipo_documento) = 'CC'
            AND (estado_civil IS NULL OR TRIM(UPPER(estado_civil)) IN ({EMPTY_SQL}))
        """,
        "spark_col": (
            (F.upper(F.col("tipo_documento")) == "CC") &
            (F.col("estado_civil").isNull() | F.trim(F.upper(F.col("estado_civil"))).isin(*EMPTY_VALUES))
        ),
    },
    {
        "pk_regla_calidad": "EC-VAL-1",
        "atributo":  "estado_civil",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Estado civil debe ser: SOLTERO, CASADO, UNION_LIBRE, DIVORCIADO, VIUDO o SEPARADO.",
        "sql_expr": """
            estado_civil IS NOT NULL
            AND UPPER(TRIM(estado_civil)) NOT IN ('SOLTERO','CASADO','UNION_LIBRE','DIVORCIADO','VIUDO','SEPARADO')
        """,
        "spark_col": (
            F.col("estado_civil").isNotNull() &
            ~F.upper(F.trim(F.col("estado_civil"))).isin("SOLTERO","CASADO","UNION_LIBRE","DIVORCIADO","VIUDO","SEPARADO")
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # ACTIVIDAD ECONÓMICA
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "AE-COM-1",
        "atributo":  "actividad_economica",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Actividad económica es obligatoria para tipo_documento = NIT.",
        "sql_expr": f"""
            UPPER(tipo_documento) = 'NIT'
            AND (actividad_economica IS NULL OR TRIM(UPPER(actividad_economica)) IN ({EMPTY_SQL}))
        """,
        "spark_col": (
            (F.upper(F.col("tipo_documento")) == "NIT") &
            (F.col("actividad_economica").isNull() | F.trim(F.upper(F.col("actividad_economica"))).isin(*EMPTY_VALUES))
        ),
    },
    {
        "pk_regla_calidad": "AE-VAL-1",
        "atributo":  "actividad_economica",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Actividad económica debe ser un código CIIU Rev. 4 A.C. del DANE: numérico de 4 dígitos.",
        "sql_expr": r"""
            actividad_economica IS NOT NULL
            AND NOT actividad_economica RLIKE '^[0-9]{4}$'
        """,
        "spark_col": (
            F.col("actividad_economica").isNotNull() &
            ~F.col("actividad_economica").rlike(r"^[0-9]{4}$")
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # DEPARTAMENTO
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "DPTO-COM-1",
        "atributo":  "departamento",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Departamento es obligatorio cuando la dirección de residencia está diligenciada.",
        "sql_expr": f"""
            direccion_residencia IS NOT NULL AND TRIM(direccion_residencia) <> ''
            AND (departamento IS NULL OR TRIM(UPPER(departamento)) IN ({EMPTY_SQL}))
        """,
        "spark_col": (
            F.col("direccion_residencia").isNotNull() & (F.trim(F.col("direccion_residencia")) != "") &
            (F.col("departamento").isNull() | F.trim(F.upper(F.col("departamento"))).isin(*EMPTY_VALUES))
        ),
    },
    {
        "pk_regla_calidad": "DPTO-VAL-1",
        "atributo":  "departamento",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "Departamento debe contener solo letras del alfabeto español. Debe pertenecer a DIVIPOLA.",
        "sql_expr": r"""
            departamento IS NOT NULL
            AND NOT TRIM(departamento) RLIKE '^[a-zA-ZáéíóúÁÉÍÓÚüÜñÑ \\-]+$'
        """,
        "spark_col": (
            F.col("departamento").isNotNull() &
            ~F.trim(F.col("departamento")).rlike(r"^[a-zA-ZáéíóúÁÉÍÓÚüÜñÑ \-]+$")
        ),
    },
    {
        "pk_regla_calidad": "DPTO-CON-1",
        "atributo":  "departamento",
        "dimension": "Consistencia",
        "tipo_regla": "Relación entre campos",
        "dominio":   "Cliente",
        "descripcion": "Si país = COLOMBIA, departamento debe pertenecer a DIVIPOLA. Ciudad debe corresponder al departamento.",
        "sql_expr": """
            UPPER(pais) = 'COLOMBIA'
            AND departamento IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM referencia_divipola d
                WHERE d.departamento = departamento
            )
        """,
        "spark_col": None,  # requiere join con tabla de referencia DIVIPOLA (ver Bloque 5)
    },

    # ══════════════════════════════════════════════════════════════════════
    # PAÍS
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "PAIS-COM-1",
        "atributo":  "pais",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "País es obligatorio para todos los registros.",
        "sql_expr": f"""
            pais IS NULL
            OR TRIM(UPPER(pais)) IN ({EMPTY_SQL})
        """,
        "spark_col": (
            F.col("pais").isNull() |
            F.trim(F.upper(F.col("pais"))).isin(*EMPTY_VALUES)
        ),
    },
    {
        "pk_regla_calidad": "PAIS-VAL-1",
        "atributo":  "pais",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Cliente",
        "descripcion": "País debe pertenecer a la lista ISO 3166-1 (alpha-2, alpha-3 o nombre oficial).",
        "sql_expr": """
            pais IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM referencia_iso3166 p WHERE p.nombre_pais = UPPER(pais) OR p.codigo_alpha2 = UPPER(pais) OR p.codigo_alpha3 = UPPER(pais)
            )
        """,
        "spark_col": None,  # requiere join con tabla de referencia ISO 3166 (ver Bloque 5)
    },

    # ══════════════════════════════════════════════════════════════════════
    # ROL (DOMINIO CONTRATO)
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "ROL-COM-1",
        "atributo":  "rol",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Contrato",
        "descripcion": "Rol es obligatorio para todas las personas vinculadas a un contrato.",
        "sql_expr": f"""
            rol IS NULL
            OR TRIM(UPPER(rol)) IN ({EMPTY_SQL})
        """,
        "spark_col": (
            F.col("rol").isNull() |
            F.trim(F.upper(F.col("rol"))).isin(*EMPTY_VALUES)
        ),
    },
    {
        "pk_regla_calidad": "ROL-VAL-1",
        "atributo":  "rol",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Contrato",
        "descripcion": "Rol debe ser: TOMADOR, ASEGURADO, BENEFICIARIO, ASEGURADO_BENEFICIARIO o TITULAR.",
        "sql_expr": """
            rol IS NOT NULL
            AND UPPER(TRIM(rol)) NOT IN ('TOMADOR','ASEGURADO','BENEFICIARIO','ASEGURADO_BENEFICIARIO','TITULAR')
        """,
        "spark_col": (
            F.col("rol").isNotNull() &
            ~F.upper(F.trim(F.col("rol"))).isin("TOMADOR","ASEGURADO","BENEFICIARIO","ASEGURADO_BENEFICIARIO","TITULAR")
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # TIPO DE CONTRATO
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "TC-COM-1",
        "atributo":  "tipo_contrato",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Contrato",
        "descripcion": "Tipo de contrato es obligatorio para todos los registros de contrato.",
        "sql_expr": f"""
            tipo_contrato IS NULL
            OR TRIM(UPPER(tipo_contrato)) IN ({EMPTY_SQL})
        """,
        "spark_col": (
            F.col("tipo_contrato").isNull() |
            F.trim(F.upper(F.col("tipo_contrato"))).isin(*EMPTY_VALUES)
        ),
    },
    {
        "pk_regla_calidad": "TC-VAL-1",
        "atributo":  "tipo_contrato",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Contrato",
        "descripcion": "Tipo contrato debe ser: INDIVIDUAL, COLECTIVO, FAMILIAR o GRUPAL.",
        "sql_expr": """
            tipo_contrato IS NOT NULL
            AND UPPER(TRIM(tipo_contrato)) NOT IN ('INDIVIDUAL','COLECTIVO','FAMILIAR','GRUPAL')
        """,
        "spark_col": (
            F.col("tipo_contrato").isNotNull() &
            ~F.upper(F.trim(F.col("tipo_contrato"))).isin("INDIVIDUAL","COLECTIVO","FAMILIAR","GRUPAL")
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # PLAN
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "PLAN-COM-1",
        "atributo":  "plan",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Contrato",
        "descripcion": "Plan es obligatorio para todos los registros de contrato.",
        "sql_expr": f"""
            plan IS NULL
            OR TRIM(UPPER(plan)) IN ({EMPTY_SQL})
        """,
        "spark_col": (
            F.col("plan").isNull() |
            F.trim(F.upper(F.col("plan"))).isin(*EMPTY_VALUES)
        ),
    },
    {
        "pk_regla_calidad": "PLAN-VAL-1",
        "atributo":  "plan",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Contrato",
        "descripcion": "Plan debe pertenecer al catálogo de productos vigentes.",
        "sql_expr": """
            plan IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM catalogo_planes p WHERE p.codigo_plan = plan AND p.estado = 'VIGENTE'
            )
        """,
        "spark_col": None,  # requiere join con catálogo_planes (ver Bloque 5)
    },
    {
        "pk_regla_calidad": "PLAN-CON-1",
        "atributo":  "plan",
        "dimension": "Consistencia",
        "tipo_regla": "Relación entre campos",
        "dominio":   "Contrato",
        "descripcion": "El plan debe ser coherente con el ramo del negocio (ARL, VIDA, PYC).",
        "sql_expr": """
            plan IS NOT NULL AND ramo IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM catalogo_planes p
                WHERE p.codigo_plan = plan AND p.ramo = ramo
            )
        """,
        "spark_col": None,  # requiere join con catálogo_planes (ver Bloque 5)
    },

    # ══════════════════════════════════════════════════════════════════════
    # CONTRATO / PÓLIZA
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "CONT-COM-1",
        "atributo":  "contrato",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Contrato",
        "descripcion": "Número de contrato es obligatorio para todos los registros activos y cancelados.",
        "sql_expr": f"""
            contrato IS NULL
            OR TRIM(UPPER(contrato)) IN ({EMPTY_SQL})
        """,
        "spark_col": (
            F.col("contrato").isNull() |
            F.trim(F.upper(F.col("contrato"))).isin(*EMPTY_VALUES)
        ),
    },
    {
        "pk_regla_calidad": "CONT-VAL-1",
        "atributo":  "contrato",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Contrato",
        "descripcion": "Número de contrato debe ser alfanumérico, sin espacios ni caracteres especiales.",
        "sql_expr": r"""
            contrato IS NOT NULL
            AND (contrato LIKE '% %' OR NOT contrato RLIKE '^[a-zA-Z0-9\\-]+$')
        """,
        "spark_col": (
            F.col("contrato").isNotNull() &
            (F.col("contrato").contains(" ") | ~F.col("contrato").rlike(r"^[a-zA-Z0-9\-]+$"))
        ),
    },
    {
        "pk_regla_calidad": "CONT-UNI-1",
        "atributo":  "contrato",
        "dimension": "Unicidad",
        "tipo_regla": "Tabla",
        "dominio":   "Contrato",
        "descripcion": "Número de contrato no debe repetirse dentro del mismo ramo y compañía.",
        "sql_expr": """
            (contrato, ramo) IN (
                SELECT contrato, ramo FROM {SOURCE}
                WHERE contrato IS NOT NULL
                GROUP BY contrato, ramo HAVING COUNT(*) > 1
            )
        """,
        "spark_col": None,  # se evalúa con lógica especial de ventana (ver Bloque 4)
    },

    # ══════════════════════════════════════════════════════════════════════
    # ASESOR
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "ASE-COM-1",
        "atributo":  "asesor",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Contrato",
        "descripcion": "Asesor es obligatorio para todos los contratos.",
        "sql_expr": f"""
            asesor IS NULL
            OR TRIM(UPPER(asesor)) IN ({EMPTY_SQL})
        """,
        "spark_col": (
            F.col("asesor").isNull() |
            F.trim(F.upper(F.col("asesor"))).isin(*EMPTY_VALUES)
        ),
    },
    {
        "pk_regla_calidad": "ASE-VAL-1",
        "atributo":  "asesor",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Contrato",
        "descripcion": "Asesor debe existir en el maestro de asesores activos y autorizados.",
        "sql_expr": """
            asesor IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM maestro_asesores a
                WHERE a.codigo_asesor = asesor AND a.estado = 'ACTIVO'
            )
        """,
        "spark_col": None,  # requiere join con maestro_asesores (ver Bloque 5)
    },

    # ══════════════════════════════════════════════════════════════════════
    # ESTADO DEL CONTRATO
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "EST-COM-1",
        "atributo":  "estado",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Contrato",
        "descripcion": "Estado del contrato es obligatorio para todos los registros.",
        "sql_expr": f"""
            estado IS NULL
            OR TRIM(UPPER(estado)) IN ({EMPTY_SQL})
        """,
        "spark_col": (
            F.col("estado").isNull() |
            F.trim(F.upper(F.col("estado"))).isin(*EMPTY_VALUES)
        ),
    },
    {
        "pk_regla_calidad": "EST-VAL-1",
        "atributo":  "estado",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Contrato",
        "descripcion": "Estado debe ser: ACTIVO, INACTIVO, CANCELADO, SUSPENDIDO, EN_TRAMITE o VENCIDO.",
        "sql_expr": """
            estado IS NOT NULL
            AND UPPER(TRIM(estado)) NOT IN ('ACTIVO','INACTIVO','CANCELADO','SUSPENDIDO','EN_TRAMITE','VENCIDO')
        """,
        "spark_col": (
            F.col("estado").isNotNull() &
            ~F.upper(F.trim(F.col("estado"))).isin("ACTIVO","INACTIVO","CANCELADO","SUSPENDIDO","EN_TRAMITE","VENCIDO")
        ),
    },
    {
        "pk_regla_calidad": "EST-RAZ-1",
        "atributo":  "estado",
        "dimension": "Razonabilidad",
        "tipo_regla": "Relación entre campos",
        "dominio":   "Contrato",
        "descripcion": "Estado debe ser coherente con fechas de vigencia: si fecha_fin < hoy → no puede ser ACTIVO.",
        "sql_expr": """
            estado IS NOT NULL AND fecha_fin_vigencia IS NOT NULL
            AND (
                (CAST(fecha_fin_vigencia AS DATE) < CURRENT_DATE() AND UPPER(estado) = 'ACTIVO')
                OR (CAST(fecha_inicio_vigencia AS DATE) > CURRENT_DATE() AND UPPER(estado) NOT IN ('EN_TRAMITE','INACTIVO'))
            )
        """,
        "spark_col": (
            F.col("estado").isNotNull() & F.col("fecha_fin_vigencia").isNotNull() &
            (
                ((F.to_date(F.col("fecha_fin_vigencia")) < F.current_date()) & (F.upper(F.col("estado")) == "ACTIVO")) |
                ((F.to_date(F.col("fecha_inicio_vigencia")) > F.current_date()) &
                 ~F.upper(F.col("estado")).isin("EN_TRAMITE","INACTIVO"))
            )
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # FECHA INICIO DE VIGENCIA
    # ══════════════════════════════════════════════════════════════════════
    {
        "pk_regla_calidad": "FIV-COM-1",
        "atributo":  "fecha_inicio_vigencia",
        "dimension": "Completitud",
        "tipo_regla": "Campo",
        "dominio":   "Contrato",
        "descripcion": "Fecha de inicio de vigencia es obligatoria para todos los contratos.",
        "sql_expr": f"""
            fecha_inicio_vigencia IS NULL
            OR TRIM(CAST(fecha_inicio_vigencia AS STRING)) IN ({EMPTY_SQL})
        """,
        "spark_col": (
            F.col("fecha_inicio_vigencia").isNull() |
            F.trim(F.col("fecha_inicio_vigencia").cast("string")).isin(*EMPTY_VALUES)
        ),
    },
    {
        "pk_regla_calidad": "FIV-VAL-1",
        "atributo":  "fecha_inicio_vigencia",
        "dimension": "Validez",
        "tipo_regla": "Campo",
        "dominio":   "Contrato",
        "descripcion": "Fecha de inicio de vigencia debe ser válida, dentro del rango [-5 años, +1 año] desde hoy.",
        "sql_expr": """
            fecha_inicio_vigencia IS NOT NULL
            AND (
                TRY_CAST(fecha_inicio_vigencia AS DATE) IS NULL
                OR CAST(fecha_inicio_vigencia AS DATE) < ADD_MONTHS(CURRENT_DATE(), -60)
                OR CAST(fecha_inicio_vigencia AS DATE) > ADD_MONTHS(CURRENT_DATE(), 12)
            )
        """,
        "spark_col": (
            F.col("fecha_inicio_vigencia").isNotNull() &
            (
                F.to_date(F.col("fecha_inicio_vigencia")).isNull() |
                (F.to_date(F.col("fecha_inicio_vigencia")) < F.date_add(F.current_date(), -5*365)) |
                (F.to_date(F.col("fecha_inicio_vigencia")) > F.date_add(F.current_date(), 365))
            )
        ),
    },
    {
        "pk_regla_calidad": "FIV-CON-1",
        "atributo":  "fecha_inicio_vigencia",
        "dimension": "Consistencia",
        "tipo_regla": "Relación entre campos",
        "dominio":   "Contrato",
        "descripcion": "Fecha inicio vigencia debe ser anterior a fecha fin vigencia. Si ACTIVO, inicio <= hoy. Si EN_TRAMITE, inicio > hoy.",
        "sql_expr": """
            fecha_inicio_vigencia IS NOT NULL AND fecha_fin_vigencia IS NOT NULL
            AND (
                CAST(fecha_inicio_vigencia AS DATE) >= CAST(fecha_fin_vigencia AS DATE)
                OR (UPPER(estado) = 'ACTIVO'    AND CAST(fecha_inicio_vigencia AS DATE) > CURRENT_DATE())
                OR (UPPER(estado) = 'EN_TRAMITE' AND CAST(fecha_inicio_vigencia AS DATE) <= CURRENT_DATE())
            )
        """,
        "spark_col": (
            F.col("fecha_inicio_vigencia").isNotNull() & F.col("fecha_fin_vigencia").isNotNull() &
            (
                (F.to_date(F.col("fecha_inicio_vigencia")) >= F.to_date(F.col("fecha_fin_vigencia"))) |
                ((F.upper(F.col("estado")) == "ACTIVO") &
                 (F.to_date(F.col("fecha_inicio_vigencia")) > F.current_date())) |
                ((F.upper(F.col("estado")) == "EN_TRAMITE") &
                 (F.to_date(F.col("fecha_inicio_vigencia")) <= F.current_date()))
            )
        ),
    },
    {
        "pk_regla_calidad": "FIV-OP-1",
        "atributo":  "fecha_inicio_vigencia",
        "dimension": "Razonabilidad",
        "tipo_regla": "Campo",
        "dominio":   "Contrato",
        "descripcion": "Contratos con fecha de inicio > 5 años sin renovación registrada son datos potencialmente desactualizados.",
        "sql_expr": """
            fecha_inicio_vigencia IS NOT NULL
            AND CAST(fecha_inicio_vigencia AS DATE) < ADD_MONTHS(CURRENT_DATE(), -60)
            AND UPPER(estado) = 'ACTIVO'
        """,
        "spark_col": (
            F.col("fecha_inicio_vigencia").isNotNull() &
            (F.to_date(F.col("fecha_inicio_vigencia")) < F.date_add(F.current_date(), -5*365)) &
            (F.upper(F.col("estado")) == "ACTIVO")
        ),
    },
]

print(f"Total de reglas cargadas: {len(RULES_CATALOG)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 3 — Construir fuente de evaluación
# MAGIC
# MAGIC Une cada satélite con el HUB para obtener `pk_hub_cliente` (→ `fk_hub_cliente`).
# MAGIC Los tres satélites se unen con `unionByName(allowMissingColumns=True)` para
# MAGIC preservar todas las columnas aunque difieran entre sistemas.

def read_sat_with_hub(sat_table: str, pk_sat: str, sat_name: str):
    """Lee un satélite y lo enriquece con pk_hub_cliente del HUB."""
    sat = spark.sql(f"SELECT * FROM {sat_table}")
    hub = spark.sql(f"""
        SELECT pk_hub_cliente, id_satelite
        FROM {HUB_TABLE}
        WHERE satelite = '{sat_name}'
    """)
    return (
        sat.join(hub, sat[pk_sat].cast("string") == hub["id_satelite"], "left")
           .drop("id_satelite")
    )

df_arl = read_sat_with_hub(SAT_ARL, "id_sat_arl",           "sat_arl")
df_bh  = read_sat_with_hub(SAT_BH,  "id_sat_beyond_health", "sat_beyond_health")
df_pyc = read_sat_with_hub(SAT_PYC, "id_sat_pyc",           "sat_pyc")

df_source = (
    df_arl
    .unionByName(df_bh,  allowMissingColumns=True)
    .unionByName(df_pyc, allowMissingColumns=True)
)

df_source.cache()
total_registros = df_source.count()
print(f"Registros a evaluar: {total_registros:,}")
print(f"  ARL          : {df_arl.count():>10,}")
print(f"  Beyond Health: {df_bh.count():>10,}")
print(f"  PyC / SISE   : {df_pyc.count():>10,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 4 — Evaluación de reglas
# MAGIC
# MAGIC Para cada regla:
# MAGIC - Si tiene `spark_col` → filtra el DataFrame con la expresión PySpark
# MAGIC - Si `spark_col` es None (reglas de unicidad o que requieren JOIN con catálogo) →
# MAGIC   se ejecuta la `sql_expr` directamente sobre la vista temporal
# MAGIC
# MAGIC El resultado es un DataFrame de **registros que fallan** la regla,
# MAGIC listo para insertar en FACT_REPORTE_DE_CALIDAD.

df_source.createOrReplaceTempView("_dq_source")

all_results = []

for rule in RULES_CATALOG:
    pk   = rule["pk_regla_calidad"]
    attr = rule["atributo"]
    dim  = rule["dimension"]

    try:
        if rule["spark_col"] is not None:
            # Evaluación con expresión PySpark
            df_fail = (
                df_source
                .filter(rule["spark_col"])
                .select(
                    F.col("pk_hub_cliente").alias("fk_hub_cliente"),
                    F.lit(pk).alias("fk_regla_calidad"),
                    F.lit(attr).alias("atributo"),
                    F.col(attr).cast("string").alias("valor_atributo")
                    if attr in df_source.columns else F.lit(None).cast("string").alias("valor_atributo"),
                    F.lit(LOAD_TS).alias("fecha_ini_deteccion"),
                    F.lit(None).cast("string").alias("fecha_fin_deteccion"),
                    F.lit(LOAD_TS).alias("fecha_actualizacion"),
                    F.lit("N").alias("flag_remediado"),
                    F.lit(dim).alias("pivot"),
                    F.lit("FALLA").alias("flag_resultado"),
                    F.lit(LOAD_TS).alias("fecha_creacion"),
                )
            )
        else:
            # Evaluación con SQL para reglas de unicidad / JOIN con catálogos
            sql = rule["sql_expr"].replace("{SOURCE}", "_dq_source")
            df_fail = spark.sql(f"""
                SELECT
                    pk_hub_cliente            AS fk_hub_cliente,
                    '{pk}'                    AS fk_regla_calidad,
                    '{attr}'                  AS atributo,
                    CAST({attr} AS STRING)    AS valor_atributo,
                    '{LOAD_TS}'               AS fecha_ini_deteccion,
                    NULL                      AS fecha_fin_deteccion,
                    '{LOAD_TS}'               AS fecha_actualizacion,
                    'N'                       AS flag_remediado,
                    '{dim}'                   AS pivot,
                    'FALLA'                   AS flag_resultado,
                    '{LOAD_TS}'               AS fecha_creacion
                FROM _dq_source
                WHERE {sql}
            """)

        fail_count = df_fail.count()
        all_results.append({
            "pk_regla_calidad": pk,
            "atributo":  attr,
            "dimension": dim,
            "tipo_regla": rule["tipo_regla"],
            "registros_evaluados": total_registros,
            "registros_fallidos":  fail_count,
            "pct_calidad": round((1 - fail_count / total_registros) * 100, 2) if total_registros > 0 else 100.0,
            "df_fail": df_fail,
        })
        estado = "✓" if fail_count == 0 else "✗"
        print(f"  {estado}  [{dim[:3].upper()}] {pk:<20} → {fail_count:>8,} registros fallan")

    except Exception as e:
        all_results.append({
            "pk_regla_calidad": pk,
            "atributo":  attr,
            "dimension": dim,
            "tipo_regla": rule["tipo_regla"],
            "registros_evaluados": total_registros,
            "registros_fallidos":  -1,
            "pct_calidad": None,
            "df_fail": None,
            "error": str(e),
        })
        print(f"  ✗  [{dim[:3].upper()}] {pk:<20} → ERROR: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 5 — Carga en FACT_REPORTE_DE_CALIDAD

def table_exists(table_sql):
    try:
        spark.sql(f"DESCRIBE TABLE {table_sql}")
        return True
    except Exception:
        return False

# Unir todos los DataFrames de fallas
dfs_to_load = [r["df_fail"] for r in all_results if r.get("df_fail") is not None]

if dfs_to_load:
    df_final = dfs_to_load[0]
    for df in dfs_to_load[1:]:
        df_final = df_final.unionByName(df, allowMissingColumns=True)

    if not table_exists(FACT_TABLE):
        print(f"Creando tabla {FACT_TABLE}...")
        df_final.limit(0).createOrReplaceTempView("_fact_schema_ref")
        spark.sql(f"""
            CREATE TABLE {FACT_TABLE}
            USING DELTA
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
            AS SELECT * FROM _fact_schema_ref
        """)
        spark.catalog.dropTempView("_fact_schema_ref")

    df_final.createOrReplaceTempView("_fact_staging")

    spark.sql(f"""
        MERGE INTO {FACT_TABLE} AS tgt
        USING _fact_staging AS src
          ON  tgt.fk_hub_cliente    = src.fk_hub_cliente
          AND tgt.fk_regla_calidad  = src.fk_regla_calidad
          AND tgt.fecha_ini_deteccion = src.fecha_ini_deteccion
        WHEN MATCHED THEN
          UPDATE SET
            tgt.valor_atributo    = src.valor_atributo,
            tgt.fecha_actualizacion = src.fecha_actualizacion,
            tgt.flag_resultado    = src.flag_resultado
        WHEN NOT MATCHED THEN
          INSERT *
    """)
    spark.catalog.dropTempView("_fact_staging")
    print(f"  Carga completada en {FACT_TABLE}")
else:
    print("  Sin registros fallidos — no se insertó nada en el FACT.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 6 — Resumen ejecutivo de calidad

import pandas as pd

resumen = pd.DataFrame([
    {
        "pk_regla_calidad":      r["pk_regla_calidad"],
        "atributo":              r["atributo"],
        "dimension":             r["dimension"],
        "tipo_regla":            r["tipo_regla"],
        "registros_evaluados":   str(r["registros_evaluados"]),
        "registros_fallidos":    str(r["registros_fallidos"]),
        "pct_calidad":           str(r.get("pct_calidad", "N/A")),
        "error":                 r.get("error", ""),
    }
    for r in all_results
])

ok_count  = sum(1 for r in all_results if r["registros_fallidos"] == 0)
err_count = sum(1 for r in all_results if r["registros_fallidos"] > 0)
skip_count= sum(1 for r in all_results if r["registros_fallidos"] == -1)

print(f"\nReglas sin hallazgos : {ok_count}")
print(f"Reglas con hallazgos : {err_count}")
print(f"Reglas con error     : {skip_count}")
print(f"Total evaluadas      : {len(all_results)}\n")

display(spark.createDataFrame(resumen.fillna("")))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bloque 7 — Resumen por dimensión de calidad

dim_resumen = (
    resumen.groupby("dimension")
    .agg(
        reglas=("pk_regla_calidad", "count"),
        con_hallazgos=("registros_fallidos", lambda x: sum(1 for v in x if int(v) > 0)),
    )
    .reset_index()
)
display(spark.createDataFrame(dim_resumen))
