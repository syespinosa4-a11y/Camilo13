# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # sat_beyond_health — Satélite Beyond Health
# MAGIC
# MAGIC Este notebook construye el **satélite de Beyond Health**, que es la tabla central donde
# MAGIC se consolida toda la información de los asegurados del sistema Beyond Health.
# MAGIC
# MAGIC ## ¿Qué hace este notebook?
# MAGIC
# MAGIC Toma información de **6 tablas fuente** del sistema Beyond Health y las une en una sola tabla
# MAGIC que contiene los datos completos de cada asegurado, identificando claramente si es
# MAGIC **titular** (el asegurado principal del contrato) o **beneficiario** (familiar u otra
# MAGIC persona incluida en el contrato).
# MAGIC
# MAGIC ## Tablas fuente
# MAGIC
# MAGIC | Tabla | Qué contiene |
# MAGIC |---|---|
# MAGIC | `bh_sa_member` | Lista de todos los miembros (titulares y beneficiarios) de cada contrato |
# MAGIC | `bh_sa_affiliation_contract` | Información del contrato de afiliación |
# MAGIC | `bh_sa_person` | Datos personales de cada persona (nombre, documento, etc.) |
# MAGIC | `bh_sa_institution` | Datos de la institución o empresa que suscribe el contrato |
# MAGIC | `bh_sa_address` | Direcciones registradas de cada persona |
# MAGIC | `bh_sa_city` | Catálogo de ciudades con su código y nombre |
# MAGIC
# MAGIC ## Tabla destino
# MAGIC
# MAGIC `uc_axa_cli.silver.sat_beyond_health`
# MAGIC
# MAGIC ## Regla de negocio clave
# MAGIC
# MAGIC - **Titular**: la persona se obtiene desde el **contrato** (`aco.per_ncode`)
# MAGIC - **Beneficiario**: la persona se obtiene desde el **member** (`mem.per_ncode`)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 1 — Preparar la dirección residencial de cada persona
# MAGIC
# MAGIC Antes de armar el satélite, necesitamos tener lista la **dirección residencial** de cada
# MAGIC persona. El problema es que una persona puede tener varias direcciones registradas
# MAGIC (residencial, laboral, de correo, etc.), por eso hacemos este paso previo.
# MAGIC
# MAGIC **¿Qué hacemos aquí?**
# MAGIC
# MAGIC 1. De la tabla de direcciones (`bh_sa_address`), filtramos solo las que son de tipo
# MAGIC    **residencial** (`lty_ncode = 1`).
# MAGIC 2. Cruzamos con el catálogo de ciudades para traer el **código de ciudad**.
# MAGIC 3. Agrupamos por persona para quedarnos con **una sola fila por persona**
# MAGIC    (usamos `MAX` para tomar un valor cuando hay más de uno).
# MAGIC
# MAGIC El resultado es una tabla auxiliar `residencial` con una fila por persona que tiene:
# MAGIC su dirección residencial y el código de su ciudad.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 2 — Preparar el nombre de la ciudad
# MAGIC
# MAGIC Con el código de ciudad que obtuvimos en el paso anterior, buscamos el
# MAGIC **nombre legible de la ciudad** en el catálogo `bh_sa_city`.
# MAGIC
# MAGIC Esto nos permite mostrar, por ejemplo, `"BOGOTA D.C."` en lugar de solo el código `"11001"`.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 3 — Construir los registros de TITULARES
# MAGIC
# MAGIC Aquí construimos la información completa de los **titulares** del contrato.
# MAGIC
# MAGIC **¿Cómo identificamos al titular?**
# MAGIC La persona del titular viene del **contrato de afiliación** (`aco.per_ncode`),
# MAGIC no del member. Esto es porque el titular es quien firma el contrato.
# MAGIC
# MAGIC **Joins que se hacen:**
# MAGIC
# MAGIC | Unión | Para qué |
# MAGIC |---|---|
# MAGIC | `member` + `contrato` | Traer los datos del contrato al que pertenece cada miembro |
# MAGIC | `contrato` + `persona` | Traer los datos personales del **titular del contrato** |
# MAGIC | `contrato` + `institución` | Traer los datos de la empresa u organización del contrato |
# MAGIC | `contrato` + `residencial` | Traer la dirección del **titular** (via `aco.per_ncode`) |
# MAGIC | `residencial` + `ciudad` | Traer el nombre de la ciudad del titular |
# MAGIC
# MAGIC Al final, cada fila queda marcada con `rol = 'TITULAR'`.
# MAGIC
# MAGIC **Nota sobre los nombres de columnas:**
# MAGIC Cuando una misma columna existe en varias tablas (por ejemplo `fec_cargue` existe en
# MAGIC member, contrato y persona), se le agrega un prefijo para saber de dónde viene:
# MAGIC `mem_fec_cargue`, `aco_fec_cargue`, `per_fec_cargue`.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 4 — Construir los registros de BENEFICIARIOS
# MAGIC
# MAGIC Aquí construimos la información completa de los **beneficiarios** del contrato.
# MAGIC
# MAGIC **¿En qué se diferencia del titular?**
# MAGIC Solo hay **dos diferencias** respecto al bloque anterior:
# MAGIC
# MAGIC 1. La persona del beneficiario viene del **member** (`mem.per_ncode`), no del contrato.
# MAGIC    Esto es porque el beneficiario es quien está registrado como miembro en el contrato.
# MAGIC 2. La dirección residencial también se busca con el `per_ncode` del **member**.
# MAGIC
# MAGIC Todo lo demás (columnas, estructura, prefijos) es idéntico al bloque de titulares.
# MAGIC Al final, cada fila queda marcada con `rol = 'BENEFICIARIO'`.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 5 — Unir titulares y beneficiarios en una sola tabla
# MAGIC
# MAGIC Con `UNION ALL` juntamos los registros de titulares y beneficiarios en una sola tabla.
# MAGIC
# MAGIC La columna `rol` nos permite distinguir en todo momento quién es quién:
# MAGIC - `TITULAR` → persona principal del contrato
# MAGIC - `BENEFICIARIO` → familiar u otro miembro incluido en el contrato
# MAGIC
# MAGIC **Resultado esperado:** ~1.475.962 filas totales (737.981 titulares + 737.981 beneficiarios)
# MAGIC con 170 columnas que cubren todos los atributos de las 6 tablas fuente.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Ejecución — Crear el satélite Beyond Health
# MAGIC
# MAGIC La siguiente celda ejecuta todo el proceso descrito arriba en un solo paso.
# MAGIC Crea (o reemplaza) la tabla `uc_axa_cli.silver.sat_beyond_health` con todos los datos.
# MAGIC
# MAGIC > ⚠️ **Importante:** Este proceso borra y recrea la tabla completa cada vez que se ejecuta.
# MAGIC > Esto garantiza que siempre refleje el estado más reciente de las tablas fuente.

# COMMAND ----------
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE uc_axa_cli.silver.sat_beyond_health
# MAGIC USING DELTA AS
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- PASO 1: Dirección residencial por persona
# MAGIC -- Filtra solo tipo residencial (lty_ncode = 1) y deja una
# MAGIC -- fila por persona con su dirección y código de ciudad.
# MAGIC -- ============================================================
# MAGIC WITH residencial AS (
# MAGIC     SELECT
# MAGIC         a.per_ncode             AS res_per_ncode,
# MAGIC         MAX(a.add_caddress)     AS dir_res,
# MAGIC         MAX(c.cit_clegalcode)   AS ciu_res_codigo
# MAGIC     FROM axa_col_slv_dv.core_bh.bh_sa_address a
# MAGIC     LEFT JOIN axa_col_slv_dv.core_bh.bh_sa_city c
# MAGIC         ON a.cit_ncode = c.cit_ncode
# MAGIC     WHERE a.lty_ncode = 1
# MAGIC     GROUP BY a.per_ncode
# MAGIC ),
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- PASO 2: Nombre de ciudad por código legal
# MAGIC -- ============================================================
# MAGIC ciudad AS (
# MAGIC     SELECT
# MAGIC         cit_clegalcode  AS ciu_res_codigo,
# MAGIC         cit_cname       AS ciudad_residencia
# MAGIC     FROM axa_col_slv_dv.core_bh.bh_sa_city
# MAGIC ),
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- PASO 3: Titulares
# MAGIC -- Persona identificada desde el CONTRATO (aco.per_ncode).
# MAGIC -- Columnas con mismo nombre entre tablas llevan prefijo de origen.
# MAGIC -- ============================================================
# MAGIC titular AS (
# MAGIC     SELECT
# MAGIC         mem.* EXCEPT (aco_ncode, per_ncode, ins_ncode,
# MAGIC                       fec_cargue, fec_actualizacion, fec_eliminacion,
# MAGIC                       tipo_proceso, fec_movimiento,
# MAGIC                       COMPANIA, TIPO_CARGA, PERIODO, FECHA_CARGUE),
# MAGIC         mem.aco_ncode           AS mem_aco_ncode,
# MAGIC         mem.per_ncode           AS mem_per_ncode,
# MAGIC         mem.ins_ncode           AS mem_ins_ncode,
# MAGIC         mem.fec_cargue          AS mem_fec_cargue,
# MAGIC         mem.fec_actualizacion   AS mem_fec_actualizacion,
# MAGIC         mem.fec_eliminacion     AS mem_fec_eliminacion,
# MAGIC         mem.tipo_proceso        AS mem_tipo_proceso,
# MAGIC         mem.fec_movimiento      AS mem_fec_movimiento,
# MAGIC         mem.COMPANIA            AS mem_COMPANIA,
# MAGIC         mem.TIPO_CARGA          AS mem_TIPO_CARGA,
# MAGIC         mem.PERIODO             AS mem_PERIODO,
# MAGIC         mem.FECHA_CARGUE        AS mem_FECHA_CARGUE,
# MAGIC         aco.* EXCEPT (per_ncode, ins_ncode, add_ncode, MST_NCODE,
# MAGIC                       FEC_CARGUE, FEC_ACTUALIZACION, FEC_ELIMINACION,
# MAGIC                       TIPO_PROCESO, FEC_MOVIMIENTO,
# MAGIC                       COMPANIA, TIPO_CARGA, PERIODO, FECHA_CARGUE),
# MAGIC         aco.per_ncode           AS aco_per_ncode,
# MAGIC         aco.ins_ncode           AS aco_ins_ncode,
# MAGIC         aco.add_ncode           AS aco_ADD_NCODE,
# MAGIC         aco.MST_NCODE           AS aco_MST_NCODE,
# MAGIC         aco.FEC_CARGUE          AS aco_FEC_CARGUE,
# MAGIC         aco.FEC_ACTUALIZACION   AS aco_FEC_ACTUALIZACION,
# MAGIC         aco.FEC_ELIMINACION     AS aco_FEC_ELIMINACION,
# MAGIC         aco.TIPO_PROCESO        AS aco_TIPO_PROCESO,
# MAGIC         aco.FEC_MOVIMIENTO      AS aco_FEC_MOVIMIENTO,
# MAGIC         aco.COMPANIA            AS aco_COMPANIA,
# MAGIC         aco.TIPO_CARGA          AS aco_TIPO_CARGA,
# MAGIC         aco.PERIODO             AS aco_PERIODO,
# MAGIC         aco.FECHA_CARGUE        AS aco_FECHA_CARGUE,
# MAGIC         per.* EXCEPT (MST_NCODE, HOL_NCODE, EAC_NCODE,
# MAGIC                       TID_NCODE, PER_CIDENTIFICATIONNUMBER,
# MAGIC                       FEC_CARGUE, FEC_ACTUALIZACION, FEC_ELIMINACION,
# MAGIC                       TIPO_PROCESO, FEC_MOVIMIENTO,
# MAGIC                       COMPANIA, TIPO_CARGA, PERIODO, FECHA_CARGUE),
# MAGIC         per.TID_NCODE                 AS tipo_documento,
# MAGIC         per.PER_CIDENTIFICATIONNUMBER AS numero_documento,
# MAGIC         per.MST_NCODE           AS per_MST_NCODE,
# MAGIC         per.HOL_NCODE           AS per_HOL_NCODE,
# MAGIC         per.EAC_NCODE           AS per_EAC_NCODE,
# MAGIC         per.FEC_CARGUE          AS per_FEC_CARGUE,
# MAGIC         per.FEC_ACTUALIZACION   AS per_FEC_ACTUALIZACION,
# MAGIC         per.FEC_ELIMINACION     AS per_FEC_ELIMINACION,
# MAGIC         per.TIPO_PROCESO        AS per_TIPO_PROCESO,
# MAGIC         per.FEC_MOVIMIENTO      AS per_FEC_MOVIMIENTO,
# MAGIC         per.COMPANIA            AS per_COMPANIA,
# MAGIC         per.TIPO_CARGA          AS per_TIPO_CARGA,
# MAGIC         per.PERIODO             AS per_PERIODO,
# MAGIC         per.FECHA_CARGUE        AS per_FECHA_CARGUE,
# MAGIC         ins.* EXCEPT (EAC_NCODE, HOL_NCODE,
# MAGIC                       FEC_CARGUE, FEC_ACTUALIZACION, FEC_ELIMINACION,
# MAGIC                       TIPO_PROCESO, FEC_MOVIMIENTO,
# MAGIC                       COMPANIA, TIPO_CARGA, PERIODO, FECHA_CARGUE),
# MAGIC         ins.EAC_NCODE           AS ins_EAC_NCODE,
# MAGIC         ins.HOL_NCODE           AS ins_HOL_NCODE,
# MAGIC         ins.FEC_CARGUE          AS ins_FEC_CARGUE,
# MAGIC         ins.FEC_ACTUALIZACION   AS ins_FEC_ACTUALIZACION,
# MAGIC         ins.FEC_ELIMINACION     AS ins_FEC_ELIMINACION,
# MAGIC         ins.TIPO_PROCESO        AS ins_TIPO_PROCESO,
# MAGIC         ins.FEC_MOVIMIENTO      AS ins_FEC_MOVIMIENTO,
# MAGIC         ins.COMPANIA            AS ins_COMPANIA,
# MAGIC         ins.TIPO_CARGA          AS ins_TIPO_CARGA,
# MAGIC         ins.PERIODO             AS ins_PERIODO,
# MAGIC         ins.FECHA_CARGUE        AS ins_FECHA_CARGUE,
# MAGIC         res.dir_res,
# MAGIC         res.ciu_res_codigo,
# MAGIC         ciudad.ciudad_residencia,
# MAGIC         'TITULAR'               AS rol
# MAGIC     FROM axa_col_slv_dv.core_bh.bh_sa_member mem
# MAGIC     INNER JOIN axa_col_slv_dv.core_bh.bh_sa_affiliation_contract aco
# MAGIC         ON mem.aco_ncode = aco.aco_ncode
# MAGIC     LEFT JOIN axa_col_slv_dv.core_bh.bh_sa_person per
# MAGIC         ON aco.per_ncode = per.per_ncode          -- titular: persona del CONTRATO
# MAGIC     LEFT JOIN axa_col_slv_dv.core_bh.bh_sa_institution ins
# MAGIC         ON aco.ins_ncode = ins.ins_ncode
# MAGIC     LEFT JOIN residencial res
# MAGIC         ON aco.per_ncode = res.res_per_ncode      -- dirección del titular
# MAGIC     LEFT JOIN ciudad
# MAGIC         ON res.ciu_res_codigo = ciudad.ciu_res_codigo
# MAGIC ),
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- PASO 4: Beneficiarios
# MAGIC -- Misma estructura que titulares. Única diferencia:
# MAGIC -- persona e identificada desde el MEMBER (mem.per_ncode).
# MAGIC -- ============================================================
# MAGIC beneficiario AS (
# MAGIC     SELECT
# MAGIC         mem.* EXCEPT (aco_ncode, per_ncode, ins_ncode,
# MAGIC                       fec_cargue, fec_actualizacion, fec_eliminacion,
# MAGIC                       tipo_proceso, fec_movimiento,
# MAGIC                       COMPANIA, TIPO_CARGA, PERIODO, FECHA_CARGUE),
# MAGIC         mem.aco_ncode           AS mem_aco_ncode,
# MAGIC         mem.per_ncode           AS mem_per_ncode,
# MAGIC         mem.ins_ncode           AS mem_ins_ncode,
# MAGIC         mem.fec_cargue          AS mem_fec_cargue,
# MAGIC         mem.fec_actualizacion   AS mem_fec_actualizacion,
# MAGIC         mem.fec_eliminacion     AS mem_fec_eliminacion,
# MAGIC         mem.tipo_proceso        AS mem_tipo_proceso,
# MAGIC         mem.fec_movimiento      AS mem_fec_movimiento,
# MAGIC         mem.COMPANIA            AS mem_COMPANIA,
# MAGIC         mem.TIPO_CARGA          AS mem_TIPO_CARGA,
# MAGIC         mem.PERIODO             AS mem_PERIODO,
# MAGIC         mem.FECHA_CARGUE        AS mem_FECHA_CARGUE,
# MAGIC         aco.* EXCEPT (per_ncode, ins_ncode, add_ncode, MST_NCODE,
# MAGIC                       FEC_CARGUE, FEC_ACTUALIZACION, FEC_ELIMINACION,
# MAGIC                       TIPO_PROCESO, FEC_MOVIMIENTO,
# MAGIC                       COMPANIA, TIPO_CARGA, PERIODO, FECHA_CARGUE),
# MAGIC         aco.per_ncode           AS aco_per_ncode,
# MAGIC         aco.ins_ncode           AS aco_ins_ncode,
# MAGIC         aco.add_ncode           AS aco_ADD_NCODE,
# MAGIC         aco.MST_NCODE           AS aco_MST_NCODE,
# MAGIC         aco.FEC_CARGUE          AS aco_FEC_CARGUE,
# MAGIC         aco.FEC_ACTUALIZACION   AS aco_FEC_ACTUALIZACION,
# MAGIC         aco.FEC_ELIMINACION     AS aco_FEC_ELIMINACION,
# MAGIC         aco.TIPO_PROCESO        AS aco_TIPO_PROCESO,
# MAGIC         aco.FEC_MOVIMIENTO      AS aco_FEC_MOVIMIENTO,
# MAGIC         aco.COMPANIA            AS aco_COMPANIA,
# MAGIC         aco.TIPO_CARGA          AS aco_TIPO_CARGA,
# MAGIC         aco.PERIODO             AS aco_PERIODO,
# MAGIC         aco.FECHA_CARGUE        AS aco_FECHA_CARGUE,
# MAGIC         per.* EXCEPT (MST_NCODE, HOL_NCODE, EAC_NCODE,
# MAGIC                       TID_NCODE, PER_CIDENTIFICATIONNUMBER,
# MAGIC                       FEC_CARGUE, FEC_ACTUALIZACION, FEC_ELIMINACION,
# MAGIC                       TIPO_PROCESO, FEC_MOVIMIENTO,
# MAGIC                       COMPANIA, TIPO_CARGA, PERIODO, FECHA_CARGUE),
# MAGIC         per.TID_NCODE                 AS tipo_documento,
# MAGIC         per.PER_CIDENTIFICATIONNUMBER AS numero_documento,
# MAGIC         per.MST_NCODE           AS per_MST_NCODE,
# MAGIC         per.HOL_NCODE           AS per_HOL_NCODE,
# MAGIC         per.EAC_NCODE           AS per_EAC_NCODE,
# MAGIC         per.FEC_CARGUE          AS per_FEC_CARGUE,
# MAGIC         per.FEC_ACTUALIZACION   AS per_FEC_ACTUALIZACION,
# MAGIC         per.FEC_ELIMINACION     AS per_FEC_ELIMINACION,
# MAGIC         per.TIPO_PROCESO        AS per_TIPO_PROCESO,
# MAGIC         per.FEC_MOVIMIENTO      AS per_FEC_MOVIMIENTO,
# MAGIC         per.COMPANIA            AS per_COMPANIA,
# MAGIC         per.TIPO_CARGA          AS per_TIPO_CARGA,
# MAGIC         per.PERIODO             AS per_PERIODO,
# MAGIC         per.FECHA_CARGUE        AS per_FECHA_CARGUE,
# MAGIC         ins.* EXCEPT (EAC_NCODE, HOL_NCODE,
# MAGIC                       FEC_CARGUE, FEC_ACTUALIZACION, FEC_ELIMINACION,
# MAGIC                       TIPO_PROCESO, FEC_MOVIMIENTO,
# MAGIC                       COMPANIA, TIPO_CARGA, PERIODO, FECHA_CARGUE),
# MAGIC         ins.EAC_NCODE           AS ins_EAC_NCODE,
# MAGIC         ins.HOL_NCODE           AS ins_HOL_NCODE,
# MAGIC         ins.FEC_CARGUE          AS ins_FEC_CARGUE,
# MAGIC         ins.FEC_ACTUALIZACION   AS ins_FEC_ACTUALIZACION,
# MAGIC         ins.FEC_ELIMINACION     AS ins_FEC_ELIMINACION,
# MAGIC         ins.TIPO_PROCESO        AS ins_TIPO_PROCESO,
# MAGIC         ins.FEC_MOVIMIENTO      AS ins_FEC_MOVIMIENTO,
# MAGIC         ins.COMPANIA            AS ins_COMPANIA,
# MAGIC         ins.TIPO_CARGA          AS ins_TIPO_CARGA,
# MAGIC         ins.PERIODO             AS ins_PERIODO,
# MAGIC         ins.FECHA_CARGUE        AS ins_FECHA_CARGUE,
# MAGIC         res.dir_res,
# MAGIC         res.ciu_res_codigo,
# MAGIC         ciudad.ciudad_residencia,
# MAGIC         'BENEFICIARIO'          AS rol
# MAGIC     FROM axa_col_slv_dv.core_bh.bh_sa_member mem
# MAGIC     INNER JOIN axa_col_slv_dv.core_bh.bh_sa_affiliation_contract aco
# MAGIC         ON mem.aco_ncode = aco.aco_ncode
# MAGIC     LEFT JOIN axa_col_slv_dv.core_bh.bh_sa_person per
# MAGIC         ON mem.per_ncode = per.per_ncode          -- beneficiario: persona del MEMBER
# MAGIC     LEFT JOIN axa_col_slv_dv.core_bh.bh_sa_institution ins
# MAGIC         ON aco.ins_ncode = ins.ins_ncode
# MAGIC     LEFT JOIN residencial res
# MAGIC         ON mem.per_ncode = res.res_per_ncode      -- dirección del beneficiario
# MAGIC     LEFT JOIN ciudad
# MAGIC         ON res.ciu_res_codigo = ciudad.ciu_res_codigo
# MAGIC )
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- PASO 5: Resultado final — titulares + beneficiarios
# MAGIC -- UNION ALL apila ambos conjuntos. La columna 'rol' identifica
# MAGIC -- de qué tipo es cada fila: TITULAR o BENEFICIARIO.
# MAGIC -- ============================================================
# MAGIC SELECT * FROM titular
# MAGIC UNION ALL
# MAGIC SELECT * FROM beneficiario

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validación — Verificar el resultado
# MAGIC
# MAGIC Ejecuta las siguientes celdas para confirmar que el satélite quedó cargado correctamente.

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Conteo de filas por rol
# MAGIC SELECT
# MAGIC     rol,
# MAGIC     COUNT(*) AS total_filas
# MAGIC FROM uc_axa_cli.silver.sat_beyond_health
# MAGIC GROUP BY rol
# MAGIC ORDER BY rol

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Total de filas y columnas del satélite
# MAGIC SELECT COUNT(*) AS total_filas
# MAGIC FROM uc_axa_cli.silver.sat_beyond_health

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Vista previa de los primeros 5 registros
# MAGIC SELECT
# MAGIC     mem_ncode,
# MAGIC     tipo_documento,
# MAGIC     numero_documento,
# MAGIC     rol,
# MAGIC     dir_res,
# MAGIC     ciudad_residencia
# MAGIC FROM uc_axa_cli.silver.sat_beyond_health
# MAGIC LIMIT 5
