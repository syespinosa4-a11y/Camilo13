# Databricks notebook source
# MAGIC %md
# MAGIC # sat_beyond_health — Satélite Beyond Health
# MAGIC
# MAGIC Construye el satélite Beyond Health consolidando titulares y beneficiarios
# MAGIC desde las tablas fuente del sistema BH (core_bh).
# MAGIC
# MAGIC ## Roles
# MAGIC - **TITULAR**: `mem.per_ncode = aco.per_ncode`
# MAGIC - **BENEFICIARIO**: `mem.per_ncode <> aco.per_ncode`
# MAGIC
# MAGIC ## Tablas fuente
# MAGIC | Tabla | Contenido |
# MAGIC |---|---|
# MAGIC | `bh_sa_member` | Miembros del contrato |
# MAGIC | `bh_sa_affiliation_contract` | Contratos de afiliación |
# MAGIC | `bh_sa_person` | Datos personales |
# MAGIC | `bh_sa_institution` | Institución (EPS) |
# MAGIC | `bh_sa_address` | Direcciones (lty_ncode=1 → residencial) |
# MAGIC | `bh_sa_city` | Catálogo ciudades → departamento/país |
# MAGIC
# MAGIC ## Tabla destino
# MAGIC `axa_col_slv_dv.stg_cliente.sat_beyond_health`

# COMMAND ----------
# MAGIC %md
# MAGIC ## Ejecución — Crear el satélite Beyond Health

# COMMAND ----------
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE axa_col_slv_dv.stg_cliente.sat_beyond_health
# MAGIC USING DELTA AS
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- PASO 1: Dirección residencial por persona
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
# MAGIC -- PASO 2: Ciudad y país
# MAGIC -- dep_ncode presente → ciudad colombiana → Colombia
# MAGIC -- dep_ncode ausente  → ciudad extranjera → Exterior
# MAGIC -- ============================================================
# MAGIC ciudad AS (
# MAGIC     SELECT
# MAGIC         c.cit_clegalcode        AS ciu_res_codigo,
# MAGIC         c.cit_cname             AS ciudad_residencia,
# MAGIC         c.dep_ncode             AS departamento,
# MAGIC         CASE
# MAGIC             WHEN c.dep_ncode IS NOT NULL THEN 'Colombia'
# MAGIC             ELSE 'Exterior'
# MAGIC         END                     AS pais
# MAGIC     FROM axa_col_slv_dv.core_bh.bh_sa_city c
# MAGIC ),
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- PASO 3: Titulares (mem.per_ncode = aco.per_ncode)
# MAGIC -- ============================================================
# MAGIC titular AS (
# MAGIC     SELECT
# MAGIC         mem.mem_ncode                           AS mem_ncode,
# MAGIC         mem.per_ncode                           AS mem_per_ncode,
# MAGIC         mem.aco_ncode                           AS mem_aco_ncode,
# MAGIC         aco.aco_ncode                           AS aco_ncode,
# MAGIC         aco.per_ncode                           AS aco_per_ncode,
# MAGIC         aco.ins_ncode                           AS aco_ins_ncode,
# MAGIC         per.per_ncode                           AS per_ncode,
# MAGIC         ins.ins_ncode                           AS ins_ncode,
# MAGIC         per.TID_NCODE                           AS tipo_documento,
# MAGIC         per.PER_CIDENTIFICATIONNUMBER           AS numero_documento,
# MAGIC         per.PER_CFIRSTNAME                      AS primer_nombre,
# MAGIC         per.PER_CLASTNAME                       AS primer_apellido,
# MAGIC         per.PER_CMIDDLENAME                     AS segundo_nombre,
# MAGIC         per.PER_CMOTHERNAME                     AS segundo_apellido,
# MAGIC         COALESCE(per.PER_CEMAIL, per.PER_CMAIL) AS email,
# MAGIC         per.PER_CMOBILEPHONE                    AS celular,
# MAGIC         per.PER_DBIRTHDATE                      AS fecha_nacimiento,
# MAGIC         per.PER_CGENDER                         AS genero,
# MAGIC         per.PER_BAUTHPERSONALINFO               AS ATDP,
# MAGIC         per.EAC_NCODE                           AS actividad_economica,
# MAGIC         per.MST_NCODE                           AS estado_civil,
# MAGIC         per.FECHA_CARGUE                        AS per_fecha_cargue,
# MAGIC         ins.INS_CNAME                           AS nombre_completo_razon_social,
# MAGIC         ins.INS_CLEGALCODE                      AS eps,
# MAGIC         ins.INS_BEMAIL_SEND                     AS preferencia_contacto_email,
# MAGIC         ins.INS_BSMS_SEND                       AS preferencia_contacto_sms,
# MAGIC         res.dir_res                             AS direccion_residencial,
# MAGIC         res.ciu_res_codigo                      AS codigo_ciudad,
# MAGIC         ciudad.ciudad_residencia,
# MAGIC         ciudad.departamento,
# MAGIC         ciudad.pais,
# MAGIC         aco.pla_ncode                           AS plan,
# MAGIC         aco.ACO_CONTRACTCODE                    AS contrato,
# MAGIC         CASE
# MAGIC             WHEN per.per_ncode IS NOT NULL THEN 'Natural'
# MAGIC             ELSE 'Juridica'
# MAGIC         END                                     AS tipo_persona,
# MAGIC         CASE
# MAGIC             WHEN aco.cty_ncode != 5
# MAGIC              AND mem.MEM_DSTARTINGDATE <= CURRENT_DATE()
# MAGIC              AND mem.MEM_DENDINGDATE   >= CURRENT_DATE()
# MAGIC             THEN 'Activo'
# MAGIC             ELSE 'No Activo'
# MAGIC         END                                     AS estado,
# MAGIC         mem.MEM_DSTARTINGDATE                   AS fecha_inicio_vigencia,
# MAGIC         mem.MEM_DENDINGDATE                     AS fecha_fin_vigencia,
# MAGIC         CASE aco.CTI_NCODE
# MAGIC             WHEN 1 THEN 'FAMILIAR'
# MAGIC             WHEN 3 THEN 'COLECTIVO'
# MAGIC         END                                     AS tipo_contrato,
# MAGIC         CONCAT(per.TID_NCODE, per.PER_CIDENTIFICATIONNUMBER) AS llave_negocio,
# MAGIC         'TITULAR'                               AS rol
# MAGIC     FROM axa_col_slv_dv.core_bh.bh_sa_member mem
# MAGIC     INNER JOIN axa_col_slv_dv.core_bh.bh_sa_affiliation_contract aco
# MAGIC         ON mem.aco_ncode = aco.aco_ncode
# MAGIC     LEFT JOIN axa_col_slv_dv.core_bh.bh_sa_person per
# MAGIC         ON aco.per_ncode = per.per_ncode
# MAGIC     LEFT JOIN axa_col_slv_dv.core_bh.bh_sa_institution ins
# MAGIC         ON aco.ins_ncode = ins.ins_ncode
# MAGIC     LEFT JOIN residencial res
# MAGIC         ON aco.per_ncode = res.res_per_ncode
# MAGIC     LEFT JOIN ciudad
# MAGIC         ON res.ciu_res_codigo = ciudad.ciu_res_codigo
# MAGIC     WHERE mem.per_ncode = aco.per_ncode
# MAGIC       AND per.FECHA_CARGUE >= ADD_MONTHS(CURRENT_DATE(), -6)
# MAGIC ),
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- PASO 4: Beneficiarios (mem.per_ncode <> aco.per_ncode)
# MAGIC -- ============================================================
# MAGIC beneficiario AS (
# MAGIC     SELECT
# MAGIC         mem.mem_ncode                           AS mem_ncode,
# MAGIC         mem.per_ncode                           AS mem_per_ncode,
# MAGIC         mem.aco_ncode                           AS mem_aco_ncode,
# MAGIC         aco.aco_ncode                           AS aco_ncode,
# MAGIC         aco.per_ncode                           AS aco_per_ncode,
# MAGIC         aco.ins_ncode                           AS aco_ins_ncode,
# MAGIC         per.per_ncode                           AS per_ncode,
# MAGIC         ins.ins_ncode                           AS ins_ncode,
# MAGIC         per.TID_NCODE                           AS tipo_documento,
# MAGIC         per.PER_CIDENTIFICATIONNUMBER           AS numero_documento,
# MAGIC         per.PER_CFIRSTNAME                      AS primer_nombre,
# MAGIC         per.PER_CLASTNAME                       AS primer_apellido,
# MAGIC         per.PER_CMIDDLENAME                     AS segundo_nombre,
# MAGIC         per.PER_CMOTHERNAME                     AS segundo_apellido,
# MAGIC         COALESCE(per.PER_CEMAIL, per.PER_CMAIL) AS email,
# MAGIC         per.PER_CMOBILEPHONE                    AS celular,
# MAGIC         per.PER_DBIRTHDATE                      AS fecha_nacimiento,
# MAGIC         per.PER_CGENDER                         AS genero,
# MAGIC         per.PER_BAUTHPERSONALINFO               AS ATDP,
# MAGIC         per.EAC_NCODE                           AS actividad_economica,
# MAGIC         per.MST_NCODE                           AS estado_civil,
# MAGIC         per.FECHA_CARGUE                        AS per_fecha_cargue,
# MAGIC         ins.INS_CNAME                           AS nombre_completo_razon_social,
# MAGIC         ins.INS_CLEGALCODE                      AS eps,
# MAGIC         ins.INS_BEMAIL_SEND                     AS preferencia_contacto_email,
# MAGIC         ins.INS_BSMS_SEND                       AS preferencia_contacto_sms,
# MAGIC         res.dir_res                             AS direccion_residencial,
# MAGIC         res.ciu_res_codigo                      AS codigo_ciudad,
# MAGIC         ciudad.ciudad_residencia,
# MAGIC         ciudad.departamento,
# MAGIC         ciudad.pais,
# MAGIC         aco.pla_ncode                           AS plan,
# MAGIC         aco.ACO_CONTRACTCODE                    AS contrato,
# MAGIC         CASE
# MAGIC             WHEN per.per_ncode IS NOT NULL THEN 'Natural'
# MAGIC             ELSE 'Juridica'
# MAGIC         END                                     AS tipo_persona,
# MAGIC         CASE
# MAGIC             WHEN aco.cty_ncode != 5
# MAGIC              AND mem.MEM_DSTARTINGDATE <= CURRENT_DATE()
# MAGIC              AND mem.MEM_DENDINGDATE   >= CURRENT_DATE()
# MAGIC             THEN 'Activo'
# MAGIC             ELSE 'No Activo'
# MAGIC         END                                     AS estado,
# MAGIC         mem.MEM_DSTARTINGDATE                   AS fecha_inicio_vigencia,
# MAGIC         mem.MEM_DENDINGDATE                     AS fecha_fin_vigencia,
# MAGIC         CASE aco.CTI_NCODE
# MAGIC             WHEN 1 THEN 'FAMILIAR'
# MAGIC             WHEN 3 THEN 'COLECTIVO'
# MAGIC         END                                     AS tipo_contrato,
# MAGIC         CONCAT(per.TID_NCODE, per.PER_CIDENTIFICATIONNUMBER) AS llave_negocio,
# MAGIC         'BENEFICIARIO'                          AS rol
# MAGIC     FROM axa_col_slv_dv.core_bh.bh_sa_member mem
# MAGIC     INNER JOIN axa_col_slv_dv.core_bh.bh_sa_affiliation_contract aco
# MAGIC         ON mem.aco_ncode = aco.aco_ncode
# MAGIC     LEFT JOIN axa_col_slv_dv.core_bh.bh_sa_person per
# MAGIC         ON mem.per_ncode = per.per_ncode
# MAGIC     LEFT JOIN axa_col_slv_dv.core_bh.bh_sa_institution ins
# MAGIC         ON aco.ins_ncode = ins.ins_ncode
# MAGIC     LEFT JOIN residencial res
# MAGIC         ON mem.per_ncode = res.res_per_ncode
# MAGIC     LEFT JOIN ciudad
# MAGIC         ON res.ciu_res_codigo = ciudad.ciu_res_codigo
# MAGIC     WHERE mem.per_ncode <> aco.per_ncode
# MAGIC       AND per.FECHA_CARGUE >= ADD_MONTHS(CURRENT_DATE(), -6)
# MAGIC )
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- PASO 5: Resultado final — titulares + beneficiarios
# MAGIC -- ============================================================
# MAGIC SELECT * FROM titular
# MAGIC UNION ALL
# MAGIC SELECT * FROM beneficiario

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validación — Verificar el resultado

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Total de filas por rol
# MAGIC SELECT
# MAGIC     rol,
# MAGIC     COUNT(*)                         AS total_filas,
# MAGIC     COUNT(DISTINCT numero_documento) AS personas_unicas
# MAGIC FROM axa_col_slv_dv.stg_cliente.sat_beyond_health
# MAGIC GROUP BY rol
# MAGIC ORDER BY rol

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Vista previa
# MAGIC SELECT
# MAGIC     rol,
# MAGIC     tipo_documento,
# MAGIC     numero_documento,
# MAGIC     primer_nombre,
# MAGIC     primer_apellido,
# MAGIC     email,
# MAGIC     ciudad_residencia,
# MAGIC     departamento,
# MAGIC     pais,
# MAGIC     eps,
# MAGIC     plan,
# MAGIC     contrato,
# MAGIC     tipo_persona,
# MAGIC     estado,
# MAGIC     fecha_inicio_vigencia,
# MAGIC     fecha_fin_vigencia,
# MAGIC     per_fecha_cargue
# MAGIC FROM axa_col_slv_dv.stg_cliente.sat_beyond_health
# MAGIC LIMIT 5
