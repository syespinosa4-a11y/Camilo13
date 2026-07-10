-- ============================================================
-- FACT_RESUMEN_REPORTE_CALIDAD — Métricas por ámbito
-- Tabla fuente: uc_axa_cli.gold.fact_resumen_reporte_calidad
-- Ámbitos: Regla, Grupo Regla, Cliente, Atributo, Fuente
-- ============================================================

-- porcentaje_calidad siempre calculado: cant_validos / cant_evaluados * 100
-- ── Vista general: una fila por ámbito con totales ──────────
SELECT
    ambito,
    COUNT(DISTINCT descripcion_ambito)              AS total_elementos,
    SUM(cant_evaluados)                             AS total_evaluados,
    SUM(cant_validos)                               AS total_validos,
    SUM(cant_invalidos)                             AS total_invalidos,
    SUM(cant_remediados)                            AS total_remediados,
    ROUND(
        SUM(cant_validos) * 100.0
        / NULLIF(SUM(cant_evaluados), 0)
    , 2)                                            AS porcentaje_calidad
FROM  uc_axa_cli.gold.fact_resumen_reporte_calidad
GROUP BY ambito
ORDER BY
    CASE ambito
        WHEN 'Fuente'      THEN 1
        WHEN 'Cliente'     THEN 2
        WHEN 'Grupo Regla' THEN 3
        WHEN 'Atributo'    THEN 4
        WHEN 'Regla'       THEN 5
        ELSE                    6
    END;

-- ============================================================
-- DETALLE POR ÁMBITO (una sección por cada uno)
-- ============================================================

-- ── 1. FUENTE (satélite) ─────────────────────────────────────
SELECT
    'Fuente'            AS ambito,
    descripcion_ambito  AS fuente,
    SUM(cant_evaluados) AS evaluados,
    SUM(cant_validos)   AS validos,
    SUM(cant_invalidos) AS invalidos,
    SUM(cant_remediados)AS remediados,
    ROUND(
        SUM(cant_validos) * 100.0
        / NULLIF(SUM(cant_evaluados), 0)
    , 2)                AS porcentaje_calidad
FROM  uc_axa_cli.gold.fact_resumen_reporte_calidad
WHERE ambito = 'Fuente'
GROUP BY descripcion_ambito
ORDER BY porcentaje_calidad ASC;   -- los más críticos primero

-- ── 2. CLIENTE (fk_hub_cliente) ──────────────────────────────
SELECT
    'Cliente'           AS ambito,
    descripcion_ambito  AS cliente,
    SUM(cant_evaluados) AS evaluados,
    SUM(cant_validos)   AS validos,
    SUM(cant_invalidos) AS invalidos,
    SUM(cant_remediados)AS remediados,
    ROUND(
        SUM(cant_validos) * 100.0
        / NULLIF(SUM(cant_evaluados), 0)
    , 2)                AS porcentaje_calidad
FROM  uc_axa_cli.gold.fact_resumen_reporte_calidad
WHERE ambito = 'Cliente'
GROUP BY descripcion_ambito
ORDER BY porcentaje_calidad ASC;

-- ── 3. GRUPO REGLA ───────────────────────────────────────────
SELECT
    'Grupo Regla'       AS ambito,
    descripcion_ambito  AS grupo_regla,
    SUM(cant_evaluados) AS evaluados,
    SUM(cant_validos)   AS validos,
    SUM(cant_invalidos) AS invalidos,
    SUM(cant_remediados)AS remediados,
    ROUND(
        SUM(cant_validos) * 100.0
        / NULLIF(SUM(cant_evaluados), 0)
    , 2)                AS porcentaje_calidad
FROM  uc_axa_cli.gold.fact_resumen_reporte_calidad
WHERE ambito = 'Grupo Regla'
GROUP BY descripcion_ambito
ORDER BY porcentaje_calidad ASC;

-- ── 4. ATRIBUTO ──────────────────────────────────────────────
SELECT
    'Atributo'          AS ambito,
    descripcion_ambito  AS atributo,
    SUM(cant_evaluados) AS evaluados,
    SUM(cant_validos)   AS validos,
    SUM(cant_invalidos) AS invalidos,
    SUM(cant_remediados)AS remediados,
    ROUND(
        SUM(cant_validos) * 100.0
        / NULLIF(SUM(cant_evaluados), 0)
    , 2)                AS porcentaje_calidad
FROM  uc_axa_cli.gold.fact_resumen_reporte_calidad
WHERE ambito = 'Atributo'
GROUP BY descripcion_ambito
ORDER BY porcentaje_calidad ASC;

-- ── 5. REGLA (pk_regla_calidad) ──────────────────────────────
SELECT
    'Regla'             AS ambito,
    descripcion_ambito  AS regla,
    SUM(cant_evaluados) AS evaluados,
    SUM(cant_validos)   AS validos,
    SUM(cant_invalidos) AS invalidos,
    SUM(cant_remediados)AS remediados,
    ROUND(
        SUM(cant_validos) * 100.0
        / NULLIF(SUM(cant_evaluados), 0)
    , 2)                AS porcentaje_calidad
FROM  uc_axa_cli.gold.fact_resumen_reporte_calidad
WHERE ambito = 'Regla'
GROUP BY descripcion_ambito
ORDER BY porcentaje_calidad ASC;

-- ============================================================
-- VISTA PIVOTEADA: todos los ámbitos en un solo resultado
-- Útil para exportar a Excel o alimentar un dashboard
-- ============================================================
SELECT
    ambito,
    descripcion_ambito,
    cant_evaluados,
    cant_validos,
    cant_invalidos,
    cant_remediados,
    ROUND(
        cant_validos * 100.0
        / NULLIF(cant_evaluados, 0)
    , 2)                AS porcentaje_calidad,
    CASE
        WHEN ROUND(cant_validos * 100.0 / NULLIF(cant_evaluados, 0), 2) >= 95 THEN 'VERDE'
        WHEN ROUND(cant_validos * 100.0 / NULLIF(cant_evaluados, 0), 2) >= 80 THEN 'AMARILLO'
        ELSE 'ROJO'
    END                 AS semaforo_calidad
FROM  uc_axa_cli.gold.fact_resumen_reporte_calidad
ORDER BY
    CASE ambito
        WHEN 'Fuente'      THEN 1
        WHEN 'Cliente'     THEN 2
        WHEN 'Grupo Regla' THEN 3
        WHEN 'Atributo'    THEN 4
        WHEN 'Regla'       THEN 5
        ELSE                    6
    END,
    porcentaje_calidad ASC;
