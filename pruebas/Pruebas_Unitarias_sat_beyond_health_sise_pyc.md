NTT DATA 2026

# Documento de Pruebas Unitarias
## Notebook: `notebooks/sat_beyond_health.py`
### Satélites: `sat_beyond_health` y `sat_sise_pyc`

| Campo               | Valor                                                                 |
|----------------------|------------------------------------------------------------------------|
| Proyecto             | AXA Colombia — Data Vault 2.0 (Databricks / Unity Catalog)            |
| Notebook fuente       | `notebooks/sat_beyond_health.py`                                      |
| Repositorio           | syespinosa4-a11y/Camilo13                                             |
| Lenguaje              | PySpark (DataFrame API, sin SQL salvo `DROP TABLE IF EXISTS`)         |
| Tablas destino        | `uc_axa_cli.silver.sat_beyond_health`, `uc_axa_cli.silver.sat_sise_pyc` |
| Fecha de elaboración  | 2026-06-30                                                             |

---

## 1. Objetivo

Documentar las pruebas unitarias e integración aplicadas a las funciones del notebook
`sat_beyond_health.py`, que construye los satélites `sat_beyond_health` (fuente
`core_bh`) y `sat_sise_pyc` (fuente `core_sise`) bajo el patrón Data Vault 2.0,
verificando que cada función produzca el resultado esperado de forma aislada
(unitaria) y que el pipeline completo (integración) entregue una tabla Delta
consistente, deduplicada y en minúsculas.

## 2. Alcance

Cubre las funciones reutilizables de ambos satélites:

- Resolución dinámica de llaves (`llave_entidad`, `llave_comun`, `_mapa_columnas`)
- Construcción de universos (`base_entidades`, `unir_por_entidad`, `unir_por_puente`,
  `unir_por_llave_compuesta`, `universo_persona_sise`, `universo_poliza_sise`)
- Utilidades transversales (`deduplicar_columnas`, `minusculizar_columnas`,
  `_materializar`)
- Construcción y escritura final de cada satélite (`build_sat_beyond_health`,
  `build_sat_sise_pyc`, bloques de escritura Delta)

No cubre: el contenido de negocio de las tablas fuente (`core_bh`/`core_sise`), ni
la definición de `DIM_PARAMETROS` (pendiente de existir como tabla Delta).

## 3. Estrategia de prueba

| Tipo de prueba | Herramienta sugerida                | Descripción |
|-----------------|--------------------------------------|-------------|
| Unitaria        | `pytest` + `pyspark.testing` / DataFrames sintéticos en memoria (`spark.createDataFrame`) | Cada función se prueba con DataFrames pequeños construidos a mano, sin depender de Unity Catalog. |
| Integración      | Ejecución del notebook completo contra un esquema de pruebas (`core_bh`/`core_sise` con muestra de datos) | Valida que el pipeline end-to-end escriba la tabla Delta destino sin errores y con las propiedades esperadas. |

Convención de IDs: `PU-BH-##` (sat_beyond_health), `PU-SS-##` (sat_sise_pyc),
`PU-COM-##` (utilidades comunes a ambos), `PI-##` (integración).

---

## 4. Casos de prueba — Utilidades comunes

| ID | Función | Caso de prueba | Datos de entrada | Resultado esperado | Estado |
|----|----------|-----------------|--------------------|----------------------|--------|
| PU-COM-01 | `_mapa_columnas` | Mapea nombres de columna a su versión mayúscula como clave | DataFrame con columnas `Per_Ncode`, `cit_ncode` | `{"PER_NCODE": "Per_Ncode", "CIT_NCODE": "cit_ncode"}` | Pendiente |
| PU-COM-02 | `deduplicar_columnas` — caso simple | Una columna repetida exactamente dos veces | DataFrame con columnas `["A", "A"]` | Columnas resultantes `["A", "A_1"]` | Pendiente |
| PU-COM-03 | `deduplicar_columnas` — colisión con nombre real | El sufijo generado (`COD_AGENTE_1`) ya existe como columna literal | Columnas `["COD_AGENTE", "COD_AGENTE_1", "COD_AGENTE"]` | Resultado `["COD_AGENTE", "COD_AGENTE_1", "COD_AGENTE_2"]` (incrementa hasta encontrar nombre libre) | Pendiente |
| PU-COM-04 | `deduplicar_columnas` — case-insensitive | Mismo nombre lógico con distinto casing | Columnas `["aco_ncode", "ACO_NCODE"]` | Se detecta como duplicado: `["aco_ncode", "ACO_NCODE_1"]` | Pendiente |
| PU-COM-05 | `minusculizar_columnas` | Todas las columnas de salida quedan en minúscula sin importar el casing original | Columnas `["PER_NCODE", "Fec_Cargue", "id_sat_beyond_health"]` | Columnas `["per_ncode", "fec_cargue", "id_sat_beyond_health"]` | Pendiente |
| PU-COM-06 | `minusculizar_columnas` — no altera datos | Verifica que solo cambian nombres de columna, no valores | DataFrame con 3 filas de datos | Mismos valores de fila, mismo orden, solo cambia metadata de columnas | Pendiente |
| PU-COM-07 | `_materializar` | Escribe y relee un DataFrame desde una ruta Delta temporal, truncando el linaje | DataFrame con plan lógico largo (varios joins encadenados) | El DataFrame devuelto tiene un plan lógico de un solo `Scan` (linaje truncado); mismos datos que el original | Pendiente |
| PU-COM-08 | `_materializar` — sobrescritura | Llamada repetida con el mismo `nombre` sobrescribe la ruta anterior | Ejecutar dos veces con datos distintos y el mismo `nombre` | La segunda lectura refleja los datos de la segunda escritura, no acumula filas | Pendiente |

---

## 5. Casos de prueba — `sat_beyond_health`

| ID | Función | Caso de prueba | Datos de entrada | Resultado esperado | Estado |
|----|----------|-----------------|--------------------|----------------------|--------|
| PU-BH-01 | `llave_entidad` | Tabla con ambas columnas `PER_NCODE` e `INS_NCODE` | Fila con `PER_NCODE='P1'`, `INS_NCODE=NULL` | Devuelve `'P1'` (coalesce prioriza `PER_NCODE`) | Pendiente |
| PU-BH-02 | `llave_entidad` | Tabla solo con `PER_NCODE` | Fila con `PER_NCODE='P2'` | Devuelve `'P2'` | Pendiente |
| PU-BH-03 | `llave_entidad` | Tabla solo con `INS_NCODE` | Fila con `INS_NCODE='I9'` | Devuelve `'I9'` | Pendiente |
| PU-BH-04 | `llave_entidad` | Tabla sin `PER_NCODE` ni `INS_NCODE` | DataFrame con columnas `["OTRA_COL"]` | Devuelve `None` (sin lanzar excepción) | Pendiente |
| PU-BH-05 | `llave_comun` | Dos tablas con una sola columna `*_NCODE` en común, distinta de `PER_NCODE`/`INS_NCODE` | `addr` con `CIT_NCODE`, `ciu` con `cit_ncode` | Devuelve `("CIT_NCODE", "cit_ncode")` (nombres reales, case-insensitive) | Pendiente |
| PU-BH-06 | `llave_comun` | Prioriza columna específica sobre `PER_NCODE`/`INS_NCODE` cuando ambas existen | Tablas con `PER_NCODE` y `ACO_NCODE` en común | Devuelve la llave `ACO_NCODE`, no `PER_NCODE` | Pendiente |
| PU-BH-07 | `llave_comun` | Sin ninguna columna `*_NCODE` en común | Dos tablas sin columnas compartidas | Lanza `ValueError("No se encontro llave comun...")` | Pendiente |
| PU-BH-08 | `base_entidades` | Une `bh_sa_person` y `bh_sa_institution` con discriminador | DataFrames sintéticos de persona (2 filas) e institución (1 fila) | DataFrame de 3 filas; `TIPO_ENTIDAD` = `PERSONA`/`INSTITUCION` correctamente asignado; `ID_ENTIDAD_HUB` resuelto en cada fila | Pendiente |
| PU-BH-09 | `unir_por_entidad` | Join LEFT por `ID_ENTIDAD_HUB` sin duplicar la columna llave | `base` con 2 entidades, `bh_sa_address` con 1 coincidencia | Resultado de 2 filas, sin columna `ID_ENTIDAD_HUB` duplicada (ambigüedad), dirección presente solo en la fila que matchea | Pendiente |
| PU-BH-10 | `unir_por_entidad` | Entidad sin coincidencia en la tabla satélite | `base` con entidad `E1`, `bh_sa_address` sin fila para `E1` | Fila de `E1` se conserva (LEFT JOIN) con columnas de `addr` en `NULL` | Pendiente |
| PU-BH-11 | `unir_por_entidad` | Tabla sin `PER_NCODE`/`INS_NCODE` | `bh_sa_member` (no tiene llave de entidad directa) | Lanza `ValueError("...no se puede unir por entidad")` | Pendiente |
| PU-BH-12 | `unir_por_puente` | Join puente sin broadcast | `aco` (2 filas) y `bh_sa_member` (1 coincidencia) | Resultado correcto vía la llave `*_NCODE` común detectada dinámicamente | Pendiente |
| PU-BH-13 | `unir_por_puente` | `broadcast=True` aplica hint sin alterar el resultado de datos | Mismo caso que PU-BH-12 con `broadcast=True` | Mismos datos resultantes; plan físico contiene `BroadcastHashJoin` | Pendiente |
| PU-BH-14 | `build_sat_beyond_health` (integración de función) | Pipeline completo con datos sintéticos pequeños para las 6 tablas BH | Mini-set de persona, institución, address, affiliation_contract, member, city | DataFrame final con una fila por entidad, columnas de `addr`, `aco`, `mem`, `ciu` correctamente unidas | Pendiente |
| PU-BH-15 | Bloque final — PK y fechas | Verifica generación de `id_sat_beyond_health`, `dv_load_date`, `fecha_creacion` | Resultado de `build_sat_beyond_health()` deduplicado | `id_sat_beyond_health` único por fila (monotonically_increasing_id), `dv_load_date` = `LOAD_TS`, `fecha_creacion` es `DATE` derivada de `LOAD_TS` | Pendiente |
| PU-BH-16 | Bloque final — minúsculas | Todas las columnas de salida están en minúscula | Esquema de `df_resultado` tras `minusculizar_columnas` | Ningún nombre de columna contiene mayúsculas | Pendiente |
| PU-BH-17 | Bloque final — sin columnas duplicadas | Esquema final no tiene nombres repetidos (case-insensitive) | Esquema de `df_resultado` final | `len(columnas) == len(set(c.upper() for c in columnas))` | Pendiente |
| PI-01 | Integración — escritura Delta | Ejecución completa del bloque `sat_beyond_health` contra esquema de pruebas | Tablas `core_bh.*` con muestra de datos | Tabla `uc_axa_cli.silver.sat_beyond_health` creada/sobrescrita sin error; `spark.table(destino).count() > 0`; propiedades `TBLPROPERTIES` aplicadas (CDF, Iceberg compat, sin Deletion Vectors) | Pendiente |

---

## 6. Casos de prueba — `sat_sise_pyc`

| ID | Función | Caso de prueba | Datos de entrada | Resultado esperado | Estado |
|----|----------|-----------------|--------------------|----------------------|--------|
| PU-SS-01 | `unir_por_llave_compuesta` | Llave simple de una columna | `personas` y `dir`, ambas con `ID_PERSONA` | Join correcto por `ID_PERSONA`; columna llave del lado derecho descartada tras el join | Pendiente |
| PU-SS-02 | `unir_por_llave_compuesta` | Llave compuesta de 3 columnas | `dir` y `ss_tmunicipio` con `COD_MUNICIPIO`, `FEC_ACTUALIZACION`, `FECHA_CARGUE` | Condición `AND` sobre las 3 columnas; solo matchea cuando las 3 coinciden | Pendiente |
| PU-SS-03 | `unir_por_llave_compuesta` | Resolución case-insensitive del nombre real de columna en el lado derecho | Izquierda `COD_ASEG`, derecha `cod_aseg` | Join correcto sin lanzar `[COLUMN_ALREADY_EXISTS]`; columna derecha descartada | Pendiente |
| PU-SS-04 | `unir_por_llave_compuesta` | `alias_izq` ancla la referencia cuando hay columnas repetidas aguas arriba | DataFrame izquierdo con `FEC_ACTUALIZACION` proveniente de 2 alias distintos (`dir`, `age`) | Sin `[AMBIGUOUS_REFERENCE]`: la condición usa específicamente `izq["dir.FEC_ACTUALIZACION"]` | Pendiente |
| PU-SS-05 | `unir_por_llave_compuesta` | `broadcast=True` no altera el resultado de datos, solo el plan físico | Catálogo pequeño (`ss_tpais`) unido con `broadcast=True` | Mismos datos que sin broadcast; plan físico con `BroadcastHashJoin` | Pendiente |
| PU-SS-06 | `unir_por_llave_compuesta` | Tipo de join distinto a `left` | `tipo="inner"` con filas sin coincidencia en ambos lados | Solo se conservan las filas que matchean en ambos DataFrames | Pendiente |
| PU-SS-07 | `universo_persona_sise` | Telefonos NO se deduplican (fan-out intencional) | Persona con 2 filas en `ss_mpersona_telef` | Resultado final con 2 filas para esa persona, repitiendo el resto de columnas | Pendiente |
| PU-SS-08 | `universo_persona_sise` | Puente geográfico anclado a `dir`, no al universo completo | `dir` con múltiples columnas `FEC_ACTUALIZACION` ya presentes desde otros joins | Joins contra `ss_tmunicipio`/`ss_tpais`/`ss_tdpto` se resuelven sin error de ambigüedad | Pendiente |
| PU-SS-09 | `universo_poliza_sise` | Rama SV y SG se deduplican individualmente antes del `unionByName` | `sv` y `sg` con columna `COD_ASEG` repetida internamente en cada rama | Cada rama queda sin columnas duplicadas antes del union; `unionByName(allowMissingColumns=True)` no falla | Pendiente |
| PU-SS-10 | `universo_poliza_sise` | Discriminador `TIPO_POLIZA` correcto | Una póliza de rama SV y otra de rama SG | Resultado con `TIPO_POLIZA='SV'` y `'SG'` respectivamente, una sola vez cada una (no ambas) | Pendiente |
| PU-SS-11 | `build_sat_sise_pyc` | Puente final personas↔pólizas por `COD_ASEG`, cada universo dedup. antes del puente | Universos `personas`/`polizas` sintéticos con `COD_ASEG` repetido entre tablas internas | Join final sin `[COLUMN_ALREADY_EXISTS]`; columna `COD_ASEG` del lado derecho descartada | Pendiente |
| PU-SS-12 | Bloque final — PK y fechas | Verifica `id_sat_sise_pyc`, `dv_load_date`, `fecha_creacion` | Resultado de `build_sat_sise_pyc()` deduplicado | Mismas validaciones que PU-BH-15, aplicadas a `CONFIG_SISE` | Pendiente |
| PU-SS-13 | Bloque final — minúsculas | Todas las columnas de salida en minúscula | Esquema de `df_resultado_sise` tras `minusculizar_columnas` | Ningún nombre de columna contiene mayúsculas | Pendiente |
| PU-SS-14 | `ss_tciiu` excluida | Verifica que la tabla excluida por instrucción del usuario no aparece en el pipeline | `TABLAS_SISE` y funciones de carga | `"ss_tciiu"` no está en `TABLAS_SISE`; no se referencia en ningún join | Pendiente |
| PI-02 | Integración — escritura Delta | Ejecución completa del bloque `sat_sise_pyc` contra esquema de pruebas | Tablas `core_sise.*` con muestra de datos | Tabla `uc_axa_cli.silver.sat_sise_pyc` creada/sobrescrita sin error; conteo post-write `> 0`; mismas `TBLPROPERTIES` que `sat_beyond_health` | Pendiente |
| PI-03 | Integración — regresión de errores ya corregidos | Re-ejecutar el pipeline completo end-to-end | Datos reales/de prueba representativos | No reaparecen `[AMBIGUOUS_REFERENCE]` (`FEC_ACTUALIZACION`), `[COLUMN_ALREADY_EXISTS]` (`cod_aseg`, `cod_agente_1`) ni `[JVM_ATTRIBUTE_NOT_SUPPORTED]` (uso de `sparkContext` en shared cluster) | Pendiente |
| PI-04 | Integración — rendimiento | Tiempo de ejecución del notebook completo (ambos satélites) | Cluster compartido Unity Catalog, datos reales | Tiempo de ejecución dentro del SLA acordado con el equipo (referencia: < 2 horas, valor anterior antes de las optimizaciones de broadcast/materialización) | Pendiente |

---

## 7. Trazabilidad con incidencias resueltas

| Incidencia original | Caso(s) de regresión asociados |
|----------------------|----------------------------------|
| `[AMBIGUOUS_REFERENCE] FEC_ACTUALIZACION is ambiguous` | PU-SS-04, PI-03 |
| `[COLUMN_ALREADY_EXISTS] cod_aseg already exists` (3 ocurrencias) | PU-SS-03, PU-SS-09, PU-SS-11, PI-03 |
| `[COLUMN_ALREADY_EXISTS] cod_agente_1 already exists` | PU-COM-03, PI-03 |
| `[JVM_ATTRIBUTE_NOT_SUPPORTED]` (`sparkContext.setCheckpointDir` en shared cluster) | PU-COM-07, PU-COM-08, PI-04 |
| Tiempo de ejecución excesivo (~2 horas) | PI-04 |

## 8. Notas y pendientes de validación con negocio

- `build_sat_sise_pyc`: el puente persona↔póliza por `COD_ASEG` y la unión SV/SG por
  `UNION` con discriminador `TIPO_POLIZA` son inferencias del desarrollo (no
  confirmadas por el usuario); los casos PU-SS-10 y PU-SS-11 deben re-ejecutarse con
  datos reales para confirmar que el conteo de filas es el esperado por negocio.
- `ss_tciiu` permanece excluida (PU-SS-14) hasta confirmación del equipo técnico.

---

Script de referencia: https://github.com/syespinosa4-a11y/Camilo13/blob/claude/epic-sagan-vyb5rx/notebooks/sat_beyond_health.py
