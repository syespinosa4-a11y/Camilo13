# Laboratorio: la tienda de café en Databricks ☕

Construirás una plataforma de datos completa: ingesta de pedidos → Delta Lake (bronze, silver, gold) → pipeline declarativo → job orquestado → dashboard y Genie → modelo de ML → despliegue como código.

```
laboratorio/
├── notebooks/
│   ├── _config.py                 # catálogo, esquema y volumen (lo usan todos)
│   ├── 00_setup.py                # Módulos 1 y 4
│   ├── 01_spark_basico.py         # Módulo 2
│   ├── 02_delta_lake.py           # Módulo 3
│   ├── 03_autoloader_medallon.py  # Módulo 5
│   ├── 04_pipeline_declarativo.sql# Módulo 5 (se ejecuta como pipeline)
│   ├── 05_sql_analitica.sql       # Módulo 7
│   ├── 06_mlflow.py               # Módulo 8
│   └── 07_tarea_calidad.py        # Módulo 6 (tarea de un job)
├── databricks.yml                 # Asset Bundle (Módulo 9)
└── resources/                     # definición del job y del pipeline como código
```

## Preparación (una vez)
1. Crea tu cuenta en **Databricks Free Edition** (módulo 1).
2. **Workspace → Create → Git folder** → URL de este repo. (O importa la carpeta `notebooks/`.)
3. Abre `notebooks/00_setup`, conéctalo a **Serverless** y ejecútalo.

Cada notebook termina con **✅ Retos**. Hazlos: ahí está el aprendizaje de verdad.

---

## Lab 1 — Plataforma y Unity Catalog · `00_setup` ⏱ 30 min
Ejecuta el notebook y sus retos. Después, en la interfaz:
- Recorre **Catalog**, **Compute**, **Jobs & Pipelines**, **SQL Warehouses** y **Experiments**.
- En `samples.nyctaxi.trips` mira las pestañas *Overview*, *Sample data* y *Details*.

**Comprueba:** existen `workspace.curso_db` y el volumen `raw`, y sabes explicar catálogo → esquema → tabla/volumen.

## Lab 2 — Spark · `01_spark_basico` ⏱ 1 h
**Comprueba:** resuelves los 4 retos tanto en PySpark como en SQL y creas al menos 1 gráfico con `display`.

## Lab 3 — Delta Lake · `02_delta_lake` ⏱ 1 h
**Comprueba:** puedes explicar qué versión creó cada operación de `DESCRIBE HISTORY` y recuperas datos con `RESTORE`.

## Lab 4 — Permisos (Módulo 4) ⏱ 20 min
En Free Edition estás solo, pero practica la sintaxis y revisa el resultado:
```sql
CREATE VIEW curso_db.v_pedidos_bogota AS SELECT * FROM curso_db.pedidos_silver WHERE ciudad = 'Bogotá';
GRANT SELECT ON VIEW curso_db.v_pedidos_bogota TO `account users`;
SHOW GRANTS ON VIEW curso_db.v_pedidos_bogota;
-- Máscara de columna: oculta el cliente a quien no sea admin
CREATE FUNCTION curso_db.mascara_cliente(c STRING) RETURN
  CASE WHEN is_account_group_member('admins') THEN c ELSE '***' END;
ALTER TABLE curso_db.pedidos_silver ALTER COLUMN cliente SET MASK curso_db.mascara_cliente;
SELECT cliente FROM curso_db.pedidos_silver LIMIT 5;   -- ¿ves los datos o ***?
ALTER TABLE curso_db.pedidos_silver ALTER COLUMN cliente DROP MASK;
```
(Hazlo después del Lab 5, cuando ya exista `pedidos_silver`.) Consulta también `system.information_schema.table_privileges`.

## Lab 5 — Ingesta y medallón · `03_autoloader_medallon` ⏱ 1 h
Ejecútalo **3 veces** (tres lotes). Luego una vez con `generar_lote = no`.
**Comprueba:** bronze crece solo con archivos nuevos; silver no tiene duplicados; las filas malas están en `pedidos_cuarentena`.

## Lab 6 — Pipeline declarativo · `04_pipeline_declarativo` ⏱ 45 min
Crea el ETL pipeline siguiendo las instrucciones del propio archivo.
**Comprueba:** ves el grafo de 4 tablas y las métricas de las *expectations* (cuántas filas se descartaron).

## Lab 7 — Orquestación con Jobs ⏱ 45 min
1. **Jobs & Pipelines → Create → Job**.
2. Tarea 1 `ingesta_medallon`: notebook `03_autoloader_medallon`, compute Serverless.
3. Tarea 2 `control_calidad`: notebook `07_tarea_calidad`, *depends on* la tarea 1.
4. Parámetros del job: `catalogo = workspace` y `esquema = curso_db`.
5. **Run now**. Abre la ejecución y mira la salida de cada tarea y los *task values*.
6. Fuerza un fallo: pon `max_pct_cuarentena = 1` en la tarea 2 → ejecuta → usa **Repair run** tras corregirlo.
7. Añade un schedule diario y una notificación por email si falla. Luego **pausa** el schedule.
8. Extra: añade una tercera tarea que actualice el pipeline del Lab 6 (tipo *Pipeline*).

**Comprueba:** tienes un job de 2–3 tareas con dependencias, parámetros, reintentos y notificaciones, y has usado *Repair run*.

## Lab 8 — SQL, dashboards y Genie · `05_sql_analitica` ⏱ 1 h
**Comprueba:** un dashboard publicado con 4 visualizaciones y un filtro, un espacio de Genie que responde bien 3 preguntas, y una alerta.

## Lab 9 — Machine Learning · `06_mlflow` ⏱ 1 h
**Comprueba:** 3 ejecuciones comparadas en Experiments, el modelo `tarifa_taxi` en UC con alias `@champion` y la tabla `predicciones_tarifa`.

## Lab 10 — Todo como código con Asset Bundles (Módulo 9) ⏱ 1 h
En tu computadora:
```bash
# 1. Instala la CLI de Databricks (ver docs) y autentícate
databricks auth login --host https://<tu-workspace>
# 2. Edita el host en databricks.yml
cd curso-databricks/laboratorio
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run -t dev medallon_pedidos
```
**Comprueba:** en el workspace aparecen `[dev tu_usuario] medallon-pedidos` y el pipeline, desplegados sin tocar la interfaz.
Extra: crea un workflow de GitHub Actions que ejecute `databricks bundle validate` en cada PR.

---

## 🏁 Proyecto final (sin guía)
Elige un dataset público (p. ej. `samples.tpch` o un CSV de datos.gov.co) y construye tú solo:
1. Ingesta con Auto Loader o `read_files` a bronze en un volumen propio.
2. Pipeline declarativo bronze → silver → gold con al menos 3 expectations.
3. Job programado que actualice el pipeline y ejecute un control de calidad.
4. Dashboard publicado y espacio de Genie.
5. (Opcional) Un modelo registrado en UC que use tus tablas gold.
6. Todo definido en un Asset Bundle en tu repositorio.

Si lo completas, estás listo para la certificación **Data Engineer Associate**. 🎓
