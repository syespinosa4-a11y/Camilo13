# Curso práctico de Databricks: de cero a completo

Curso de 9 módulos con laboratorio. Cada módulo tiene su notebook, que ejecutas en un workspace real **gratis**.
Duración estimada: **15–20 horas** (unas 2 semanas a 1–2 h/día).

| # | Módulo | Qué aprendes | Notebook del lab |
|---|--------|--------------|-----|
| 1 | [La plataforma](01-plataforma.md) | Lakehouse, workspace, compute, cuenta gratis | `00_setup` |
| 2 | [Notebooks y Spark](02-notebooks-y-spark.md) | DataFrames, Spark SQL, `display`, `dbutils`, widgets | `01_spark_basico` |
| 3 | [Delta Lake](03-delta-lake.md) | ACID, MERGE, time travel, optimización | `02_delta_lake` |
| 4 | [Unity Catalog](04-unity-catalog.md) | Catálogos, esquemas, volúmenes, permisos, linaje | `00_setup` + ejercicios |
| 5 | [Ingesta y pipelines](05-ingesta-y-pipelines.md) | Auto Loader, arquitectura medallón, Lakeflow Declarative Pipelines | `03_autoloader_medallon`, `04_pipeline_declarativo` |
| 6 | [Orquestación con Jobs](06-jobs.md) | Tareas, dependencias, parámetros, triggers | ejercicio de Jobs |
| 7 | [Databricks SQL y BI](07-sql-y-bi.md) | SQL warehouses, dashboards, Genie, alertas | `05_sql_analitica` |
| 8 | [Machine Learning e IA](08-ml-e-ia.md) | MLflow, registro de modelos, serving, funciones de IA | `06_mlflow` |
| 9 | [DevOps y buenas prácticas](09-devops-y-buenas-practicas.md) | Git folders, CLI, Asset Bundles, costes, certificaciones | `databricks.yml` + `resources/` |

📋 [Chuleta](chuleta.md) · 🧪 [Laboratorio](laboratorio/README.md)

## Cómo estudiar
1. Lee el módulo (20–30 min).
2. Ejecuta su notebook **celda por celda**, leyendo los comentarios. No te limites a darle a "Run all".
3. Haz los **retos** que hay al final de cada notebook y en la guía del laboratorio.
4. Cuando termines, prepárate la certificación **Databricks Certified Data Engineer Associate** (módulo 9).

## El mapa mental de Databricks
```
            ┌─────────────── Unity Catalog (gobierno: permisos, linaje, auditoría) ───────────────┐
 Fuentes ─► │ Ingesta (Auto Loader, Lakeflow Connect) ─► Delta Lake: Bronze ─► Silver ─► Gold     │
            │        transformaciones con Spark / SQL / Lakeflow Declarative Pipelines            │
            │        orquestadas con Lakeflow Jobs                                                 │
            └──► Consumo: Databricks SQL + Dashboards + Genie │ ML/IA: MLflow, Model Serving ──────┘
                        Todo sobre compute (serverless o clústeres) y almacenamiento en la nube
```
Si entiendes este diagrama y sabes hacer cada flecha, **sabes Databricks**.

> Nota: Databricks cambia rápido y renombra productos (p. ej. *Delta Live Tables* → *Lakeflow Declarative Pipelines*, *Workflows* → *Lakeflow Jobs*). Si ves un nombre distinto en la interfaz, suele ser el mismo concepto. Ante la duda, consulta docs.databricks.com.
