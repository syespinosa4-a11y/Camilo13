# Módulo 9 — DevOps, buenas prácticas y certificación

## 9.1 Git folders
Clona repos de GitHub, GitLab o Azure DevOps dentro del workspace: crear ramas, commit, push, pull y merge desde la interfaz.
Guarda los notebooks como archivos fuente (`.py`/`.sql`), que se revisan bien en PRs.

## 9.2 Databricks CLI
```bash
# instalar: https://docs.databricks.com/dev-tools/cli/install  (brew, winget o script curl)
databricks auth login --host https://<tu-workspace>.cloud.databricks.com
databricks workspace list /Users/tu@email.com
databricks fs ls dbfs:/Volumes/workspace/curso_db/raw
databricks jobs list
databricks clusters list
```
También existen el **SDK de Python** (`databricks-sdk`), la API REST, **Databricks Connect** (ejecutar Spark desde tu IDE local), la extensión de VS Code y el proveedor de **Terraform**.

## 9.3 Databricks Asset Bundles (DABs)
Infraestructura como código: jobs, pipelines, dashboards y modelos definidos en YAML junto a tu código, desplegados a varios entornos.
```bash
databricks bundle init            # plantilla
databricks bundle validate
databricks bundle deploy -t dev   # despliega en el entorno dev
databricks bundle run -t dev nombre_job
databricks bundle deploy -t prod
```
Mira el ejemplo en [`laboratorio/databricks.yml`](laboratorio/databricks.yml) y [`laboratorio/resources/`](laboratorio/resources/). Combínalo con GitHub Actions para CI/CD: validar en cada PR y desplegar a prod al hacer merge.

## 9.4 Buenas prácticas
**Datos**
- Todo en Unity Catalog; tablas **gestionadas**; nombres de tres niveles.
- Catálogos por entorno (`dev`, `staging`, `prod`) o por dominio.
- Medallón; `MERGE` para upserts; expectations para la calidad.
- Liquid clustering en lugar de particionar; activa Predictive Optimization.

**Código**
- Evita `collect()` y `toPandas()` sobre datos grandes; evita UDFs de Python fila a fila (mejor funciones nativas o pandas UDFs).
- Nada de secretos en el código: usa `dbutils.secrets.get(scope, key)`.
- Notebooks para explorar; módulos `.py` con tests (pytest) para la lógica reutilizable.

**Costes**
- Serverless o job clusters para producción; auto-stop corto en warehouses y clústeres.
- Aplica **políticas de compute** y **tags** para atribuir el gasto; revisa `system.billing.usage`.
- Establece presupuestos y alertas.

**Rendimiento: dónde mirar**
- *Spark UI* (clústeres clásicos) o *Query Profile* (serverless/SQL): busca shuffles grandes, *skew* (una tarea tarda mucho más que las demás) y *spill*.
- Tablas pequeñas en joins → *broadcast*: `F.broadcast(df_pequeña)`.

## 9.5 Certificaciones (ruta sugerida)
1. **Databricks Fundamentals** (gratis, conceptual).
2. **Data Engineer Associate** ← la más útil tras este curso (Spark SQL, Delta, UC, pipelines, jobs).
3. **Data Analyst Associate** (SQL, dashboards) o **Machine Learning Associate**.
4. Nivel Professional (Data Engineer / ML) más adelante.

Recursos: **Databricks Academy** (cursos gratis a la carta), docs.databricks.com y la comunidad (community.databricks.com).
