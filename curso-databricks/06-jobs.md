# Módulo 6 — Orquestación con Lakeflow Jobs

## 6.1 Conceptos
- **Job**: un flujo de trabajo programado. Contiene **tareas** con dependencias (un DAG).
- **Tipos de tarea**: notebook, script Python, wheel, SQL (consulta, archivo o dashboard), pipeline, dbt, otro job, *If/else* y *For each*.
- **Compute**: serverless (recomendado) o job cluster. Evita usar clústeres all-purpose en producción (son más caros).
- **Triggers**: horario (cron), llegada de archivos (*file arrival*), actualización de una tabla, continuo o manual.

## 6.2 Pasar datos entre tareas
- **Parámetros del job** → llegan a los widgets del notebook: `dbutils.widgets.get("fecha")`.
- **Valores dinámicos**: `{{job.start_time.iso_date}}`, `{{job.run_id}}`.
- **Task values**: una tarea publica un valor y otra lo lee:
  ```python
  dbutils.jobs.taskValues.set(key="filas", value=123)                      # tarea A
  dbutils.jobs.taskValues.get(taskKey="A", key="filas", default=0)         # tarea B
  ```

## 6.3 Operación
- **Reintentos**, timeouts, número máximo de ejecuciones simultáneas.
- **Notificaciones** por email, Slack o webhook al empezar, fallar o terminar.
- **Repair run**: vuelve a ejecutar solo las tareas que fallaron.
- Historial de ejecuciones con la salida de cada tarea; monitorización de costes con `system.lakeflow` y `system.billing`.
- Define los jobs como código con **Asset Bundles** (módulo 9); la interfaz permite exportarlos a YAML.

➡️ Lab: ejercicio de Jobs en la guía del laboratorio.
