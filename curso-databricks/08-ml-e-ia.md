# Módulo 8 — Machine Learning e IA

## 8.1 El ciclo en Databricks
Datos (Delta/UC) → Features → Entrenamiento (con seguimiento en **MLflow**) → Registro en **Unity Catalog** → **Model Serving** o inferencia por lotes → Monitorización.

## 8.2 MLflow
```python
import mlflow
mlflow.set_registry_uri("databricks-uc")      # registrar los modelos en Unity Catalog
mlflow.autolog()                              # registra parámetros, métricas y el modelo automáticamente

with mlflow.start_run(run_name="rf_v1"):
    modelo.fit(X_train, y_train)
    mlflow.log_metric("rmse_test", rmse)
```
- **Experiments**: comparar ejecuciones (métricas, parámetros, gráficos) en la interfaz.
- **Model Registry en UC**: `catalogo.esquema.modelo`, con versiones y **alias** (`@champion`, `@challenger`) en lugar de las antiguas *stages*.
- Cargar un modelo: `mlflow.pyfunc.load_model("models:/workspace.curso_db.modelo@champion")`.
- Inferencia por lotes en Spark: `mlflow.pyfunc.spark_udf(spark, uri)`.

## 8.3 Otras herramientas
| Herramienta | Qué hace |
|---|---|
| **AutoML** | Entrena varios modelos automáticamente y genera los notebooks |
| **Feature Engineering en UC** | Tablas de features reutilizables, *point-in-time lookups* |
| **Model Serving** | Endpoint REST con escalado automático (hasta cero) para tus modelos |
| **Foundation Model APIs** | LLMs (Llama, Claude, GPT, etc.) listos para usar por API o `ai_query` |
| **AI Playground** | Probar y comparar LLMs y prompts desde la interfaz |
| **Vector Search** | Índices vectoriales sobre tablas Delta, para RAG |
| **Agent Framework + MLflow Tracing/Evaluation** | Construir, trazar y evaluar agentes y apps de IA generativa |
| **Lakehouse Monitoring** | Detectar deriva y calidad en tablas y modelos |
| **Databricks Apps** | Publicar apps (Streamlit, Dash, Gradio, Flask) dentro de Databricks |

➡️ Lab: `06_mlflow`.
