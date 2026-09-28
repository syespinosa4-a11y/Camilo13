# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Machine Learning con MLflow (Módulo 8)
# MAGIC Entrenamos un modelo que **predice la tarifa de un taxi**, registramos los experimentos con MLflow, guardamos el modelo en Unity Catalog y hacemos predicciones por lotes con Spark.

# COMMAND ----------

# MAGIC %pip install -q -U mlflow scikit-learn

# COMMAND ----------

dbutils.library.restartPython()   # reinicia Python para usar las versiones recién instaladas

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F

datos = (spark.read.table("samples.nyctaxi.trips")
    .withColumn("duracion_min",
                (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60)
    .withColumn("hora", F.hour("tpep_pickup_datetime"))
    .withColumn("dia_semana", F.dayofweek("tpep_pickup_datetime"))
    .filter("trip_distance > 0 AND fare_amount > 0 AND fare_amount < 200 AND duracion_min BETWEEN 1 AND 180")
    .select("trip_distance", "duracion_min", "hora", "dia_semana", "fare_amount"))

# Datos pequeños → pandas para scikit-learn (con datos grandes usarías Spark ML o entrenamiento distribuido)
pdf = datos.sample(fraction=0.5, seed=42).limit(20000).toPandas()
features = ["trip_distance", "duracion_min", "hora", "dia_semana"]
print(pdf.shape)
pdf.describe()

# COMMAND ----------

import mlflow
from mlflow.models import infer_signature
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, root_mean_squared_error
from sklearn.model_selection import train_test_split

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Users/{spark.sql('SELECT current_user()').first()[0]}/curso_taxi")
mlflow.sklearn.autolog(log_models=False)   # parámetros y métricas automáticas; el modelo lo registramos nosotros

X_train, X_test, y_train, y_test = train_test_split(pdf[features], pdf["fare_amount"], test_size=0.2, random_state=42)

def entrenar(nombre, modelo):
    with mlflow.start_run(run_name=nombre):
        modelo.fit(X_train, y_train)
        pred = modelo.predict(X_test)
        mae = mean_absolute_error(y_test, pred)
        mlflow.log_metrics({"mae_test": mae, "rmse_test": root_mean_squared_error(y_test, pred)})
        info = mlflow.sklearn.log_model(modelo, "modelo",
                                        signature=infer_signature(X_train, modelo.predict(X_train)),
                                        input_example=X_train.head(3))
        print(f"{nombre}: MAE={mae:.2f}")
        return {"nombre": nombre, "mae": mae, "uri": info.model_uri}

resultados = [
    entrenar("lineal", LinearRegression()),
    entrenar("rf_50",  RandomForestRegressor(n_estimators=50, max_depth=8, random_state=42, n_jobs=-1)),
    entrenar("rf_100", RandomForestRegressor(n_estimators=100, max_depth=12, random_state=42, n_jobs=-1)),
]

# COMMAND ----------

# MAGIC %md 👉 Abre **Experiments** (barra lateral) → `curso_taxi`. Selecciona las 3 ejecuciones y pulsa **Compare**.

# COMMAND ----------

# Elegir la mejor ejecución por MAE y registrarla en Unity Catalog
mejor = min(resultados, key=lambda r: r["mae"])
print("Mejor:", mejor["nombre"], "MAE:", round(mejor["mae"], 2))

nombre_modelo = f"{catalogo}.{esquema}.tarifa_taxi"
version = mlflow.register_model(mejor["uri"], nombre_modelo)

from mlflow import MlflowClient
MlflowClient().set_registered_model_alias(nombre_modelo, "champion", version.version)
print(f"Registrado {nombre_modelo} v{version.version} con alias @champion")

# COMMAND ----------

# MAGIC %md ## Inferencia por lotes con Spark (sobre millones de filas, en paralelo)

# COMMAND ----------

predecir = mlflow.pyfunc.spark_udf(spark, f"models:/{nombre_modelo}@champion", result_type="double")

predicciones = (datos.limit(10000)
    .withColumn("tarifa_predicha", F.round(predecir(*[F.col(c) for c in features]), 2))
    .withColumn("error", F.round(F.abs(F.col("fare_amount") - F.col("tarifa_predicha")), 2)))

predicciones.write.mode("overwrite").saveAsTable("predicciones_tarifa")
display(spark.table("predicciones_tarifa"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Retos
# MAGIC 1. Busca el modelo en **Catalog** (`curso_db → Models`) y mira sus versiones, alias y linaje.
# MAGIC 2. Añade una feature (p. ej. `pickup_zip` como número), entrena de nuevo, registra la versión 2 con alias `@challenger` y compara.
# MAGIC 3. Prueba **AutoML** (*Experiments → Create AutoML experiment*, si tu edición lo incluye) sobre `predicciones_tarifa`.
# MAGIC 4. Si tu edición lo permite, crea un endpoint de **Model Serving** para `@champion` y pruébalo desde la pestaña *Query*.
# MAGIC 5. Abre **AI Playground** y compara dos LLMs con el mismo prompt.
