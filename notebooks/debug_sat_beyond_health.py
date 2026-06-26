# Databricks notebook source
# Debug ultra paso a paso — sat_beyond_health
# No reemplaza al notebook de produccion (satelites_parametrizados.py).
# Premisa: sigue parametrizado, todo sale de DIM_PARAMETROS via fuente()/get_param().
# Cada celda hace UNA sola accion. Ejecuta una por una.

# COMMAND ----------
# 1. import SparkSession

from pyspark.sql import SparkSession

# COMMAND ----------
# 2. import Window

from pyspark.sql import Window

# COMMAND ----------
# 3. import functions as F

from pyspark.sql import functions as F

# COMMAND ----------
# 4. obtener spark

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# 5. variable: nombre completo de la tabla de parametros

PARAMS_TABLE = "`uc_axa_cli`.`silver`.`dim_parametros`"

# COMMAND ----------
# 6. variable: grupo que vamos a depurar

GRUPO = "sat_beyond_health"

# COMMAND ----------
# 7. leer DIM_PARAMETROS (sin cache todavia, solo leer)

df_params = spark.table(PARAMS_TABLE)

# COMMAND ----------
# 8. contar filas totales de DIM_PARAMETROS

df_params.count()

# COMMAND ----------
# 9. ver el esquema (columnas) de DIM_PARAMETROS

df_params.printSchema()

# COMMAND ----------
# 10. filtrar solo las filas del grupo sat_beyond_health (sin mostrar aun)

df_grupo = df_params.filter(F.col("grupo_parametros") == GRUPO)

# COMMAND ----------
# 11. contar cuantas filas tiene ese grupo
# *** si esto da 0, ya encontramos el problema: no hay parametros cargados
# para sat_beyond_health y por eso nada de lo siguiente puede funcionar ***

df_grupo.count()

# COMMAND ----------
# 12. mostrar esas filas (nombre / valor)

df_grupo.select("nombre", "valor").show(50, truncate=False)

# COMMAND ----------
# 13. extraer puntualmente la fila "catalogo_fuente"

fila_catalogo_fuente = df_grupo.filter(F.col("nombre") == "catalogo_fuente").select("valor").first()

# COMMAND ----------
# 14. ver que trajo esa fila (None si no existe)

fila_catalogo_fuente

# COMMAND ----------
# 15. extraer puntualmente la fila "esquema_fuente"

fila_esquema_fuente = df_grupo.filter(F.col("nombre") == "esquema_fuente").select("valor").first()

# COMMAND ----------
# 16. ver que trajo esa fila (None si no existe)

fila_esquema_fuente

# COMMAND ----------
# 17. guardar el valor real de catalogo_fuente (string, no Row)

catalogo_fuente = fila_catalogo_fuente[0] if fila_catalogo_fuente else None
catalogo_fuente

# COMMAND ----------
# 18. guardar el valor real de esquema_fuente (string, no Row)

esquema_fuente = fila_esquema_fuente[0] if fila_esquema_fuente else None
esquema_fuente

# COMMAND ----------
# 19. armar el nombre completo de la tabla bh_sa_member con esos valores
# *** si catalogo_fuente o esquema_fuente salieron None en los pasos 17/18,
# esta celda va a fallar o va a armar un nombre invalido como ``.``.`bh_sa_member` ***

nombre_tabla_member = f"`{catalogo_fuente}`.`{esquema_fuente}`.`bh_sa_member`"
nombre_tabla_member

# COMMAND ----------
# 20. intentar leer esa tabla

m = spark.table(nombre_tabla_member)

# COMMAND ----------
# 21. contar filas de bh_sa_member

m.count()

# COMMAND ----------
# 22. ver 5 filas de bh_sa_member

m.show(5)

# COMMAND ----------
# 23. armar el nombre completo de bh_sa_affiliation_contract

nombre_tabla_aco = f"`{catalogo_fuente}`.`{esquema_fuente}`.`bh_sa_affiliation_contract`"
nombre_tabla_aco

# COMMAND ----------
# 24. leer bh_sa_affiliation_contract

a = spark.table(nombre_tabla_aco)

# COMMAND ----------
# 25. contar filas de bh_sa_affiliation_contract

a.count()

# COMMAND ----------
# 26. ver el tipo de dato de m.ACO_NCODE

m.select("ACO_NCODE").printSchema()

# COMMAND ----------
# 27. ver el tipo de dato de a.ACO_NCODE
# *** si el tipo aqui es distinto al del paso 26 (ej. string vs bigint),
# ese es el motivo de que el join no cruce ***

a.select("ACO_NCODE").printSchema()

# COMMAND ----------
# 28. ver 10 valores distintos de m.ACO_NCODE

m.select("ACO_NCODE").distinct().show(10)

# COMMAND ----------
# 29. ver 10 valores distintos de a.ACO_NCODE
# *** compara a simple vista contra el paso 28: si los valores no se
# parecen en nada (formato, longitud, ceros a la izquierda), no van a
# cruzar aunque el tipo de dato sea el mismo ***

a.select("ACO_NCODE").distinct().show(10)

# COMMAND ----------
# 30. recien aqui, el primer join real: m + a (inner, ACO_NCODE)

df1 = m.join(a, a["ACO_NCODE"] == m["ACO_NCODE"], "inner")

# COMMAND ----------
# 31. contar filas del join
# *** compara contra paso 21 y 25: si aqui da 0 pero ahi habia filas,
# el problema es la condicion del join (tipo de dato o formato del valor),
# no la lectura de las tablas ni la parametrizacion ***

df1.count()
