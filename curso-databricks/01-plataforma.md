# Módulo 1 — La plataforma

## 1.1 ¿Qué es Databricks?
Una plataforma de datos e IA en la nube (AWS, Azure, GCP) creada por los autores de Apache Spark.
Su idea es el **Lakehouse**: combina
- lo barato y flexible de un **data lake** (archivos en almacenamiento de objetos: S3, ADLS, GCS), con
- la fiabilidad y el rendimiento de un **data warehouse** (transacciones, esquemas, SQL rápido).

Esto lo hace posible **Delta Lake**, un formato de tabla abierto sobre archivos Parquet.

## 1.2 Piezas principales
| Pieza | Para qué |
|---|---|
| **Workspace** | Tu entorno web: notebooks, carpetas, jobs, dashboards |
| **Compute** | Dónde se ejecuta el código (ver 1.3) |
| **Unity Catalog** | Gobierno de datos: `catálogo.esquema.tabla`, permisos, linaje |
| **Delta Lake** | Formato de almacenamiento de las tablas |
| **Lakeflow** | Ingesta (Connect), pipelines (Declarative Pipelines) y orquestación (Jobs) |
| **Databricks SQL** | Warehouses SQL, editor de consultas, dashboards, Genie |
| **Mosaic AI / MLflow** | Machine learning, modelos, agentes de IA, serving |

**Plano de control vs. plano de datos**: la interfaz y la gestión las pone Databricks; los datos viven en el almacenamiento en la nube (tuyo o gestionado).

## 1.3 Tipos de compute
| Tipo | Uso |
|---|---|
| **Serverless** (notebooks, jobs, pipelines) | Arranca en segundos, sin configurar nada. Es la opción por defecto recomendada |
| **All-purpose cluster** | Clúster interactivo que configuras tú (versión de runtime, tamaño, librerías) |
| **Job cluster** | Se crea para un job y se destruye al acabar (más barato que all-purpose) |
| **SQL warehouse** | Optimizado para SQL/BI (Photon). Serverless o clásico |

El **Databricks Runtime** (DBR) es la imagen con Spark y las librerías; la versión **ML** añade scikit-learn, PyTorch, etc.
**Photon** es el motor vectorizado en C++ que acelera SQL y DataFrames.
Se paga por **DBU** (Databricks Unit) según el tiempo de uso más el coste de la nube.

## 1.4 Crea tu cuenta gratis (hazlo ahora)
1. Ve a **Databricks Free Edition** (busca "Databricks Free Edition signup"; sustituyó a la antigua Community Edition).
2. Regístrate con tu email. Obtienes un workspace con compute serverless, Unity Catalog y SQL warehouse.
3. Limitaciones: solo serverless, cuotas de uso diarias y algunas funciones empresariales no disponibles. **Es suficiente para todo este curso.**
   (Alternativa: la prueba gratuita de 14 días en AWS/Azure/GCP, con todas las funciones.)

## 1.5 Recorrido por la interfaz (barra lateral)
- **Workspace**: carpetas y notebooks. **Recents**, **Catalog** (explorador de datos), **Jobs & Pipelines**, **Compute**.
- **SQL**: SQL Editor, Queries, Dashboards, Genie, Alerts, Query History, SQL Warehouses.
- **AI/ML**: Playground, Experiments, Features, Models, Serving.
- El **asistente de IA de Databricks** está integrado en notebooks y en el editor SQL: te explica código, corrige errores y genera consultas.

## 1.6 Importar el laboratorio
1. Workspace → tu carpeta de usuario → **Create → Git folder** → pega la URL de este repositorio.
   (O **Import** y sube los archivos de `laboratorio/notebooks/`.)
2. Los archivos `.py` y `.sql` que empiezan con `# Databricks notebook source` se abren como notebooks.
3. Abre `00_setup` y ejecútalo.

➡️ Lab: `00_setup`.
