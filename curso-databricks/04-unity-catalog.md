# Módulo 4 — Unity Catalog (gobierno de datos)

## 4.1 Jerarquía
```
Metastore (uno por región)
└── Catálogo              ej: workspace, prod, dev
    └── Esquema           ej: ventas            (también llamado database)
        ├── Tablas        (gestionadas o externas)
        ├── Vistas y vistas materializadas
        ├── Volúmenes     (archivos no tabulares: CSV, JSON, imágenes, PDFs)
        ├── Funciones     (UDFs en SQL o Python)
        └── Modelos       (modelos de ML registrados)
```
Nombre completo de tres niveles: `catalogo.esquema.objeto`. Usa `USE CATALOG x; USE SCHEMA y;` para abreviar.

## 4.2 Tablas gestionadas vs. externas
| | Gestionada (recomendada) | Externa |
|---|---|---|
| Ubicación | La decide Unity Catalog | La indicas tú con `LOCATION` |
| `DROP TABLE` | Borra datos y metadatos | Borra solo los metadatos |
| Optimización automática | Sí | No |

Las externas requieren una **storage credential** y una **external location** (las configura el administrador).

## 4.3 Volúmenes
Sirven para archivos: `/Volumes/<catalogo>/<esquema>/<volumen>/ruta/archivo.csv`.
Se usan con Spark, con `dbutils.fs` y con Python normal (`open()`, pandas). Sustituyen al antiguo DBFS/`/mnt`.

## 4.4 Permisos (SQL estándar)
```sql
GRANT USE CATALOG ON CATALOG prod TO `analistas`;
GRANT USE SCHEMA ON SCHEMA prod.ventas TO `analistas`;
GRANT SELECT ON TABLE prod.ventas.pedidos TO `analistas`;
REVOKE SELECT ON TABLE prod.ventas.pedidos FROM `analistas`;
SHOW GRANTS ON TABLE prod.ventas.pedidos;
```
Para leer una tabla necesitas **las tres**: `USE CATALOG`, `USE SCHEMA` y `SELECT`. Asigna permisos a **grupos**, no a personas.

Control fino:
- **Row filters** y **column masks**: funciones que filtran filas u ocultan columnas según quién consulta.
- **Vistas dinámicas** con `is_account_group_member('grupo')`.
- **Tags** y clasificación de datos sensibles (PII).

## 4.5 Otras capacidades
- **Linaje** automático a nivel de tabla y columna: pestaña *Lineage* en Catalog Explorer.
- **Tablas del sistema** (`system.access.audit`, `system.billing.usage`, `system.query.history`): auditoría y costes con SQL.
- **Delta Sharing**: compartir datos con otras organizaciones sin copiarlos.
- **Lakehouse Federation**: consultar PostgreSQL, MySQL, Snowflake, etc. sin moverlos (*foreign catalogs*).
- **Information schema**: `SELECT * FROM system.information_schema.tables`.

➡️ Lab: `00_setup` y los ejercicios del módulo 4 en la guía del laboratorio.
