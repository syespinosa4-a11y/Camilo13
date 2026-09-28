# Módulo 3 — Delta Lake

## 3.1 Qué es
Una tabla Delta = archivos **Parquet** + un **registro de transacciones** (`_delta_log/`, archivos JSON y checkpoints).
Ese log le da:
- **Transacciones ACID**: las escrituras concurrentes no corrompen los datos.
- **Aplicación del esquema** (*schema enforcement*): rechaza datos con columnas o tipos incorrectos. La **evolución del esquema** (*schema evolution*) es opcional.
- **Time travel**: consultar versiones anteriores.
- **DML** completo: `UPDATE`, `DELETE`, `MERGE`.
- Es un formato abierto; con **UniForm**, otros motores pueden leerlo también como Iceberg.

En Databricks, **toda tabla es Delta por defecto**.

## 3.2 Operaciones clave
```sql
CREATE TABLE clientes (id INT, nombre STRING, ciudad STRING);
INSERT INTO clientes VALUES (1, 'Ana', 'Bogotá');
UPDATE clientes SET ciudad = 'Medellín' WHERE id = 1;
DELETE FROM clientes WHERE id = 99;

-- Upsert: la operación más importante en ingeniería de datos
MERGE INTO clientes AS t
USING cambios AS s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

DESCRIBE HISTORY clientes;                        -- versiones
SELECT * FROM clientes VERSION AS OF 1;           -- time travel
SELECT * FROM clientes TIMESTAMP AS OF '2026-01-01';
RESTORE TABLE clientes TO VERSION AS OF 1;        -- deshacer
DESCRIBE DETAIL clientes;                         -- ubicación, nº de archivos, tamaño
```

## 3.3 Rendimiento y mantenimiento
| Comando / función | Qué hace |
|---|---|
| `OPTIMIZE tabla` | Compacta archivos pequeños en archivos grandes |
| `CLUSTER BY (col)` | **Liquid clustering**: organiza los datos por columnas de filtro frecuentes (sustituye a particionar y a Z-ORDER en tablas nuevas) |
| `ZORDER BY` | Técnica anterior de co-localización de datos (solo para tablas heredadas) |
| `VACUUM tabla` | Borra archivos que ya no se usan (por defecto conserva 7 días; limita cuánto puedes viajar en el tiempo) |
| `ANALYZE TABLE ... COMPUTE STATISTICS` | Estadísticas para el optimizador |
| **Predictive Optimization** | Databricks ejecuta OPTIMIZE/VACUUM automáticamente en tablas gestionadas |

Regla: **no particiones tablas de menos de ~1 TB**; usa liquid clustering.

## 3.4 Más funciones útiles
- **Change Data Feed** (`delta.enableChangeDataFeed = true`): lee solo los cambios entre versiones (`table_changes()`).
- **Constraints**: `ALTER TABLE t ADD CONSTRAINT precio_ok CHECK (precio >= 0)`.
- **Columnas generadas e identidad**: `id BIGINT GENERATED ALWAYS AS IDENTITY`.
- **Clone**: `CREATE TABLE copia SHALLOW CLONE original` (copia instantánea, ideal para pruebas).
- **Evolución del esquema** al escribir: `.option("mergeSchema", "true")`.

➡️ Lab: `02_delta_lake`.
