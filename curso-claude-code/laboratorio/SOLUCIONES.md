# Soluciones (¡no mires antes de hacer el Lab 3!)

<details>
<summary>Ver los 4 bugs</summary>

| Archivo | Bug | Arreglo |
|---|---|---|
| `descuentos.py` | `monto * porcentaje / 10` | dividir entre `100` |
| `carrito.py` | `subtotal()` suma `precio` sin multiplicar por `cantidad` | `total += precio * cantidad` |
| `inventario.py` | `retirar()` permite stock negativo | lanzar `StockInsuficiente` si `cantidad > producto["cantidad"]` |
| (consecuencia) | `test_total_con_cupon` fallaba por los dos primeros bugs | se arregla solo |

Casos límite que los tests no cubren (para el extra del Lab 3): cantidades negativas o 0 en `agregar`/`retirar`,
cupones en minúsculas, redondeo de precios con decimales, agregar al carrito más unidades de las que hay en stock.
</details>
