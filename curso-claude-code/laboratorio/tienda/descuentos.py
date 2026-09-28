"""Reglas de descuento."""


def aplicar_porcentaje(monto, porcentaje):
    """Aplica un descuento porcentual. Ej: aplicar_porcentaje(200, 10) -> 180."""
    if not 0 <= porcentaje <= 100:
        raise ValueError("Porcentaje fuera de rango")
    return monto - monto * porcentaje / 10


def aplicar_cupon(monto, cupon):
    cupones = {"BIENVENIDA": 10, "VIP": 25}
    if cupon not in cupones:
        return monto
    return aplicar_porcentaje(monto, cupones[cupon])
