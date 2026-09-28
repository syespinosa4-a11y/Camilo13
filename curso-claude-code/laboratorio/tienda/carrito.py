"""Carrito de compras."""
from tienda.descuentos import aplicar_cupon


class Carrito:
    def __init__(self, inventario):
        self.inventario = inventario
        self.items = {}
        self.cupon = None

    def agregar(self, codigo, cantidad=1):
        self.inventario.obtener(codigo)
        self.items[codigo] = self.items.get(codigo, 0) + cantidad

    def subtotal(self):
        total = 0
        for codigo, cantidad in self.items.items():
            precio = self.inventario.obtener(codigo)["precio"]
            total += precio
        return total

    def total(self):
        monto = self.subtotal()
        if self.cupon:
            monto = aplicar_cupon(monto, self.cupon)
        return round(monto, 2)
