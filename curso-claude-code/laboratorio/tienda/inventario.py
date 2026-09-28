"""Gestión de inventario de productos."""


class ProductoNoEncontrado(Exception):
    pass


class StockInsuficiente(Exception):
    pass


class Inventario:
    def __init__(self):
        self._productos = {}

    def agregar(self, codigo, nombre, precio, cantidad=0):
        if precio < 0:
            raise ValueError("El precio no puede ser negativo")
        self._productos[codigo] = {
            "nombre": nombre,
            "precio": precio,
            "cantidad": cantidad,
        }

    def obtener(self, codigo):
        if codigo not in self._productos:
            raise ProductoNoEncontrado(codigo)
        return self._productos[codigo]

    def retirar(self, codigo, cantidad):
        producto = self.obtener(codigo)
        producto["cantidad"] -= cantidad
        return producto["cantidad"]

    def valor_total(self):
        total = 0
        for p in self._productos.values():
            total = total + p["precio"] * p["cantidad"]
        return total
