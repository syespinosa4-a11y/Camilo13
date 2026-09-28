import unittest

from tienda.carrito import Carrito
from tienda.descuentos import aplicar_cupon, aplicar_porcentaje
from tienda.inventario import Inventario, ProductoNoEncontrado, StockInsuficiente


def inventario_base():
    inv = Inventario()
    inv.agregar("A1", "Café", 10.0, 5)
    inv.agregar("B2", "Taza", 4.5, 10)
    return inv


class TestInventario(unittest.TestCase):
    def test_valor_total(self):
        self.assertEqual(inventario_base().valor_total(), 95.0)

    def test_producto_inexistente(self):
        with self.assertRaises(ProductoNoEncontrado):
            inventario_base().obtener("ZZ")

    def test_no_permite_stock_negativo(self):
        inv = inventario_base()
        with self.assertRaises(StockInsuficiente):
            inv.retirar("A1", 6)


class TestDescuentos(unittest.TestCase):
    def test_porcentaje(self):
        self.assertEqual(aplicar_porcentaje(200, 10), 180)

    def test_cupon_invalido(self):
        self.assertEqual(aplicar_cupon(100, "NOEXISTE"), 100)


class TestCarrito(unittest.TestCase):
    def test_subtotal_con_cantidades(self):
        carrito = Carrito(inventario_base())
        carrito.agregar("A1", 2)
        carrito.agregar("B2", 1)
        self.assertEqual(carrito.subtotal(), 24.5)

    def test_total_con_cupon(self):
        carrito = Carrito(inventario_base())
        carrito.agregar("A1", 2)
        carrito.cupon = "VIP"
        self.assertEqual(carrito.total(), 15.0)


if __name__ == "__main__":
    unittest.main()
