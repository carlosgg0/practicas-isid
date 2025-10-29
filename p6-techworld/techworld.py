import pymongo
from datetime import datetime

class TechWorld:
    def __init__(self):
        self.client = pymongo.MongoClient("mongodb://admin:admin@localhost:27017/")
        self.db = self.client["practica6"]
        self.db["clientes"].drop()
        self.db["productos"].drop()
        self.db["ventas"].drop()

    def insertar_clientes(self):
        self.db["clientes"].insert_many([
            {
                "nombre": "Ana García",
                "email": "ana@techworld.com",
                "ciudad": "Madrid",
                "premium": True,
                "fecha_registro": datetime(2023, 12, 1)
            },
            {
                "nombre": "Carlos López", 
                "email": "carlos@techworld.com",
                "ciudad": "Barcelona",
                "premium": False,
                "fecha_registro": datetime(2024, 1, 15)
            },
            {
                "nombre": "María Rodríguez",
                "email": "maria@techworld.com", 
                "ciudad": "Madrid",
                "premium": True,
                "fecha_registro": datetime(2023, 11, 20)
            }
        ])

    def insertar_productos(self):
        self.db["productos"].insert_many([
            {
                "_id": 1,
                "nombre": "Laptop Gaming Pro",
                "categoria": "computadoras",
                "precio": 1500,
                "stock": 8,
                "marca": "ASUS",
                "tags": ["gaming", "portatil", "rendimiento"],
                "fecha_ingreso": datetime(2024, 1, 10),
                "activo": True
            },
            {
                "_id": 2,
                "nombre": "Smartphone Galaxy",
                "categoria": "moviles", 
                "precio": 799,
                "stock": 25,
                "marca": "Samsung",
                "tags": ["android", "5G", "camara"],
                "fecha_ingreso": datetime(2024, 2, 15),
                "activo": True
            },
            {
                "_id": 3,
                "nombre": "Tablet iPad",
                "categoria": "tablets",
                "precio": 599,
                "stock": 15,
                "marca": "Apple",
                "tags": ["apple", "creatividad", "portatil"],
                "fecha_ingreso": datetime(2024, 1, 25),
                "activo": True
            },
            {
                "_id": 4,
                "nombre": "Auriculares Bluetooth",
                "categoria": "audio",
                "precio": 299,
                "stock": 0,
                "marca": "Sony", 
                "tags": ["audio", "inalambrico", "calidad"],
                "fecha_ingreso": datetime(2024, 3, 1),
                "activo": False
            }
        ])

    def insertar_ventas(self):
        self.db["ventas"].insert_many([
            {
                "producto_id": 1,
                "cliente_email": "ana@techworld.com",
                "cantidad": 1,
                "total": 1500,
                "fecha": datetime(2024, 3, 15),
                "ciudad": "Madrid"
            },
            {
                "producto_id": 2,
                "cliente_email": "carlos@techworld.com", 
                "cantidad": 2,
                "total": 1598,
                "fecha": datetime(2024, 3, 16),
                "ciudad": "Barcelona"
            },
            {
                "producto_id": 3,
                "cliente_email": "maria@techworld.com",
                "cantidad": 1, 
                "total": 599,
                "fecha": datetime(2024, 3, 17),
                "ciudad": "Madrid"
            }
        ])

    def obtener_productos(self):
        print("Todos los productos:\n")
        res = self.db["productos"].find({})
        for r in res:
            print(r)
        print("\n")

    def obtener_productos_activos(self):
        print("Productos Activos:\n")
        res = self.db["productos"].find(
            {"activo": True}
        )
        for r in res:
            print(r)
        print("\n")

    def obtener_productos_categoria_computadoras(self):
        print("Computadoras:\n")
        res = self.db["productos"].find(
            {"categoria": "computadoras"}
        )
        for r in res:
            print(r)
        print("\n")

    def obtener_productos_premium(self):
        print("Productos Premium:\n")

        res = self.db["productos"].find(
            {"precio": {"$gt": 500}}
        )
        for r in res:
            print(r)

        print("\n")

    def obtener_clientes_premium(self):
        print("Clientes Premium:\n")
        res = self.db["clientes"].find(
            {"premium": True}
        )
        for r in res:
            print(r)

        print("\n")

    def actualizar_precio(self, nombre_producto: str, nuevo_precio: int) -> int:
        # Devuelve el número de elementos modificados 
        res = self.db["productos"].update_one(
            {"nombre": nombre_producto},
            {"$set": {"precio": nuevo_precio}}
        )
        return res.modified_count

    def activar_productos_inactivos(self, stock: int) ->int:
        # Devuelve el número de elementos modificados
        res = self.db["productos"].update_many(
            {"activo": False},
            {"$set": {"stock": 10}}
        )
        return res.modified_count

    def aplicar_descuento_marca(self, descuento: float, marca: str) -> int:
        # Devuelve el número de elementos modificados
        res = self.db["productos"].update_many(
            {"marca": marca},
            {"$mul": {"precio": descuento}}
        )
        return res.modified_count

    def incrementar_stock(self, umbral: int, incr: int) -> int:
        # Devuelve el número de elementos modificados
        res = self.db["productos"].update_many(
            {"stock": {"$lt": umbral}},
            {"$inc": {"stock": incr}}
        )
        return res.modified_count



if __name__ == "__main__":
    tw = TechWorld()

    # Tarea 1: Poblado inicial de los catálogos
    tw.insertar_clientes()
    tw.insertar_productos()
    tw.insertar_ventas()

    # Tarea 2: Consultas
    tw.obtener_productos()
    tw.obtener_productos_activos()
    tw.obtener_productos_categoria_computadoras()
    tw.obtener_productos_premium()
    tw.obtener_clientes_premium()

    # Tarea 3: Actualizaciones
    tw.actualizar_precio(nombre_producto="Auriculares Bluetooth", nuevo_precio=249)
    tw.activar_productos_inactivos(stock=10)
    tw.aplicar_descuento_marca(descuento=0.9, marca="Apple")
    tw.incrementar_stock(umbral=5, incr=10)

    # Tarea 4: Índices
    tw.db["productos"].create_index({"nombre": 1}, {"name": "nombre"})
    tw.db["productos"].create_index({"categoria": 1, "precio": -1}, {"name": "categoria y precio"})
    tw.db["clientes"].create_index({"email": 1}, {"unique": True})
    tw.db["productos"].list_indexes()