from pymongo import MongoClient
from pprint import pprint
import json
import os
import pymongo

# Establecemos la conexión e insertamos los datos
client = MongoClient("mongodb://admin:admin@localhost:27017/")
db = client["practica6"]
clientes, productos, ventas = db["clientes"], db["productos"], db["ventas"]

def insertar_datos() -> None:
    clientes.drop()
    productos.drop()
    ventas.drop()
    
    script_dir = os.path.dirname(__file__)
    print("Script directory: ", script_dir)

    collections_to_load = ["clientes", "productos", "ventas"]
    
    for collection_name in collections_to_load:
        file_path = os.path.join(script_dir, f"{collection_name}.json")
        print(f"Loading data from: {file_path}")

        try:
            with open(file_path, "r") as f:
                data = json.load(f)
                if data:
                    db[collection_name].insert_many(data)
                    print(f"Successfully inserted data into {collection_name}.")
                else:
                    print(f"{collection_name}.json is empty!")
        
        except FileNotFoundError:
            print(f"ERROR: The file {collection_name}.json was not found at {file_path}")
        except json.JSONDecodeError:
            print(f"ERROR: Could not parse {collection_name}.json. Please check if it is valid.")

def mostrar_resultado(cursor) -> None:
    for doc in cursor:
        pprint(doc)
    print()

def obtener_productos():
    mostrar_resultado(productos.find())
    
def obtener_productos_activos():
    print("Productos Activos:\n")
    res = productos.find(
        {"activo": True}
    )
    mostrar_resultado(res)

def obtener_computadoras():
    print("Computadoras:\n")
    res = productos.find(
        {"categoria": "computadoras"}
    )
    mostrar_resultado(res)

def obtener_productos_premium():
    print("Productos Premium:\n")
    res = productos.find(
        {"precio": {"$gt": 500}}
    )
    mostrar_resultado(res)

def obtener_clientes_premium():
    print("Clientes Premium:\n")
    res = clientes.find(
        {"premium" : True}
    )
    mostrar_resultado(res)




def actualizar_precio(nombre_prod: str, nuevo_precio: int):
    res = productos.update_one(
        {"nombre": nombre_prod},
        {"$set": {"precio": nuevo_precio}}
    )
    print(res.modified_count)

def activar_productos_inactivos(stock: int):
    res = productos.update_many(
        {"activo": False},
        {"$set": {"stock": 10}}
    )
    print(res.modified_count)

def aplicar_descuento_marca(descuento: float, marca: str) -> int:
    res = productos.update_many(
        {"marca": marca},
        {"$mul": {"precio": descuento}}
    )
    print(res.modified_count)

def incrementar_stock(umbral: int, incr: int) -> int:
    res = productos.update_many(
        {"stock": {"$lt": umbral}},
        {"$inc": {"stock": incr}}
    )
    print(res.modified_count)

def ejercicio_14():
    pipeline = [
        {
            '$group': {
                '_id': '$categoria', 
                'cantidad_total': {
                    '$count': {}
                }, 
                'precio_promedio': {
                    '$avg': '$precio'
                }, 
                'stock_total': {
                    '$sum': '$stock'
                }, 
                'precio_mas_caro': {
                    '$max': '$precio'
                }, 
                'lista_productos': {
                    '$push': '$nombre'
                }
            }
        }, {
            '$sort': {
                'cantidad_todal': -1
            }
        }
    ]   
    aggCursor = productos.aggregate(pipeline)
    mostrar_resultado(aggCursor)

def ejercicio_15():
    pipeline = [    
        {
            '$lookup': {
                'from': 'productos', 
                'localField': 'producto_id', 
                'foreignField': '_id', 
                'as': 'info_producto'
            }
        }, {
            '$unwind': '$info_producto'
        }, {
            '$project': {
                '_id': 0, 
                'cliente': '$cliente_email', 
                'producto': '$info_producto.nombre', 
                'categoria': '$info_producto.categoria', 
                'marca': '$info_producto.marca', 
                'cantidad': '$cantidad', 
                'total': '$total', 
                'ciudad': '$ciudad'
            }
        }
    ]

    aggCursor = ventas.aggregate(pipeline)
    mostrar_resultado(aggCursor)

def ejercicio_16():
    pipeline = [
        {
            '$group': {
                '_id': '$ciudad', 
                'total_ventas': {
                    '$sum': '$total'
                }, 
                'total_transacciones': {
                    '$sum': '$cantidad'
                }, 
                'promedio_venta_transaccion': {
                    '$avg': '$total'
                }
            }
        }, {
            '$sort': {
                'total_ventas': -1
            }
        }
    ]
    mostrar_resultado(ventas.aggregate(pipeline))


def main():    
    # Tarea 1: Poblado inicial de los catálogos
    insertar_datos()

    # Tarea 2: Consultas
    obtener_productos()
    obtener_productos_activos()
    obtener_computadoras()
    obtener_productos_premium()
    obtener_clientes_premium()

    # Tarea 3: Actualizaciones
    actualizar_precio(nombre_prod="Auriculares Bluetooth", nuevo_precio=249)
    activar_productos_inactivos(stock=10)
    aplicar_descuento_marca(descuento=0.9, marca="Apple")
    incrementar_stock(umbral=10, incr=5)

    # Tarea 4: Índices
    productos.create_index("nombre")
    productos.create_index([("categoria", pymongo.ASCENDING), ("precio", pymongo.DESCENDING)])
    clientes.create_index("email", unique=True)
    mostrar_resultado(productos.list_indexes())

    # Tarea 5: Agregaciones
    ejercicio_14()
    ejercicio_15()
    ejercicio_16()

    # Tarea 6: Borrado
    productos.insert_one(
        {
            "_id": 5,
            "nombre": "SmartWatch Samsung",
            "categoria": "relojes",
            "precio": 199.99,
            "stock": 15,
            "marca": "Apple",
            "tags": [
                "samsung",
                "smartwatch",
                "reloj"
            ],
            "fecha_ingreso": "2024-1-25",
            "activo": True
        }
    )

    res = productos.delete_one(
        {"nombre": "SmartWatch Samsung"}
    )
    print(res)


if __name__ == "__main__":
    main()