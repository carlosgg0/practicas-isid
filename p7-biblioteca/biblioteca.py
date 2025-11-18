import json
import pprint
import pymongo
import os

client = pymongo.MongoClient("mongodb://admin:admin@localhost:27017/")
db = client["practica7"]

categorias = db["categorias"]
estudiantes = db["estudiantes"]
libros = db["libros"]
prestamos = db["prestamos"]

def insertar_datos():
    print("\nPoblado inicial de la base de datos: \n")
    categorias.drop()
    estudiantes.drop()
    libros.drop()
    prestamos.drop()

    script_dir = os.path.dirname(__file__)
    collections_to_load = ["categorias", "estudiantes", "libros", "prestamos"]

    for collection in collections_to_load:
        file_path = os.path.join(script_dir, f"{collection}.json")
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
                if data:
                    db[collection].insert_many(data)
                    print(f"Successfully inserted data into {collection}.")
        
        except FileNotFoundError as err:
            print(err)

        except json.JSONDecodeError:
            print(f"ERROR: Could not parse {collection}.json. Please check if it is valid.")

    # Buscar los IDs necesarios
    libro_1984 = libros.find_one({"titulo": "1984"})
    libro_python = libros.find_one({"titulo": "Python Crash Course"})
    estudiante_ana = estudiantes.find_one({"nombre": "Ana García López"})
    estudiante_carlos = estudiantes.find_one({"nombre": "Carlos Rodríguez Martín"})

    # Añadir los IDs a cada préstamo
    prestamos.update_one(
        {},
        {"$set": {"libro_id": libro_1984["_id"], "estudiante_id": estudiante_ana["_id"]}}
    )
    prestamos.update_one(
        {"fecha_prestamo": "2024-02-05"},
        {"$set": {"libro_id": libro_python["_id"], "estudiante_id": estudiante_carlos["_id"]}}
    )

def actualizar_stock_libro(nombre: str, nuevo_stock: int):
    res = libros.update_one(
        {"titulo": nombre},
        {"$set": {"stock": nuevo_stock}}
    )
    print(res.raw_result)

def add_campo_descuento_libros_antes(descuento: int, year: int):

    res = libros.update_many(
        {"año_publicacion": {"$lt": 2000}},
        {"$set": {"descuento": descuento}}
    )
    print(res.raw_result)

def ejercicio3():
    print("\nEjercicio 3\n")

    res = categorias.update_one(
        {"nombre": "Matemáticas"},
        {
            "$set": {"ubicacion": "Ala E"},
            "$setOnInsert": {"descripción": "Libros de matemáticas y cálculo", "libros_count": 0}
        },
        upsert=True
    )
    print(res.raw_result)

def ejercicio4():
    print("\nEjercicio 4\n")

    res = libros.find(
        {"disponible": True}
    )
    for i in res:
        pprint.pprint(i)

def ejercicio5():
    print("\nEjercicio 5\n")

    res = estudiantes.find(
        {"carrera": "Ingeniería Informática"}
    )
    for r in res:
        pprint.pprint(r)

def ejercicio6():
    print("\nEjercicio 6\n")

    res = libros.find(
        {"precio": {"$gt": 20}}
    )
    print("Se ha ejecutado correctamente!")
    for r in res:
        pprint.pprint(r)

def ejercicio7():
    print("\nEjercicio 7\n")

    res = libros.aggregate([
        {"$match": {"disponible": True}},
        {"$group":{
            "_id": "$genero",
            "total_libros": {"$sum": 1},
            "stock_total": {"$sum": "$stock"},
            "precio_promedio": {"$avg": "$precio"},
            "libros": {"$push": "$titulo"}
        }},
        {"$sort": {"total_libros": -1}}
    ])
    for r in res:
        pprint.pprint(r)

def ejercicio8():
    print("\nEjercicio 8\n")

    res = libros.aggregate([
        {
            '$bucket': {
                'groupBy': '$precio', 
                'boundaries': [
                    0, 10, 20, 30, 50
                ], 
                'default': '+50', 
                'output': {
                    'total_libros': {
                        '$sum': 1
                    }, 
                    'precio_promedio': {
                        '$avg': '$precio'
                    }
                }
            }
        }
    ])
    for r in res:
        pprint.pprint(r)


def ejercicio9():
    print("\nEjercicio 9\n")

    res = prestamos.aggregate([
        {"$lookup": {
            "from": "estudiantes",
            "localField": "estudiante_id",
            "foreignField": "_id",
            "as": "datos_estudiante"
        }},
        {"$lookup": {
            "from": "libros", 
            "localField": "libro_id",
            "foreignField": "_id",
            "as": "datos_libro"
        }},
        {"$unwind": "$datos_estudiante"},
        {"$unwind": "$datos_libro"},
        {"$project": {
            "_id": 0,
            "nombre_estudiante": "$datos_estudiante.nombre",
            "titulo_libro": "$datos_libro.titulo",
            "fecha_prestamo": "$fecha_prestamo",
            "fecha_devolucion": "$fecha_devolucion",
            "estado": "$estado",
            "carrera_estudiante": "$datos_estudiante.carrera"
        }}
    ])
    for r in res:
        pprint.pprint(r)

def ejercicio10():
    print("\nEjercicio 10\n")

    res = estudiantes.aggregate([
        #{"$match": {"activo": True}},
        {"$group": {
            "_id": "$ciudad",
            "total_estudiantes": {"$sum": 1},
            "edad_promedio": {"$avg": "$edad"},
            "carreras_unicas": {"$addToSet": "$carrera"},
            "nombres_estudiantes": {"$addToSet": "$nombre"}
        }},
        {"$project": {
            "nombre_ciudad": "$_id",
            "total_estudiantes": 1,
            "edad_promedio": {"$round": ["$edad_promedio", 0]},
            "carreras_unicas": 1,
            "nombres_estudiantes": 1
        }}
    ])
    for r in res:
        pprint.pprint(r)

def ejercicio11():
    print("\nEjercicio 11\n")

    estudiantes.insert_one(
        {
            "nombre": "estudiante nuevo para eliminar",
            "email": "nomeimporta@gmail.com", 
            "carrera": "Filosofía",
            "semestre": 6,
            "edad": 21,
            "ciudad": "Málaga",
            "fecha_registro": "2023-01-15",
            "activo": False   
        }
    )
    res = estudiantes.find_one({"nombre": "estudiante nuevo para eliminar"})
    pprint.pprint(res)

    # Lo eliminamos
    estudiantes.delete_one({"nombre": "estudiante nuevo para eliminar"})
    res = estudiantes.find()
    print("Estudiantes tras la eliminación")
    for r in res:
        pprint.pprint(r)


def ejercicio12():
    print("\nEjercicio 12 \n")

    res = libros.update_many(
        {"genero": "Programación"},
        {"$set": {
            "ubicacion": "Sala de ordenadores",
            "etiqueta_especial": "Tecnología"
        }}
    )

    pprint.pprint(res.raw_result)

    print("\nLibros de la categoría Programación tras la modificación: \n")
    res = libros.find({"genero": "Programación"})

    for r in res:
        pprint.pprint(r)

def main():
    insertar_datos()

    # Ejercicio 1
    actualizar_stock_libro("1984", 3)

    # Ejercicio 2
    add_campo_descuento_libros_antes(15, 2000)

    ejercicio3()

    ejercicio4()

    ejercicio5()

    ejercicio6()

    ejercicio7()

    ejercicio8()

    ejercicio9()

    ejercicio10()

    ejercicio11()
    
    ejercicio12()

if __name__ == "__main__":
    main()