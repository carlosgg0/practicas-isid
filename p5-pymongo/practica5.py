import pymongo

def crear_conexion():
    client = pymongo.MongoClient("mongodb://admin:admin@localhost:27017/")
    db = client["practicas"]
    db.cursos.drop()
    db.estudiantes.drop()
    db.calificaciones.drop()

    return client, db    

def insertar_curso(db):
    cursos = [
            {
                "_id": 1,
                "nombre": "Python Básico",
                "profesor": "Ana García",
                "duracion_horas": 40,
                "nivel": "Principiante"
            },
            {
                "_id": 2, 
                "nombre": "Web Development",
                "profesor": "Carlos López",
                "duracion_horas": 60,
                "nivel": "Intermedio"
            },
            {
                "_id": 3,
                "nombre": "Data Science",
                "profesor": "María Rodríguez", 
                "duracion_horas": 80,
                "nivel": "Avanzado"
            }]
    
    # Insertar documentos en la colección "cursos"
    db.cursos.insert_many(cursos)
    
def insertar_estudiantes(db):
    estudiantes = [
                {
        "_id": 1,
        "nombre": "Laura Martínez",
        "edad": 22,
        "email": "laura@email.com",
        "ciudad": "Madrid",
        "curso_id": 1
        },
        {
        "_id": 2,
        "nombre": "David Chen",
        "edad": 25, 
        "email": "david@email.com",
        "ciudad": "Barcelona",
        "curso_id": 2
        },
        {
        "_id": 3,
        "nombre": "Sofía Pérez",
        "edad": 20,
        "email": "sofia@email.com", 
        "ciudad": "Madrid",
        "curso_id": 1
        },
        {
        "_id": 4,
        "nombre": "Javier Ruiz",
        "edad": 28,
        "email": "javier@email.com",
        "ciudad": "Valencia", 
        "curso_id": 3
        }
    ]
    
    db.estudiantes.insert_many(estudiantes)


def insertar_calificaciones(db):
    calificaciones = [
        {"estudiante_id": 1, "curso_id": 1, "calificacion": 8.5},
        {"estudiante_id": 2, "curso_id": 2, "calificacion": 9.0},
        {"estudiante_id": 3, "curso_id": 1, "calificacion": 7.5},
        {"estudiante_id": 4, "curso_id": 3, "calificacion": 8.0}
    ]

    db.calificaciones.insert_many(calificaciones)


def ejercicio1_todos_los_estudiantes(db):
    estudiantes = db.estudiantes.find()
    print(f"Tipo de dato devuelto por find(): {type(estudiantes)}")

    for estudiante in estudiantes:
        print(estudiante["nombre"])
        print(estudiante["email"])


def ejercicio2_estudiantes_madrid(db):
    estudiantes_madrid = db.estudiantes.find(
        {"ciudad": "Barcelona"}
    )
    print("Estudiantes en Madrid:")
    for e in estudiantes_madrid:
        print(e)


def ejercicio3_curso_mayor50(db):
    cursos_mayor_50 = db.cursos.find(
        {"duracion_horas": {"$gt": 50}},
    )
    print("Cursos con duración mayor a 50h: ")
    for curso in cursos_mayor_50:
        print(curso)


def ejercicio4_actualizar_edad_Laura(db):
    resultado = db.estudiantes.update_one(
        {"nombre": "Laura Martínez"},
        {"$set": {"edad": 23}}
    )
    print(f"Documentos modificados: {resultado.modified_count}")


def anyadir_campo_activo(db):
    resultado = db.estudiantes.update_many(
        {},
        {"$set": {"activo": True}}
    )
    print(f"Documentos modificados: {resultado.modified_count}")


def insertar_temporal(db):
    db.estudiantes.insert_one(
        {
            "nombre": "Temporal",
            "edad": 20,
            "email": "example@gmail.com",
            "ciudad": "Test"
         }
    )
    
    estudiante_temp = db.estudiantes.find(
        {"nombre": "Temporal"}
    )

    for e in estudiante_temp:
        print(e)

    db.estudiantes.delete_one(
        {"nombre": "Temporal"}
    )

    estudiante_temp = db.estudiantes.find(
        {"nombre": "Temporal"}
    )


def estudiantes_cursos(db):
    pass



if __name__ == "__main__":
    try:
        cliente, db = crear_conexion()
        insertar_curso(db)
        insertar_estudiantes(db)
        insertar_calificaciones(db)
        ejercicio1_todos_los_estudiantes(db)
        ejercicio2_estudiantes_madrid(db)
        ejercicio3_curso_mayor50(db)
        ejercicio4_actualizar_edad_Laura(db)
        insertar_temporal(db)

        pipeline = [...]
        db.estudiantes.aggregate(pipeline)

    except Exception as e:
        print(f"Ocurrió un error: {e}")
