import redis

"""
Necesitamos almacenar:

Datos del usuario: nombre completo, email y edad
Lista de tareas pendientes del usuario


DATO            TIPO        CLAVE               CAMPOS / Información
Usuario         Hash        usuario:ID          nombre, email, edad
Lista Tareas    List        tareas:{ID user}    Contendrá nombres de diferentes tareas
"""

class GestionTareas:
    def __init__(self):
        """Conectar a la base de datos"""
        try:
            self.r = redis.Redis(
                host="localhost", port=6379, db=0, decode_responses=True
            )
            self.r.flushdb()
        except redis.ConnectionError:
            print("Error: no se pudo conectar a la base de datos")

    def crear_usuario(self, 
        id: int,
        nombre: str,
        email: str, 
        edad: int
    ) -> None:
        """Almacenar un nuevo usuario en la base de datos"""
        fields = {
            "nombre": nombre,
            "email": email,
            "edad": edad
        }
        self.r.hset(name=f"usuario:{id}", mapping=fields)

    def add_tareas_lista_usuario(
        self,
        id_user: int,
        tareas: list[str] # Lista con los nombres de las tareas a añadir
    ) -> None:
        """Añade tareas a la lista

        La tarea más reciente siempre es la primera de la lista
        """
        for tarea in tareas:
            self.r.lpush(f"tareas:{id_user}", tarea)

    def usuario_completa_tarea(
        self,
        id_user: int
    ) -> None:
        """Eliminar primera tarea de la lista de pendientes del usuario"""
        self.r.lpop(f"tareas:{id_user}")

    def consultar_usuario(
        self,
        id_user: int
    ) -> None:
        """Consultar toda la info relativa al usuario deseado"""
        user_info = self.r.hgetall(f"usuario:{id_user}")
        task_list = self.r.lrange(f"tareas:{id_user}", start=0, end=-1)

        print(f"Usuario {id_user}:")
        for k,v in user_info.items():
            print(f"    {k}: {v}")
        print("Tareas pendientes:")
        for i, task in enumerate(task_list, start=1):
            print(f"{i}: {task}")

    def consultar_todas_claves(
        self
    ) -> None:
        print(self.r.keys("*"))
            
        
if __name__ == "__main__":
    g = GestionTareas()

    g.crear_usuario(1, "Carlos G", "carlosgarciag552@gmail.com", 20)
    g.crear_usuario(2, "Ana G", "ana5@gmail.com", 17)
    g.crear_usuario(3, "Luis P", "luis@gmail.com", 26)
    g.crear_usuario(4, "Manuel V", "manu@gmail.com", 21)
    g.crear_usuario(5, 1, "error", 19)
    g.consultar_todas_claves()


    g.add_tareas_lista_usuario(2, ["a", "b", "c", "d", "e"])
    g.add_tareas_lista_usuario(4, ["1"])

    g.consultar_usuario(1)
    g.consultar_usuario(2)
