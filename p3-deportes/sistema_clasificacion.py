import redis

"""
Dato            Tipo Redis          Clave -> Contenido
------------------------------------------------------
Equipo          Hash                equipo:ID -> {nombre, ciudad, entrenador, anyo_fund}
Jugador         Hash                jugador:ID -> {nombre, edad, equipo, posicion}
Clasificacion   SortedSet           clasificacion -> {(ID_equipo: puntos), ...}
Partido         Hash                partido:ID -> {local, goles_local, visitante, goles_visitante, fecha}
PartidosJugados SortedSet           partidosJugados -> {(ID_partido: fecha)}
Goleadores      SortedSet           goleadores -> {(ID_jugador: goles)}
Calendario      SortedSet           calendario -> {({nombre_partido}: fecha)}
Traspaso        Hash                traspaso:ID -> {jugador, equipo_inicial, equipo_final, fecha}
HistTraspasos   SortedSet           histTraspasos -> {(ID_traspaso: fecha)}
"""

class SistemaClasificacion:
    def __init__(self):
        """Conectar a la base de datos"""
        try:
            self.r = redis.Redis(
                host="localhost", port=6379, db=0, decode_responses=True
            )
            self.r.flushdb()
        except redis.ConnectionError:
            print("Error: no se pudo conectar a la base de datos")



    def registrar_equipo(
        self,
        id_equipo: int,
        nombre: str, ciudad: str,
        entrenador: str, anyo_fund: int
    ) -> None:
        
        fields = {
            "nombre": nombre,
            "ciudad": ciudad,
            "entrenador": entrenador,
            "anyo_fund": anyo_fund
        }

        # Guardamos toda la información del partido en un hash
        self.r.hset(name=f"equipo:{id_equipo}", mapping=fields)
        
        # Registramos inicialmente al equipo en la clasificación con 0 puntos
        self.r.zadd("clasificacion", mapping={id_equipo: 0})

        # Añadimos un index para poder recuperarlo por su nombre
        self.r.set(name=nombre, value=id_equipo)
        
        

    def registrar_jugador(
        self,
        id_jugador: int, nombre: str,
        edad: int, equipo: str,
        posicion: str
    ) -> None:
        
        fields = {
            "nombre": nombre,
            "edad": edad,
            "equipo": equipo,
            "posicion": posicion
        }

        # Guardamos toda la info del jugador en un hash
        self.r.hset(f"jugador:{id_jugador}", mapping=fields)

        # Todos los jugadores empiezan en el ranking con 0 goles
        self.r.zadd("goleadores", mapping={id_jugador: 0})

        # Añadimos un index para poder recuperarlo por su nombre
        self.r.set(name=nombre, value=id_jugador)
        


    def registrar_partido(
        self,
        id_partido: int,
        local: str, goles_local: int,
        visitante: str, goles_visitante: int,
        fecha: int,
        goles_jugadores: dict[str, int]
    ) -> None:
        
        fields = {
            "local": local, "goles_local": goles_local,
            "visitante": visitante, "goles_visitante": goles_visitante,
            "fecha": fecha
        }

        # Guardamos toda la info sobre el partido en un hash
        self.r.hset(f"partido:{id_partido}", mapping=fields)
        
        # Se registra en orden cronológico en nuestro historial de partidos
        self.r.zadd("partidosJugados", {id_partido: fecha})

        # Cuando se juega un partido se debe actualizar la clasificación
        # Victoria = 3 puntos,  Empate = 1 punto,   Derrota = 0 puntos
        
        puntos_local = 0
        puntos_visitante = 0
        
        if goles_local > goles_visitante:
            puntos_local = 3
        elif goles_local < goles_visitante:
            puntos_visitante = 3
        else: # Empate
            puntos_local = 1
            puntos_visitante = 1

        id_local = self.r.get(local)
        assert id_local is not None

        id_visitante = self.r.get(visitante)
        assert id_visitante is not None

        self.r.zincrby("clasificacion", value=id_local, amount=puntos_local)
        self.r.zincrby("clasificacion", value=id_visitante, amount=puntos_visitante)

        # También actualizaremos el ranking de goleadores
        for jugador, goles in goles_jugadores.items():
            self.r.zincrby(
                name="goleadores",
                value=self.r.get(jugador),
                amount=goles
            )


    def registrar_traspaso(
        self, id_traspaso: int,
        nombre_jugador: str,
        equipo_inicial: str, equipo_final: str,
        fecha: int
    ) -> None:
        fields = {
            "nombre_jugador": nombre_jugador,
            "equipo_inicial": equipo_inicial,
            "equipo_final": equipo_final,
            "fecha": fecha
        }
        self.r.hset(f"traspaso:{id_traspaso}", mapping=fields)
        
        self.r.zadd("histTraspasos", mapping={id_traspaso: fecha})

        # Reflejamos el cambio en el jugador
        id_jugador = self.r.get(nombre_jugador)
        self.r.hset(f"jugador:{id_jugador}", key="equipo", value=equipo_final)

    
    def programar_proximo_partido(
        self, nombre_partido: str,
        tiempo_restante: int # (En semanas)
    ) -> None:
        self.r.zadd("calendario", mapping={nombre_partido: tiempo_restante})

    
    def mostrar_clasificacion(self) -> None:
        res = self.r.zrevrange(name="clasificacion", start=0, end=-1, withscores=True)
        print("Clasificación de equipos: ")

        for k, v in res:
            print(f"{self.r.hget(f"equipo:{k}", "nombre")}: {v} puntos")      
        
        print("\n")


    def list_max_goleadores(self, n: int) -> None:
        res = self.r.zrevrange(name="goleadores", start=0, end=n-1, withscores=True)
        print("Maximos goleadores: ")
        
        for k, v in res:
            print(f"{self.r.hget(f"jugador:{k}", "nombre")}: {v} goles")   
        
        print("\n")

    
    def mostrar_historial_partidos(self) -> None:
        res = self.r.zrevrange(name="partidosJugados", start=0, end=-1, withscores=True)
        print("Historial de partidos: (Local, Visitante, Fecha)")

        for k, v in res:
            local = self.r.hget(name=f"partido:{k}", key="local")
            visitante = self.r.hget(name=f"partido:{k}", key="visitante")
            fecha = self.r.hget(name=f"partido:{k}", key="fecha")

            print(f"{local}, {visitante}, {fecha}")

if __name__ == "__main__":
    sc = SistemaClasificacion()

    # Ejercicios 1 y 2
    sc.registrar_equipo(1, "Leones FC", "Madrid", "Carlos Ruiz", 1920)
    sc.registrar_equipo(2, "Aguilas Deportivas", "Barcelona", "Ana Martinez", 1935)
    sc.registrar_equipo(3, "Tiburones FC", "Valencia", "David Gonzalez", 1948)


    # Ejercicios 3 y 4
    sc.registrar_jugador(1, "Luis Torres", 25, "Leones FC", "delantero")
    sc.registrar_jugador(2, "Maria Rodriguez", 28, "Aguilas Deportivas", "centrocampista")
    sc.registrar_jugador(3, "Javier Lopez", 22, "Tiburones FC", "defensa")
    sc.registrar_jugador(4, "Sofia Garcia", 26, "Leones FC", "delantero")


    # Ejercicio 5, 6 y 7
    sc.registrar_partido(1, "Leones FC", 2, "Aguilas Deportivas", 1,   1,
        goles_jugadores={"Luis Torres": 2, "Maria Rodriguez": 1}
    )
    sc.registrar_partido(2, "Tiburones FC", 0, "Leones FC", 0, 2,
        goles_jugadores= {}
    )
    sc.registrar_partido(3, "Aguilas Deportivas", 3, "Tiburones FC", 2, 3,
        goles_jugadores={"Sofia Garcia": 1, "Maria Rodriguez": 2}
    )

    # Ejercicio 8
    sc.programar_proximo_partido("Leones FC vs Tiburones FC", 1)
    sc.programar_proximo_partido("Aguilas Deportivas vs Leones FC", 2)

    # Ejercicio 9
    sc.mostrar_clasificacion()
    sc.list_max_goleadores(3)
    sc.mostrar_historial_partidos()

    # Ejercicio 10
    sc.registrar_traspaso(1, "Sofia Garcia", "Leones FC", "Aguilas Deportivas", 1)
    sc.r.hgetall(f"jugador:{sc.r.get("Sofia Garcia")}")