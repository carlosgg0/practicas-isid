from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

def main():

    # EJERCICIO 1: CONFIGURACIÓN Y LECTURA DE DATOS
    spark_session = SparkSession.builder.appName("Practica10").getOrCreate()

    fields = [StructField("id_empleado", IntegerType(), True),
              StructField("nombre", StringType(), True),
              StructField("apellido", StringType(), True),
              StructField("edad", IntegerType(), True),
              StructField("departamento", StringType(), True),
              StructField("puesto", StringType(), True),
              StructField("salario", DoubleType(), True),
              StructField("fecha_contratacion", DateType(), True),
              StructField("ciudad", StringType(), True),
              StructField("activo", BooleanType(), True)]
    
    schema = StructType(fields)

    # data_frame = spark_session.read \
    #     .format("csv") \
    #     .schema(schema) \
    #     .load("data/empleados.csv")
    
    # Otra forma de leer el csv sería:
    df = spark_session \
        .read \
        .csv(path=str(DATA_DIR / "empleados.csv"),
             schema=schema, header=True)



    # EJERCICIO 2: INSPECCIÓN DE DATOS
    
    df.printSchema()
    df.show(n=10)
    df.describe().show()

    
    # EJERCICIO 3: SELECCIÓN Y PROYECCIÓN

    df_seleccion = df.select("nombre", "apellido", "departamento")
    df_seleccion.show(10)

    df_ventas = df_seleccion.filter(col("departamento") == "Ventas")
    df_ventas.show()


    # EJERCICIO 4: TRANSFORMACIÓN DE COLUMNAS

    df.withColumn("nombre_apellidos", concat(col("nombre"), lit(" "), col("apellido"))).show()

    df.withColumn("bono", col("salario") * 0.05).show()

    df.withColumnRenamed("puesto", "cargo").show()

    # EJERCICIO 5: FILTRADO BÁSICO

    df.filter(col("salario") > 45000).select("nombre", "apellido", "departamento", "salario").show()

    df.filter(col("ciudad") == "Madrid").select("nombre", "apellido", "ciudad", "puesto").show()

    df.filter(col("activo") == True).select("nombre", "apellido", "activo").show()

    # EJERCICIO 6: AGRUPACIONES SIMPLES

    df.groupBy("departamento").agg(
        count("*"),
        mean("salario"),
        max("salario")
    ).show()


    # EJERCICIO 7: ORDENAMIENTO

    df.sort("salario", "nombre", ascending=[False, True]).show()


    # EJERCICIO 8: FUNCIONES DE STRING

    df_ejercicio_8 = \
        df.withColumn("nombre_upper", upper(col("nombre"))) \
        .withColumn("email", concat(col("nombre"), lit("."), col("apellido"), lit("@empresa.com"))) \
        .withColumn("tres_primeras", substring(col("apellido"), 1, 3))
    
    df_ejercicio_8.show()


    # EJERCICIO 9: FUNCIONES DE FECHA

    df.withColumn("antiguedad", datediff(current_date(), col("fecha_contratacion"))).show()
    

    # EJERCICIO 10: CONSULTAS SQL

    df.createOrReplaceTempView("empleados_asalariados")

    spark_session.sql(
        """
        select nombre, departamento, salario
        from empleados_asalariados as t
        where t.salario > 40000
        order by t.salario desc
        """
    ).show()

    # EJERCICIO 11: ESCRITURA

    df_ejercicio_8.write.csv(str(OUTPUT_DIR), mode="overwrite")
    
if __name__ == "__main__":
    main() 