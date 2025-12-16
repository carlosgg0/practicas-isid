from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

def main():

    # EJERCICIO 1: LECTURA CON ESQUEMAS EXPLÍCITOS

    spark = SparkSession.builder.appName("Lab11").getOrCreate()

    fields_clientes = [
        StructField("cliente_id", IntegerType(), True),
        StructField("nombre", StringType(), True),
        StructField("ciudad", StringType(), True),
        StructField("pais", StringType(), True),
        StructField("fecha_registro", DateType(), True),
        StructField("vip", BooleanType(), True),
        StructField("categoria_cliente", StringType(), True)
    ]
    
    schema_clientes = StructType(fields_clientes)
    
    df_clientes = spark.read.csv(
        str(DATA_DIR / "clientes.csv"), 
        schema=schema_clientes, 
        header=True
    )

    fields_ventas = [
        StructField("venta_id", IntegerType(), True),
        StructField("fecha", DateType(), True),
        StructField("cliente_id", IntegerType(), True),
        StructField("producto", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("cantidad", IntegerType(), True),
        StructField("precio", DoubleType(), True),
        StructField("region", StringType(), True),
        StructField("descuento", DoubleType(), True),
        StructField("vendedor_id", IntegerType(), True),
        StructField("total", DoubleType(), True)
    ]
    
    schema_ventas = StructType(fields_ventas)
    
    df_ventas = spark.read.csv(
        str(DATA_DIR / "ventas.csv"), 
        schema=schema_ventas, 
        header=True
    )

    df_clientes.printSchema()
    df_ventas.printSchema() 

    df_clientes.show(n=5)
    df_ventas.show(n=5)

    print(f"Total clientes: {df_clientes.count()}")
    print(f"Total ventas: {df_ventas.count()}")

    
    # EJERCICIO 2: SELECCIÓN Y TRANSFORMACIÓN

    df_ventas.select("venta_id", "producto", "cantidad", "precio", "total").show(n=5)

    df_ventas = df_ventas.withColumn("precio_con_iva", col("precio") * 1.21)

    df_ventas = df_ventas.withColumnsRenamed({"precio": "precio_unitario", "total": "precio_final"})

    df_ventas = df_ventas.withColumn(
        "tamaño", 
        when(col("precio_final") > 800, "Grande").otherwise(
            when(col("precio_final") < 300, "Pequeña").otherwise("Mediana")
        ) 
    )
    
    df_ventas.sort("precio_final", ascending=True).show()

    
    # EJERCICIO 3: FILTRADO CON CONDICIONES

    df_ventas.filter(col("precio_final") > 500).show()

    df_ventas.filter((col("categoria") == "Electrónicos") | (col("categoria") == "Computación")).show()

    df_ventas.filter((col("cantidad") > 2) & (col("descuento") > 0.1)).show()

   
    # EJERCICIO 4: AGRUPACIONES Y CÁLCULOS

    df_ventas.groupBy("categoria").agg(
        count("*").alias("num_ventas"),
        sum("precio_final").alias("total_vendido"),
        avg("precio_final").alias("promedio_venta")
    ).show()

    df_ventas.groupBy("region").agg(
        count("*").alias("num_ventas"),
        sum("precio_final").alias("total_vendido"),
        avg("descuento").alias("descuento_promedio")
    ).show()

    df_ventas.groupBy("producto").agg(
        sum("cantidad").alias("cantidad_total")
    ).sort("cantidad_total", ascending=False).show(1)


    # EJERCICIO 5: RELLENAR VALORES NA CON MEDIAS

    analize_cols = ["precio_unitario", "descuento", "cantidad", "region"]
    df_ventas.select([count(when(col(c).isNull(), 1)).alias(c) for c in analize_cols]).show()


    df_ventas_limpio = df_ventas.fillna({
        "precio_unitario": 500.0,
        "descuento": 0.0,
        "cantidad": 2,
        "region": "Desconocida"
    })

    df_ventas_limpio.show(50)

    # EJERCICIO 6: LEFT JOIN SIMPLE
    
    df_combinado = df_ventas_limpio.join(
        other=df_clientes, 
        on="cliente_id", 
        how="left"
    )

    df_combinado.select("venta_id", "producto", "precio_final", "nombre", "ciudad").show()

    df_combinado.groupBy("ciudad").agg(
        count("*").alias("ventas_totales")
    ).sort("ventas_totales", ascending=False).show()


    # EJERCICIO 7: CONSULTAS SQL EN SPARK

    df_clientes.createOrReplaceTempView("vista_clientes")
    df_ventas_limpio.createOrReplaceTempView("vista_ventas")

    spark.sql("select * from vista_ventas limit 10").show()

    spark.sql(
        """
        select categoria, count(*) as num_ventas, sum(precio_final) as total, avg(precio_final) as promedio
        from vista_ventas as v
        group by v.categoria
        """
    ).show()

    spark.sql(
        """
        select venta_id, producto, precio_final, nombre, ciudad
        from vista_ventas as v left join vista_clientes as c
        on v.cliente_id = c.cliente_id
        """
    ).show()

    spark.sql(
        """
        select count(*) as total_ventas, 
            sum(precio_final) as ingresos, 
            avg(precio_final) as promedio,
            sum(cantidad) as unidades_vendidas 
        from vista_ventas
        """
    ).show()


if __name__ == "__main__":
    main()