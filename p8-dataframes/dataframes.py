import pandas as pd
import numpy as np

# Ejercicio 1. Crea un DataFrame a partir del siguiente diccionario:
print("\nEjercicio 1")

datos = {
    'Producto': ['Laptop', 'Mouse', 'Teclado', 'Monitor', 'Tablet'],
    'Precio': [1200, 25, 80, 300, 450],
    'Stock': [15, 100, 50, 30, 25],
    'Categoria': ['Electrónica', 'Accesorio', 'Accesorio', 'Electrónica', 'Electrónica']
}

df1 = pd.DataFrame(datos)

print(df1)

# Ejercicio 2. Crear otro dataframe a partir de la siguiente lista:
print("\nEjercicio 2")

datos = [
    [1, 'Laptop', 1200, 15],
    [2, 'Mouse', 25, 100],
    [3, 'Teclado', 80, 50],
    [4, 'Monitor', 300, 30],
    [5, 'Tablet', 450, 25]
]

arr = np.array(datos)

df2 = pd.DataFrame(arr[:, 1:], index=arr[:, 0])

print(df2)


# Ejercicio 3. Muestra los primeros 3 registros de cada DataFrame
print("\nEjercicio 3")
print(df1.head(3))
print(df2.head(3))

# Ejercicio 4
print("\nEjercicio 4")
print("shape: ", df1.shape)
print("columns: ", df1.columns)
print("datatypes:\n", df1.dtypes)
print("Estadísticas:\n", df1.describe())
print(df1.tail(2))
print(df1.sample(2))

# Ejercicio 5
print("\nEjercicio 5")
print(df1["Producto"]) # equivalente a df1.Producto
print(df1.loc[:, ["Producto", "Precio"]])
print(df1.loc[0])
print(df1.iloc[1, 2])
print(df1.loc[0:1])

# Ejercicio 6
np.random.seed(42)
datos_ventas = {
    'Fecha': pd.date_range('2024-01-01', periods=100, freq='D'),
    'Producto': np.random.choice(['Laptop', 'Mouse', 'Teclado', 'Monitor',
    'Tablet'], 100),
    'Cantidad': np.random.randint(1, 10, 100),
    'Precio_Unitario': np.random.choice([1200, 25, 80, 300, 450], 100),
    'Region': np.random.choice(['Norte', 'Sur', 'Este', 'Oeste'], 100),
    'Vendedor': np.random.choice(['Ana', 'Carlos', 'Maria', 'Pedro'], 100)
}
df_ventas = pd.DataFrame(datos_ventas)
df_ventas['Venta_Total'] = df_ventas['Cantidad'] * df_ventas['Precio_Unitario']
print(df_ventas)

print(df_ventas[df_ventas["Producto"] == "Laptop"])
print(df_ventas[df_ventas["Venta_Total"] > 1000].head())
print(df_ventas[df_ventas[["Vendedor", "Region"]] == ["Maria", "Norte"]])