import pandas as pd
import numpy as np

# --- Definición de los datos ---

clientes_data = {
    'cliente_id': [1, 2, 3, 4, 5, 6, 7],
    'nombre': ['Ana García', 'Luis Martínez', 'Carlos Rodríguez', 'María López',
               'Pedro Sánchez', 'Laura Fernández', 'Sofia Ramirez'],
    'email': ['ana@gmail.com', 'luis@empresa.com', 'carlos@hotmail.com',
              'maria@gmail.com', 'pedro@yahoo.com', 'laura@gmail.com', 'sofia@empresa.com'],
    'ciudad': ['Madrid', 'Barcelona', 'Madrid', 'Valencia', 'Sevilla', 'Barcelona', 'Bilbao'],
    'saldo': ['1500.50', '800.75', '2200.00', '950.25', '3000.80', '1200.40', '750.90'],
    'fecha_registro': ['2023-01-15', '2022-03-22', '2023-05-10', '2021-11-30',
                       '2023-08-14', '2022-01-05', '2023-12-01'],
    'categoria': ['Premium', 'Standard', 'Premium', 'Standard', 'Premium', 'Standard', 'Standard']
}

pedidos_data = {
    'pedido_id': [101, 102, 103, 104, 105, 106, 107, 108, 109],
    'cliente_id': [1, 2, 1, 3, 4, 2, 1, 8, 3],
    'producto': ['Laptop', 'Tablet', 'Smartphone', 'Monitor', 'Teclado',
                 'Mouse', 'Tablet', 'Monitor', 'Laptop'],
    'cantidad': [1, 2, 1, 1, 3, 2, 1, 1, 1],
    'precio': [800, 300, 500, 250, 50, 25, 300, 250, 800],
    'fecha_pedido': ['2023-02-20', '2023-03-15', '2023-04-10', '2023-05-25',
                     '2023-06-05', '2023-07-18', '2023-08-20', '2023-09-01', '2023-10-15'],
    'estado': ['Entregado', 'Entregado', 'Pendiente', 'Entregado', 'Cancelado',
               'Entregado', 'Entregado', 'Pendiente', 'Pendiente']
}

productos_data = {
    'producto_id': ['P001', 'P002', 'P003', 'P004', 'P005'],
    'nombre': ['Laptop', 'Tablet', 'Smartphone', 'Monitor', 'Teclado'],
    'precio': ['1200.75', '450.50', '799.99', '299.00', '89.95'],
    'categoria': ['Tecnología', 'Tecnología', 'Tecnología', 'Oficina', 'Oficina'],
    'stock': [15, 30, 25, 40, 100]
}

# --- Creación de DataFrames ---

clientes_df = pd.DataFrame(clientes_data)
pedidos_df = pd.DataFrame(pedidos_data)
productos_df = pd.DataFrame(productos_data)

# --- Preparación de Datos (Corrección de Tipos y Cálculo de Total) ---

# Convertir tipos de datos
clientes_df['saldo'] = pd.to_numeric(clientes_df['saldo'])
clientes_df['fecha_registro'] = pd.to_datetime(clientes_df['fecha_registro'])
pedidos_df['fecha_pedido'] = pd.to_datetime(pedidos_df['fecha_pedido'])
productos_df['precio'] = pd.to_numeric(productos_df['precio'])

# Calcular columna 'total' en pedidos
pedidos_df['total'] = pedidos_df['cantidad'] * pedidos_df['precio']

print("--- DataFrames Iniciales (head) ---")
print("\nClientes DF:")
print(clientes_df.head())
print("\nPedidos DF (con 'total' calculado):")
print(pedidos_df.head())
print("\nProductos DF:")
print(productos_df.head())
print("\n" + "="*40 + "\n")


# --- Ejecución de Consultas SQL con Pandas ---

# 1. SELECT nombre, ciudad FROM clientes
print("--- 1. SELECT nombre, ciudad FROM clientes ---")
print(clientes_df[['nombre', 'ciudad']])

# 2. SELECT * FROM clientes WHERE ciudad = 'Madrid'
print("\n--- 2. SELECT * FROM clientes WHERE ciudad = 'Madrid' ---")
print(clientes_df[clientes_df['ciudad'] == 'Madrid'])

# 3. SELECT * FROM clientes WHERE ciudad = 'Madrid' AND categoria = 'Premium'
print("\n--- 3. SELECT * FROM clientes WHERE ciudad = 'Madrid' AND categoria = 'Premium' ---")
print(clientes_df[(clientes_df['ciudad'] == 'Madrid') & (clientes_df['categoria'] == 'Premium')])

# 4. SELECT * FROM clientes WHERE ciudad = 'Madrid' OR ciudad = 'Barcelona'
print("\n--- 4. SELECT * FROM clientes WHERE ciudad = 'Madrid' OR ciudad = 'Barcelona' ---")
print(clientes_df[(clientes_df['ciudad'] == 'Madrid') | (clientes_df['ciudad'] == 'Barcelona')])
# Alternativa (usada en Q18): print(clientes_df[clientes_df['ciudad'].isin(['Madrid', 'Barcelona'])])

# 5. SELECT nombre, ciudad FROM clientes ORDER BY ciudad DESC, nombre ASC
print("\n--- 5. SELECT nombre, ciudad FROM clientes ORDER BY ciudad DESC, nombre ASC ---")
print(clientes_df[['nombre', 'ciudad']].sort_values(by=['ciudad', 'nombre'], ascending=[False, True]))

# 6. SELECT * FROM clientes LIMIT 3
print("\n--- 6. SELECT * FROM clientes LIMIT 3 ---")
print(clientes_df.head(3))

# 7. SELECT COUNT(*) FROM clientes
print("\n--- 7. SELECT COUNT(*) FROM clientes ---")
print(f"Total clientes: {len(clientes_df)}")

# 8. SELECT ciudad, COUNT(*) FROM clientes GROUP BY ciudad
print("\n--- 8. SELECT ciudad, COUNT(*) FROM clientes GROUP BY ciudad ---")
print(clientes_df.groupby('ciudad').size().reset_index(name='count'))

# 9. SELECT cliente_id, COUNT(*), SUM(total), AVG(total) FROM pedidos GROUP BY cliente_id
print("\n--- 9. SELECT cliente_id, COUNT(*), SUM(total), AVG(total) FROM pedidos GROUP BY cliente_id ---")
print(pedidos_df.groupby('cliente_id').agg(
    count_pedidos=pd.NamedAgg(column='pedido_id', aggfunc='count'),
    sum_total=pd.NamedAgg(column='total', aggfunc='sum'),
    avg_total=pd.NamedAgg(column='total', aggfunc='mean')
).reset_index())

# 10. SELECT ciudad, COUNT(*) as cnt FROM clientes GROUP BY ciudad HAVING cnt > 1
print("\n--- 10. SELECT ciudad, COUNT(*) as cnt FROM clientes GROUP BY ciudad HAVING cnt > 1 ---")
grouped_ciudad = clientes_df.groupby('ciudad').size().reset_index(name='cnt')
print(grouped_ciudad[grouped_ciudad['cnt'] > 1])

# 11. SELECT c.nombre, p.producto, p.total FROM clientes c JOIN pedidos p ON c.cliente_id = p.cliente_id
print("\n--- 11. (INNER JOIN) SELECT c.nombre, p.producto, p.total FROM clientes c JOIN pedidos p ---")
merged_inner = pd.merge(clientes_df, pedidos_df, on='cliente_id')
print(merged_inner[['nombre', 'producto', 'total']])

# 12. SELECT c.nombre, p.producto FROM clientes c LEFT JOIN pedidos p ON c.cliente_id = p.cliente_id
print("\n--- 12. (LEFT JOIN) SELECT c.nombre, p.producto FROM clientes c LEFT JOIN pedidos p ---")
merged_left = pd.merge(clientes_df, pedidos_df, on='cliente_id', how='left')
print(merged_left[['nombre', 'producto']])

# 13. SELECT c.nombre, p.pedido_id FROM clientes c RIGHT JOIN pedidos p ON c.cliente_id = p.cliente_id
print("\n--- 13. (RIGHT JOIN) SELECT c.nombre, p.pedido_id FROM clientes c RIGHT JOIN pedidos p ---")
merged_right = pd.merge(clientes_df, pedidos_df, on='cliente_id', how='right')
print(merged_right[['nombre', 'pedido_id', 'cliente_id']]) # Añadido cliente_id para contexto

# 14. SELECT nombre, email FROM clientes WHERE UPPER(nombre) LIKE '%A%' OR UPPER(email) LIKE '%GMAIL%'
print("\n--- 14. SELECT ... WHERE UPPER(nombre) LIKE '%A%' OR UPPER(email) LIKE '%GMAIL%' ---")
result_like = clientes_df[
    clientes_df['nombre'].str.upper().str.contains('A') |
    clientes_df['email'].str.upper().str.contains('GMAIL')
]
print(result_like[['nombre', 'email']])

# 15. SELECT UPPER(nombre), LOWER(ciudad) FROM clientes
print("\n--- 15. SELECT UPPER(nombre), LOWER(ciudad) FROM clientes ---")
result_case = pd.DataFrame({
    'nombre_upper': clientes_df['nombre'].str.upper(),
    'ciudad_lower': clientes_df['ciudad'].str.lower()
})
print(result_case)

# 16. SELECT TO_CHAR(fecha_registro, 'YYYY-MM') as año_mes, TO_CHAR(fecha_registro, 'DD/MM/YYYY') as fecha FROM clientes
print("\n--- 16. SELECT FORMAT(fecha_registro, 'YYYY-MM') as año_mes, ... ---")
result_date = pd.DataFrame({
    'año_mes': clientes_df['fecha_registro'].dt.strftime('%Y-%m'),
    'fecha': clientes_df['fecha_registro'].dt.strftime('%d/%m/%Y')
})
print(result_date)

# 17. SELECT ROUND(saldo, 0), FLOOR(saldo) FROM clientes;
print("\n--- 17. SELECT ROUND(saldo, 0), FLOOR(saldo) FROM clientes ---")
result_numeric = pd.DataFrame({
    'saldo_round': clientes_df['saldo'].round(0),
    'saldo_floor': np.floor(clientes_df['saldo'])
})
print(result_numeric)

# 18. SELECT * FROM clientes WHERE ciudad IN ('Madrid', 'Barcelona')
print("\n--- 18. SELECT * FROM clientes WHERE ciudad IN ('Madrid', 'Barcelona') ---")
print(clientes_df[clientes_df['ciudad'].isin(['Madrid', 'Barcelona'])])

# 19. SELECT nombre FROM clientes c WHERE EXISTS (SELECT 1 FROM pedidos p WHERE p.cliente_id = c.cliente_id AND p.total > 400)
print("\n--- 19. SELECT ... WHERE EXISTS (SELECT ... p.total > 400) ---")
# 1. Encontrar los cliente_id en pedidos que cumplen la condición
pedidos_filtrados = pedidos_df[pedidos_df['total'] > 400]
clientes_con_pedidos_grandes = pedidos_filtrados['cliente_id'].unique()
# 2. Filtrar clientes que están en esa lista
result_exists = clientes_df[clientes_df['cliente_id'].isin(clientes_con_pedidos_grandes)]
print(result_exists[['nombre']])

# 20. SELECT nombre FROM clientes WHERE cliente_id NOT IN (SELECT cliente_id FROM pedidos)
print("\n--- 20. SELECT ... WHERE cliente_id NOT IN (SELECT cliente_id FROM pedidos) ---")
# 1. Encontrar todos los cliente_id que SÍ están en pedidos
clientes_con_pedidos = pedidos_df['cliente_id'].unique()
# 2. Filtrar clientes que NO están (~) en esa lista
result_not_in = clientes_df[~clientes_df['cliente_id'].isin(clientes_con_pedidos)]
print(result_not_in[['nombre']])