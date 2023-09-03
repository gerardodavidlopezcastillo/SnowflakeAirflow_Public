import snowflake.connector

# Configura los parámetros de conexión
config = {
    "user": "",
    "password": "",
    "account": "us-east-2.aws.snowflakecomputing.com",
    "warehouse": "dwhgt",
    "database": "LEAGUES",
    "schema": "PUBLIC"
}

# Establece la conexión
connection = snowflake.connector.connect(**config)
print('Conexion exitosa!...')

# Crea un cursor para ejecutar consultas
cursor = connection.cursor()

# Ejemplo: ejecuta una consulta
query = "SELECT * FROM FOOTBALL_LEAGUES LIMIT 10"
cursor.execute(query)

# Recupera los resultados
results = cursor.fetchall()

# Imprime los resultados
for row in results:
    print(row)

# Cierra el cursor y la conexión
cursor.close()
connection.close()