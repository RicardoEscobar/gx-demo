import pandas as pd
from sqlalchemy import create_engine

# Parámetros de conexión
server = "TU_SERVIDOR"        # Ejemplo: "localhost" o "192.168.1.100"
database = "TU_BASE_DATOS"    # Nombre de la base de datos
username = "TU_USUARIO"
password = "TU_PASSWORD"

# Driver de ODBC (verifica el nombre instalado en tu sistema con: odbcinst -q -d -n)
driver = "ODBC Driver 17 for SQL Server"

# Crear cadena de conexión
connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}"

# Crear engine con SQLAlchemy
engine = create_engine(connection_string)

# Definir query
query = """
SELECT TOP 10 *
FROM TuTabla
"""

# Cargar a DataFrame
df = pd.read_sql(query, engine)

print(df.head())
