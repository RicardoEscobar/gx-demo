import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine

# Cargar variables del archivo .env
load_dotenv()

server = os.getenv("DB_SERVER")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
driver = os.getenv("DB_DRIVER")

# Crear cadena de conexión
connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}"

# Crear engine con SQLAlchemy
engine = create_engine(connection_string)

# Ejemplo de query
query = "SELECT TOP 10 * FROM TuTabla"

df = pd.read_sql(query, engine)

print(df.head())
