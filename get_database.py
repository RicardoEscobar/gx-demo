import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine

# Cargar variables del archivo .env
load_dotenv()

server = os.getenv("DB_SERVER")
database = os.getenv("DB_DATABASE")
driver = os.getenv("DB_DRIVER")

# Crear cadena de conexión
connection_string = f"mssql+pyodbc://{server}/{database}?driver={driver}&trusted_connection=yes"

# Crear engine con SQLAlchemy
engine = create_engine(connection_string)

# Ejemplo de query
query = """select top 50 *
FROM pos.DailyInventoryHistoryDetails
order by PosDailyInventoryHistoryDetailsKey desc;"""

df = pd.read_sql(query, engine)

print(df.head())
