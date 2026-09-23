import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
# Connect to engine

user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT")
database = os.getenv("POSTGRES_DB")

DATABASE_URL = f"postgresql://{user}:{password}@{host}:{port}/{database}"

# How many asteroids approach earth each year and month?

query = """

SELECT * FROM asteroid_approaches

	
"""

engine = create_engine(DATABASE_URL)

df = pd.read_sql(query, engine)

df.to_csv("Asteroid_approaches.csv")




