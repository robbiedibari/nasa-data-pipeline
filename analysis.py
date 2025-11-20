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
SELECT
	EXTRACT (YEAR FROM close_approach_date),
	EXTRACT(MONTH from close_approach_date),
	COUNT(*) As Num_of_asteroid_each_month,
	SUM(COUNT(*)) OVER(PARTITION BY EXTRACT(MONTH FROM close_approach_date)) as Total_Asteroid_Each_Month_Accross_All_years,
	ROUND(AVG(COUNT(*)) OVER (PARTITION BY EXTRACT(month FROM close_approach_date)),2) as Avg_Monthly_by_years,
	ROUND(AVG(COUNT(*)) OVER (PARTITION BY EXTRACT(year FROM close_approach_date)),2) As Avg_year,
	SUM(COUNT(*)) OVER (PARTITION BY EXTRACT(YEAR FROM close_approach_date)) As Total_Asteroid_EachYear
FROM asteroid_approaches
GROUP BY
	EXTRACT (YEAR FROM close_approach_date),
	EXTRACT(MONTH from close_approach_date)
ORDER BY EXTRACT (YEAR FROM close_approach_date),
	EXTRACT(MONTH FROM close_approach_date);
    """

engine = create_engine(DATABASE_URL)

df = pd.read_sql(query, engine)

print(df.head())
