import json
import os
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv(Path("../.env"))
nasa_key = os.getenv("NASA_KEY")

"""
Service Outage notice.
"""
# def load_picture_of_the_day():
#     # checking status:
#     response = requests.get(f"https://api.nasa.gov/planetary/apod?api_key={nasa_key}")
#     try:
#         if response.status_code == 200:
#             data = response.json()  # Parse as Json
#             with open("api_data.json", "w") as f:
#                 json.dump(data, f, indent=4)
#             print("Data saved into Json!")
#     except Exception as e:
#         print(f"X Api call failed with status code {response.status_code}")
# start_time = time.perf_counter()
# load_picture_of_the_day()
# end_time = time.perf_counter()
# print(f"execution time : {end_time - start_time}")


def retrieve_asteroids(start_date: str, end_date: str):
    response = requests.get(
        f"https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={end_date}&api_key={nasa_key}"
    )

    try:
        if response.status_code == 200:
            data = response.json()
            with open("asteroid.json", "w") as file:
                json.dump(data, file, indent=4)
            print("Data saved into json!")
    except Exception as e:
        print(f"{e} {response.status_code}")


start_time = time.perf_counter()
# Format 2015-09-07
start_date = "2025-11-09"
end_date = "2025-11-10"
retrieve_asteroids(start_date, end_date)
end_time = time.perf_counter()

# print(f"execution time : {end_time - start_time}")


# df = pd.read_json("asteroid.json")


# for date, asteroids in df["near_earth_objects"].items():
#     print(f"Date: {date}")
#     for asteroid in asteroids:
#         print(asteroid)
#         # print(f"Asteroid name: {asteroid['name']}")
#         # print(f"Asteroid id: {asteroid['id']}")
#         # print(f"Asteroid absolute magnitude: {asteroid['absolute_magnitude_h']}")
#         # print(f"Asteroid diameter: {asteroid['estimated_diameter']['kilometers']}")
