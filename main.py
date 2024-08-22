import requests
import os
from concurrent.futures import ThreadPoolExecutor
import time
import pandas as pd
from tqdm import tqdm


def get_url(station_id, year):
    return f'https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID={station_id}&Year={year}&Month=8&Day=14&timeframe=2&submit=Download+Data'

def get_filename(station_id, year):
    return f'StationData/{station_id}/{station_id}_{year}.csv'

def is_data_fetched(station_id, year):
    return os.path.isfile(get_filename(station_id, year))

def get_weather_data_per_year(args):
    station_id, year = args
    # print('Fetching', station_id, year)
    url = get_url(station_id, year)
    out_path = get_filename(station_id, year)

    os.makedirs(f'StationData/{station_id}', exist_ok=True)

    file = requests.get(url)
    with open(out_path, 'wb') as f:
        f.write(file.content)
    
    # print('Done', station_id, year)


df = pd.read_csv('stations.csv')
args = [(row['Station ID'], year) for _, row in df.iterrows() for year in range(int(row['First Year']), int(row["Last Year"]) + 1) if not is_data_fetched(str(row['Station ID']), year) ]

with ThreadPoolExecutor(max_workers=100) as executor:
    results = list(tqdm(executor.map(get_weather_data_per_year, args), total=len(args)))
