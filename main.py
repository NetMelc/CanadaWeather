import requests
import os
from concurrent.futures import ThreadPoolExecutor
import time
import pandas as pd
from tqdm import tqdm


def get_url(station_id, year,month):
    return f'https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID={station_id}&Year={year}&Month={month}&Day=14&timeframe=1&submit=Download+Data'

def get_filename(station_id, year,month):
    return f'StationData_hourly/{station_id}/{station_id}_{year}_{month}.csv'

def is_data_fetched(station_id, year,month):
    return os.path.isfile(get_filename(station_id, year,month))

def get_weather_data_per_year(args):
    station_id, year,month = args
    # print('Fetching', station_id, year)
    url = get_url(station_id, year,month)
    out_path = get_filename(station_id, year,month)

    os.makedirs(f'StationData_hourly/{station_id}', exist_ok=True)

    file = requests.get(url)
    with open(out_path, 'wb') as f:
        f.write(file.content)
    
    print('Done', station_id, year,month)


df = pd.read_csv('stations.csv')
args = [(row['Station ID'], year,month) for _, row in df.iterrows() for year in range(int(row['HLY First Year']), int(row["HLY Last Year"]) + 1) if not is_data_fetched(str(row['Station ID']), year,month) ]

with ThreadPoolExecutor(max_workers=1) as executor:
    results = list(tqdm(executor.map(get_weather_data_per_year, args), total=len(args)))
