import requests
import pandas as pd
import json
import os

api_key = os.environ["OPENAQ_API_KEY"]

air_quality_raw_file = '/opt/airflow/data/raw/air_quality.json'
air_quality_processed_file = "/opt/airflow/data/processed/air_quality.csv"

def download_air_quality_data():
    # Download air quality data from OpenAQ API
    sensors = [23364, 23365, 23366, 23367, 23368]
    data = {'pages': []}
    
    for sensor in sensors:
        page = 1
        while True:
            url = f"https://api.openaq.org/v3/sensors/{sensor}/measurements/hourly?limit=1000&page={page}"
            response = requests.get(url, headers={"X-API-Key": api_key})
            if response.status_code == 200:
                data['pages'].append(response.json())
                total_entries = response.json()['meta']['found']
                if page * 1000 > total_entries:
                    break
                page += 1
            else:
                print(f"Error: {response.status_code} - {response.text}")
                break
    
    with open(air_quality_raw_file, 'w') as file:
        json.dump(data, file)
    
    
def clean_air_quality_data():
    with open(air_quality_raw_file) as file:
        json_data = json.load(file)
        
    data = []
    
    for page in json_data['pages']:
        for item in page['results']:
            data.append({
                'datetime': item['period']['datetimeTo']['utc'].replace('T', ' ').replace('Z', ''),
                'sensor_type': item['parameter']['name'],
                'value': item['value']})
        
    df = pd.DataFrame(data)
    df = df.pivot(index='datetime', columns='sensor_type', values='value').reset_index()
    df.fillna(-1, inplace=True)
    df.to_csv(air_quality_processed_file, index=False)
 
clean_air_quality_data()