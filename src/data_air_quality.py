import requests
import pandas as pd
import json
import os

# Get the OpenAQ API key from environment variables
api_key = os.environ["OPENAQ_API_KEY"]

# Define file paths for raw and processed air quality data
air_quality_raw_file = '/opt/airflow/data/raw/air_quality.json'
air_quality_processed_file = "/opt/airflow/data/processed/air_quality.csv"

def download_air_quality_data():
    # Downloads air quality data from the OpenAQ API and saves it as a raw JSON file
    
    sensors = [23364, 23365, 23366, 23367, 23368] # List of sensor IDs to fetch data from
    data = {'pages': []}
    
    for sensor in sensors:
        page = 1
        while True: # Loop until all pages for the sensor are fetched
            url = f"https://api.openaq.org/v3/sensors/{sensor}/measurements/hourly?limit=1000&page={page}"
            response = requests.get(url, headers={"X-API-Key": api_key})
            if response.status_code == 200:
                data['pages'].append(response.json())
                total_entries = response.json()['meta']['found'] # Get the total number of entries
                if page * 1000 > total_entries: # Check if all pages have been fetched
                    break
                page += 1
            else:
                print(f"Error: {response.status_code} - {response.text}")
                break
    
    with open(air_quality_raw_file, 'w') as file:
        json.dump(data, file) # Save the data to the JSON file
    
    
def clean_air_quality_data():
    # Cleans and preprocesses the raw air quality data
    
    with open(air_quality_raw_file) as file:
        json_data = json.load(file)
        
    data = []
    
    for page in json_data['pages']:
        for item in page['results']:
            data.append({ # Append the relevant data to the list
                'datetime': item['period']['datetimeTo']['utc'].replace('T', ' ').replace('Z', ''),
                'sensor_type': item['parameter']['name'],
                'value': item['value']})
        
    df = pd.DataFrame(data)
    df = df.pivot(index='datetime', columns='sensor_type', values='value').reset_index() # Pivot the DataFrame to have sensor types as columns
    df.fillna(-1, inplace=True)
    df.to_csv(air_quality_processed_file, index=False) # Save the cleaned DataFrame to a CSV file
 