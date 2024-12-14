import requests
import pandas as pd
import io

weather_raw_file = '/opt/airflow/data/raw/weather_data.csv'
weather_processed_file = "/opt/airflow/data/processed/weather_data.csv"

def download_weather_data():
    url = "https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Tallinn-Harku-2004-juuni-2024.xlsx"
    response = requests.get(url)
    if response.status_code == 200:
        df = pd.read_excel(io.BytesIO(response.content), header=2)
        df.to_csv(weather_raw_file, index=False)
    else:
        print(f"Error: {response.status_code} - {response.text}")

download_weather_data()

def clean_weather_data():
    df = pd.read_csv(weather_raw_file)
    
    # Extract relevant columns
    relevant_columns = [
        "Aasta", "Kuu", "Päev", "Kell (UTC)", "Tunni keskmine summaarne kiirgus W/m²", "Õhutemperatuur °C",
        "Tunni sademete summa mm", "10 minuti keskmine tuule kiirus m/s", "10 minuti keskmine tuule suund °",
        "Suhteline õhuniiskus %", "Õhurõhk merepinna kõrgusel hPa"
    ]
    df = df[relevant_columns]

    # Rename columns for consistency
    rename_columns = {
        "Aasta": "year",
        "Kuu": "month",
        "Päev": "day",
        "Kell (UTC)": "time",
        "Tunni keskmine summaarne kiirgus W/m²": "sunlight",
        "Õhutemperatuur °C": "temperature",
        "Tunni sademete summa mm": "precipitation",
        "10 minuti keskmine tuule kiirus m/s": "wind_speed",
        "10 minuti keskmine tuule suund °": "wind_direction",
        "Suhteline õhuniiskus %": "humidity",
        "Õhurõhk merepinna kõrgusel hPa": "air_pressure"
    }
    df = df.rename(columns=rename_columns)
    
    # Convert date and time to datetime format
    df['hour'] = df['time'].astype(str).str[:2].astype(int)
    df.insert(0, 'datetime', pd.to_datetime(df[['year', 'month', 'day', 'hour']]))
    df = df.drop(columns=['year', 'month', 'day', 'time', 'hour'])
    
    df.fillna(-1, inplace=True)
    
    df.to_csv(weather_processed_file, index=False)