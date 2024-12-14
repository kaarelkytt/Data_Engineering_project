import pandas as pd

def join_data():
    air_quality_df = pd.read_csv("/opt/airflow/data/processed/air_quality.csv")
    weather_data_df = pd.read_csv("/opt/airflow/data/processed/weather_data.csv")
    
    air_quality_df['datetime'] = pd.to_datetime(air_quality_df['datetime'])
    weather_data_df['datetime'] = pd.to_datetime(weather_data_df['datetime'])
    
    analysis_df = pd.merge(air_quality_df, weather_data_df, on='datetime', how='inner')
    
    analysis_df.to_csv("/opt/airflow/data/joined/data.csv", index=False)