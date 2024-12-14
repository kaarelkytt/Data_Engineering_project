import pandas as pd
import duckdb
    
    
def create_database():

    # Creates a DuckDB database and populates it with the joined data.
    # The database uses a star schema with a fact table (combined_data) and two dimension tables (DimTime and DimWeather).

    df = pd.read_csv("/opt/airflow/data/joined/data.csv")
    df['datetime'] = pd.to_datetime(df['datetime'])
    
    conn = duckdb.connect('/opt/airflow/db/duck.db') # Connect to DuckDB database
        
    # Create the fact table (combined_data)
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS combined_data (
            co REAL,
            no2 REAL,
            o3 REAL,
            pm10 REAL,
            so2 REAL,
            time_id INTEGER,
            weather_id INTEGER
        );
        """
    )
    
    # Create the DimTime dimension table
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS DimTime (
            time_id INTEGER PRIMARY KEY,
            datetime DATETIME,
            hour INTEGER,
            day INTEGER,
            month INTEGER,
            year INTEGER
        );
        """
    )
    
    # Create the DimWeather dimension table
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS DimWeather (
            weather_id INTEGER PRIMARY KEY,
            temperature REAL,
            precipitation REAL,
            wind_speed REAL,
            sunlight REAL,
            wind_direction REAL,
            humidity REAL,
            air_pressure REAL
        );
        """
    )
    
    
    for index, row in df.iterrows():
        # Insert data into the fact table
        conn.sql(
            f"""
            INSERT INTO combined_data (co, no2, o3, pm10, so2, time_id, weather_id)
            VALUES ({row['co']}, {row['no2']}, {row['o3']}, {row['pm10']}, {row['so2']}, {index + 1}, {index + 1});
            """
        )
        
        # Insert data into the DimTime table
        conn.sql(
            f"""
            INSERT INTO DimTime (time_id, datetime, hour, day, month, year)
            VALUES ({index + 1}, '{row['datetime']}', {row['datetime'].hour}, {row['datetime'].day}, {row['datetime'].month}, {row['datetime'].year});
            """
        )
        
        # Insert data into the DimWeather table
        conn.sql(
            f"""
            INSERT INTO DimWeather (weather_id, temperature, precipitation, wind_speed, sunlight, wind_direction, humidity, air_pressure)
            VALUES ({index + 1}, {row['temperature']}, {row['precipitation']}, {row['wind_speed']}, {row['sunlight']}, {row['wind_direction']}, {row['humidity']}, {row['air_pressure']});
            """
        )
            
    conn.close() # Close the database connection

