# Air Quality and Weather Analysis Project

This project analyzes the relationship between air quality and weather conditions using data from OpenAQ and Keskkonnaagentuur.

## Business Side

*   Project Goal: To understand how weather conditions influence air quality in Estonia.
*   Data Sources:
    *   Air quality data from OpenAQ (https://openaq.org/)
    *   Weather data from the Estonian Weather Service (https://www.ilmateenistus.ee/kliima/ajaloolised-ilmaandmed/)
*   Questions:
    *   How do different weather variables (temperature, precipitation, wind speed, etc.) correlate with air quality?
    *   Are there specific weather patterns that lead to improved or worsened air quality?
    *   Can we identify trends or seasonal variations in air quality based on weather conditions?

## Technical Side

*   Tools:
    *   Python with pandas and requests for data acquisition and cleaning.
    *   Apache Airflow for orchestrating the data pipeline.
    *   DuckDB for data storage and analysis.
    *   Docker for containerization and environment management.
*   Project Structure:
    *   `data/`: Contains raw and processed data files.
    *   `src/`: Contains Python scripts for data acquisition, cleaning, transformation, and modeling.
    *   `dags/`: Contains the Airflow DAG definition file.
    *   `Dockerfile`: Defines the Docker image for the project.
    *   `compose.yml`: Orchestrates the Airflow and DuckDB containers.

## Getting Started

1.  Clone the repository: `git clone https://https://github.com/kaarelkytt/Data_Engineering_project`
2.  Set the OPENAQ_API_KEY environment variable:
    *   On Linux/macOS: `export OPENAQ_API_KEY="your_actual_api_key"`
    *   On Windows: `set OPENAQ_API_KEY="your_actual_api_key"`
3.  Build and run the Docker containers: 
    *   `echo "AIRFLOW_UID=$(id -u)" >> .env` 
    *   `docker compose up airflow-init`
    *   `docker compose up -d`
4.  Access the Airflow UI: `http://localhost:8080`

## ETL Pipeline

The ETL pipeline performs the following steps:

1.  Data Acquisition: Downloads air quality and weather data using Python scripts.
2.  Data Cleaning: Cleans and preprocesses the data using pandas.
3.  Data Transformation: Transforms the data into a star schema with a fact table (`combined_data`) and dimension tables (`DimTime`, `DimWeather`).
4.  Data Loading: Loads the processed data into DuckDB.

## Additional Notes

*   Make sure you have Docker and Docker Compose installed on your system.
