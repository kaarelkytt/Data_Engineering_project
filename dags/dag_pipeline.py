import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
import duckdb

from src.data_weather import download_weather_data, clean_weather_data
from src.data_air_quality import download_air_quality_data, clean_air_quality_data
from src.data_join import join_data
from src.database import create_database

# Define the DAG
dag = DAG(
    dag_id="pipeline",
    start_date=airflow.utils.dates.days_ago(0),
    schedule=None,
    catchup=False)

# Define the tasks
download_air_quality = PythonOperator(
    task_id="download_air_quality_data",
    dag=dag,
    python_callable=download_air_quality_data,
    trigger_rule='all_success',
)

download_weather = PythonOperator(
    task_id="download_weather_data",
    dag=dag,
    python_callable=download_weather_data,
    trigger_rule='all_success',
)

clean_air_quality = PythonOperator(
    task_id="clean_air_quality_data",
    dag=dag,
    python_callable=clean_air_quality_data,
    trigger_rule='all_success',
)

clean_weather = PythonOperator(
    task_id="clean_weather_data",
    dag=dag,
    python_callable=clean_weather_data,
    trigger_rule='all_success',
)

join_data = PythonOperator(
    task_id="join_data",
    dag=dag,
    python_callable=join_data,
    trigger_rule='all_success',
)

create_db = PythonOperator(
    task_id="create_database",
    dag=dag,
    python_callable=create_database,
    trigger_rule='all_success',
)

end = DummyOperator(
    task_id='end',
    dag=dag,
    trigger_rule='none_failed',
)


# Define the task dependencies
download_air_quality >> clean_air_quality
download_weather >> clean_weather
[clean_air_quality, clean_weather] >> join_data
join_data >> create_db
create_db >> end
