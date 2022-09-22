import requests
import math
import json
from datetime import datetime, timedelta

from airflow import DAG 
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

DEFAULT_ARGS = {
    'owner': 'Tartu',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

API_URL = 'http://api.open-notify.org/iss-now.json'

DATA_FOLDER = '/tmp/data'

fetch_iss_dag = DAG(
    dag_id='fetch_iss', # name of dag
    schedule_interval='* * * * *', # execute every minute
    start_date=datetime(2022,9,14,9,15,0),
    catchup=False, # in case execution has been paused, should it execute everything in between
    template_searchpath=DATA_FOLDER, # the PostgresOperator will look for files in this folder
    default_args=DEFAULT_ARGS, # args assigned to all operators
)

# Task 1 - fetch the current ISS location and save it as a file.
# Task 2 - find the closest country, save it to a file
# Task 3 - save the closest country information to the database
# Task 4 - output the continent

def get_current_loc(url, output_folder):
    r = requests.get(url)
    r_json = r.json()
    unix_ts = r_json['timestamp']
    latitude = r_json['iss_position']['latitude']
    longitude = r_json['iss_position']['longitude']
    with open(f'{output_folder}/iss.csv', 'w') as csv_file:
        csv_file.write(f'{unix_ts}, {latitude}, {longitude}')

first_task = PythonOperator(
    task_id='get_iss_loc',
    dag=fetch_iss_dag,
    trigger_rule='none_failed',
    python_callable=get_current_loc,
    op_kwargs={
        'output_folder': DATA_FOLDER,
        'url': API_URL,
    },
)

def get_distance(startLat, startLon, endLat, endLon):    
        
    #radius = 6371 # km
    radius = 6371000 # m
    dlat = math.radians(endLat-startLat)
    dlon = math.radians(endLon-startLon)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(startLat)) \
        * math.cos(math.radians(endLat)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c / 1000 # distance in kilometres

    return d

def find_closest_country(output_folder):
    with open(f'{output_folder}/countries.csv', 'r') as f:
        countries = f.readlines()
    with open(f'{output_folder}/iss.csv', 'r') as f:
        iss = f.readlines()[0].split(', ')

    iss_ts, iss_lat, iss_lon = iss
    closest_distance = {
        'distance': 9999
    }
    for c in countries[1:]:
        c_list = c.strip().split(',')
        c_code, c_lat, c_lon, c_name, c_continent = c_list
        c_distance = get_distance(float(iss_lat), float(iss_lon), float(c_lat), float(c_lon))
        if c_distance < closest_distance['distance']:
            closest_distance = {
                'distance': c_distance,
                'country': c_name,
                'continent': c_continent,
                'iss_ts': iss_ts,
                'iss_latitude': iss_lat,
                'iss_longitude': iss_lon,
            }
    
    with open(f'{output_folder}/closest_distance.json', 'w') as f:
        json.dump(closest_distance, f)

 
second_task = PythonOperator(
    task_id='find_iss_distance',
    dag=fetch_iss_dag,
    trigger_rule='none_failed',
    python_callable=find_closest_country,
    op_kwargs={
        'output_folder': DATA_FOLDER,
    },
)   

first_task >> second_task


def prepare_insert(output_folder):
    with open(f'{output_folder}/closest_distance.json', 'r') as f:
        closest_distance = json.load(f)
    with open(f'{output_folder}/insert.sql', 'w') as f:
        f.write(
            'CREATE TABLE IF NOT EXISTS iss_location (\n'
            'country VARCHAR(255),\n'
            'continent VARCHAR(255),\n'
            'distance DECIMAL(6,1),\n'
            'issLatitude DECIMAL(9,6),\n'
            'issLongitude DECIMAL(9,6),\n'
            'eventTimestamp TIMESTAMP,\n'
            'processedTimestamp TIMESTAMP);\n'
            'INSERT INTO iss_location\n'
            '(country, continent, distance, issLatitude, issLongitude, eventTimestamp, processedTimestamp)\n'
            f'SELECT \'{closest_distance["country"]}\' as country,\n'
            f'\'{closest_distance["continent"]}\' as continent,\n'
            f'{closest_distance["distance"]} as distance,\n'
            f'{closest_distance["iss_latitude"]} as issLatitude,\n'
            f'{closest_distance["iss_longitude"]} as issLongitude,\n'
            f'TO_TIMESTAMP({closest_distance["iss_ts"]}) as eventTimestamp,\n'
            f'CURRENT_TIMESTAMP as processedTimestamp;'
        )

    


third_task = PythonOperator(
    task_id='prepare_insert_stmt',
    dag=fetch_iss_dag,
    trigger_rule='none_failed',
    python_callable=prepare_insert,
    op_kwargs={
        'output_folder': DATA_FOLDER,
    },
)

second_task >> third_task

fourth_task = PostgresOperator(
    task_id='insert_to_db',
    dag=fetch_iss_dag,
    postgres_conn_id='airflow_pg',
    sql='insert.sql',
    trigger_rule='none_failed',
    autocommit=True,
)

third_task >> fourth_task

with open(f'{DATA_FOLDER}/countries.csv', 'r') as f:
    continents = f.readlines()
    continents = {x.split(',')[4].strip() for x in continents[1:]}

def find_closest_continent(output_folder):
    with open(f'{output_folder}/closest_distance.json', 'r') as f:
        closest_distance = json.load(f)
    return f'continent_{closest_distance["continent"]}'    

fifth_task = BranchPythonOperator(
    task_id='find_closest_continent',
    dag=fetch_iss_dag,
    python_callable=find_closest_continent,
    trigger_rule='none_failed',
    op_kwargs={
        'output_folder': DATA_FOLDER,
    },
)

fourth_task >> fifth_task

seventh_task = DummyOperator(
    task_id='joined_task',
    dag=fetch_iss_dag,
    trigger_rule='none_failed',
)

for c in continents:
    task = BashOperator(
        task_id=f'continent_{c}',
        dag=fetch_iss_dag,
        bash_command=f'echo ISS is closest to {c}',
    )

    fifth_task >> task >> seventh_task


eight_task_a = DummyOperator(
    task_id='parallel_task_a',
    dag=fetch_iss_dag,
    trigger_rule='none_failed',
)

eight_task_b = DummyOperator(
    task_id='parallel_task_b',
    dag=fetch_iss_dag,
    trigger_rule='none_failed',
)

ninth_task = DummyOperator(
    task_id='parallel_join',
    dag=fetch_iss_dag,
    trigger_rule='none_failed',
)

seventh_task >> [eight_task_a, eight_task_b] >> ninth_task