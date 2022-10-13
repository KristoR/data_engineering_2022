import requests 
import os
from datetime import datetime, timedelta

from airflow import DAG 
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.sql import BranchSQLOperator


## -- ##
# The following should be run on Postgres beforehand.
# We create a new database, and in the new database, create new tables.
# Note: you may need to open a new connection to the newly created database before creating the tables.
#
# CREATE DATABASE assignment;
# 
# CREATE TABLE crm (
# 	id serial primary key,
# 	gender text not null,
# 	name_title text not null,
# 	name_first text not null,
# 	name_last text not null,
# 	dob_date timestamp not null,
# 	dob_age int4 not null,
# 	nat text not null,
# 	created_at timestamp not null
# );
#
# create table age_trend(
# 	id serial primary key,
# 	current_avg_age int4 not null,
# 	previous_avg_age int4 not null,
# 	created_at timestamp not null
# );


## -- ##
# The following should be done in Airflow UI beforehand.
# Admin->Connections->Add a new record
#
# Connection Id: file_sensor_connection
# Connection Type: File (path)
# Extra: {"path": "/tmp/data/"}
# 
# Add a new record
#
# Connection Id: assignment_pg
# Connection Type: Postgres
# Host: postgres
# Schema: assignment
# Login: airflow
# Password: airflow
# Port: 5432


DEFAULT_ARGS = {
    'owner': 'Solution',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

API_URL = 'https://randomuser.me/api'
API_PARAMS = {
    'results': 5,
    'format': 'csv',
    'inc': 'gender,name,nat,dob'
}


DATA_FOLDER = '/tmp/data'
CRM_FILE_NAME = 'crm.csv'
SQL_FILE_NAME = 'insert_crm.sql'

# CRM generator
# fetch 5 users from API and save to csv file

crm_generator_dag = DAG(
    dag_id='crm_generator',
    schedule_interval='* * * * *',
    start_date=datetime(2022,9,1,0,0,0),
    catchup=False,
    tags=['assignment'],
    template_searchpath=DATA_FOLDER,
    default_args=DEFAULT_ARGS
)

# Task: fetch data and save as csv

def crm_generator_method(url, params, folder, file):
    r = requests.get(url=url, params=params)
    with open(os.path.join(folder, file), 'wb') as f:
        f.write(r.content)

crm_task = PythonOperator(
    task_id='fetch_crm_data',
    dag=crm_generator_dag,
    trigger_rule='none_failed',
    python_callable=crm_generator_method,
    op_kwargs={
        'url': API_URL,
        'params': API_PARAMS,
        'folder': DATA_FOLDER,
        'file': CRM_FILE_NAME,
    }
)


# User age trend

user_age_trend_dag = DAG(
    dag_id='user_age_trend',
    schedule_interval='* * * * *',
    start_date=datetime(2022,9,1,0,0,0),
    catchup=False,
    tags=['assignment'],
    template_searchpath=DATA_FOLDER,
    default_args=DEFAULT_ARGS
)


waiting_for_crm = FileSensor(
    task_id='waiting_for_crm',
    dag=user_age_trend_dag,
    filepath=CRM_FILE_NAME,
    fs_conn_id='file_sensor_connection',
    poke_interval=5,
    timeout=100,
    exponential_backoff=True,
)


def prepare_insert(folder, input_file, output_file):
    with open(os.path.join(folder,input_file), 'rb') as input_f:
        crm_data = input_f.readlines()
    
    sql_statement = """INSERT INTO crm
    (gender, name_title, name_first, name_last, dob_date, dob_age, nat, created_at)
    """

    for idx,row in enumerate(crm_data):
        if idx==0:
            continue
        if idx>1:
            sql_statement += 'UNION ALL\n'
        split_data = row.decode('utf-8').strip().split(',')
        sql_statement += f"""
        SELECT '{split_data[0]}' as gender
        , '{split_data[1]}' as name_title
        , '{split_data[2]}' as name_first
        , '{split_data[3]}' as name_last
        , CAST('{split_data[4]}' as TIMESTAMP) as dob_date
        , {split_data[5]} as dob_age
        , '{split_data[6]}' as nat
        , CURRENT_TIMESTAMP
        """
    
    with open(os.path.join(folder,output_file), 'w') as output_f:
        output_f.writelines(sql_statement)

    os.remove(os.path.join(folder,input_file))


prep_sql_task = PythonOperator(
    task_id='prepare_insert_statement',
    dag=user_age_trend_dag,
    trigger_rule='none_failed',
    python_callable=prepare_insert,
    op_kwargs={
        'folder': DATA_FOLDER,
        'input_file': CRM_FILE_NAME,
        'output_file': SQL_FILE_NAME,
    },
) 

waiting_for_crm >> prep_sql_task



sql_insert_task = PostgresOperator(
    task_id='insert_to_db',
    dag=user_age_trend_dag,
    postgres_conn_id='assignment_pg',
    sql=SQL_FILE_NAME,
    trigger_rule='none_failed',
    autocommit=True,
)

prep_sql_task >> sql_insert_task

sql_calculate_age_task = PostgresOperator(
    task_id='update_avg_age',
    dag=user_age_trend_dag,
    postgres_conn_id='assignment_pg',
    sql="""
        INSERT INTO age_trend (current_avg_age, previous_avg_age, created_at)
        SELECT ROUND(
                    CAST(
                        AVG(
                            DATE_PART('year', AGE(CURRENT_TIMESTAMP,dob_date))
                            ) as numeric
                        )
                    ,1) AS current_avg_age
                , COALESCE((SELECT current_avg_age
                    FROM age_trend
                    ORDER BY created_at DESC
                    LIMIT 1),0) AS previous_avg_age
                , CURRENT_TIMESTAMP AS created_at
                from crm
        """,
    trigger_rule='none_failed',
    autocommit=True,
)

sql_insert_task >> sql_calculate_age_task

sql_find_trend_task = BranchSQLOperator(
    task_id='find_trend',
    dag=user_age_trend_dag,
    conn_id='assignment_pg',
    sql="""
        SELECT current_avg_age>previous_avg_age 
        FROM age_trend 
        ORDER BY created_at DESC
        LIMIT 1
        """,
    follow_task_ids_if_true='age_increasing',
    follow_task_ids_if_false='age_decreasing',
)

sql_calculate_age_task >> sql_find_trend_task


age_trend_increasing_alert = BashOperator(
    task_id='age_increasing',
    dag=user_age_trend_dag,
    bash_command='echo User age is increasing!'
)

age_trend_decreasing_alert = BashOperator(
    task_id='age_decreasing',
    dag=user_age_trend_dag,
    bash_command='echo User age is decreasing!'
)

sql_find_trend_task >> age_trend_increasing_alert
sql_find_trend_task >> age_trend_decreasing_alert