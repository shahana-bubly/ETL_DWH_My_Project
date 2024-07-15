# dags/src_to_dwh_dags.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Importing the ETL functions
from ETL.source_stg.stg_all import main

# Importing the execute_all_tf_sql_scripts function
from ETL.stg_to_dwh.execute_tf_sql_all_scripts import execute_all_tf_sql_scripts

# Importing the execute_all_dwh_sql_scripts function
from ETL.stg_to_dwh.execute_dwh_sql_all_scripts import execute_all_dwh_sql_scripts

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
   #  'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),  # Ensure this is in the past
    'catchup': False,
}

# Define the DAG
with DAG(
    'src_stg_tf_dwh_All_dags',
    default_args=default_args,
    description='A DAG to run ETL tasks and execute all SQL scripts',
    schedule_interval=None  # Change as per your requirement
) as dag:
    
    # Define the ETL tasks
    execute_all_stg_task = PythonOperator(
        task_id='execute_all_stg_task',
        python_callable=main,
    )

    # Define the execute_all_tf_sql_scripts task
    execute_all_tf_sql_scripts_task = PythonOperator(
        task_id='execute_all_tf_sql_scripts',
        python_callable=execute_all_tf_sql_scripts,
    )
   
    # Define the execute_all_dwh_sql_scripts task
    execute_all_dwh_sql_scripts_task = PythonOperator(
        task_id='execute_all_dwh_sql_scripts',
        python_callable=execute_all_dwh_sql_scripts,
    )

    # Set the task dependencies
    execute_all_stg_task >> execute_all_tf_sql_scripts_task >> execute_all_dwh_sql_scripts_task
