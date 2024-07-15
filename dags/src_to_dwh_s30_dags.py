from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Importing the ETL functions
from ETL.stg.stg_s30 import stg_s30

# Importing the execute_all_tf_sql_scripts function
from ETL.stg_to_dwh.execute_tf_sql_s30_scripts import execute_s30_tf_sql_scripts

# Importing the execute_all_dwh_sql_scripts function
from ETL.stg_to_dwh.execute_dwh_sql_s30_scripts import execute_s30_dwh_sql_scripts

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,  # Send an email on task failure
    'email_on_retry': False,  # Do not send an email on retry
    'email': ['shahanayeasminbubly@gmail.com'],  # Email address to send notifications
    'retries': 0,
    'start_date': datetime(2024, 7, 10),  # Ensure this is today or later
    'email_on_success': True,  # Send an email on task success
}

# Define the DAG
with DAG(
    'src_dwh_s30_dags',
    default_args=default_args,
    description='A DAG to run ETL tasks and execute all SQL scripts',
    schedule_interval='30 0 * * *',  # Runs daily at 00:30
    catchup=False,  # Do not run for previous dates
) as dag:
    
    """""
    execute_all_stg_s30_task = PythonOperator(
        task_id='execute_all_stg_s30',
        python_callable=stg_s30,
    )
    """""
    # Define the execute_all_tf_sql_scripts task
    execute_all_tf_sql_scripts_task = PythonOperator(
        task_id='execute_all_tf_sql_s30',
        python_callable=execute_s30_tf_sql_scripts,
    )
 
    # Define the execute_all_dwh_sql_scripts task
    execute_all_dwh_sql_scripts_task = PythonOperator(
        task_id='execute_all_dwh_sql_s30',
        python_callable=execute_s30_dwh_sql_scripts,
    )

    # Set the task dependencies
    #execute_all_stg_s30_task >> 
    execute_all_tf_sql_scripts_task >> execute_all_dwh_sql_scripts_task
