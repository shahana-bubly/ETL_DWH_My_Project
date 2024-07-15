from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

# Importing the ETL functions
from ETL.stg.stg_s29 import stg_s29

# Importing the execute_s29_tf_sql_scripts function
from ETL.stg_to_dwh.execute_tf_sql_s29_scripts import execute_s29_tf_sql_scripts

# Importing the execute_s29_dwh_sql_scripts function
from ETL.stg_to_dwh.execute_dwh_sql_s29_scripts import execute_s29_dwh_sql_scripts

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'start_date': datetime(2024, 7, 10),  # Ensure this is today or later
    'email_on_success': True,
}

# Define the DAG
with DAG(
    'src_dwh_s29_dags',
    default_args=default_args,
    description='A DAG to run ETL tasks and execute all SQL scripts for S29',
    schedule_interval='30 0 * * *',  # Runs daily at 00:30
    catchup=False,  # Ensure no backfilling
) as dag:
    
    # Define the ETL tasks
    
    execute_all_stg_s29_task = PythonOperator(
        task_id='execute_all_stg_s29',
        python_callable=stg_s29,
    )
    
    
    # Define the execute_s29_tf_sql_scripts task
    execute_all_tf_sql_s29_scripts_task = PythonOperator(
        task_id='execute_all_tf_sql_s29_scripts',
        python_callable=execute_s29_tf_sql_scripts,
    )
  

    # Define the execute_s29_dwh_sql_scripts task
    execute_all_dwh_sql_s29_scripts_task = PythonOperator(
        task_id='execute_all_dwh_sql_s29_scripts',
        python_callable=execute_s29_dwh_sql_scripts,
    )

    """
    send_success_email = EmailOperator(
        task_id='send_success_email',
        to='shahanayeasminbubly@gmail.com',
        subject='DAG src_dwh_s29_dags succeeded',
        html_content='<p>Your DAG src_dwh_s29_dags ran successfully!</p>',
        trigger_rule='all_success',  # Trigger only on success
    )

    send_failure_email = EmailOperator(
        task_id='send_failure_email',
        to='shahanayeasminbubly@gmail.com',
        subject='DAG src_dwh_s29_dags failed',
        html_content='<p>Your DAG src_dwh_s29_dags failed!</p>',
        trigger_rule='one_failed',  # Trigger if any task fails
    )
    """
    
    # Set the task dependencies
    execute_all_stg_s29_task >> execute_all_tf_sql_s29_scripts_task >> execute_all_dwh_sql_s29_scripts_task

    # Add email tasks
    # execute_all_dwh_sql_s29_scripts_task >> send_success_email
    # execute_all_dwh_sql_s29_scripts_task >> send_failure_email
