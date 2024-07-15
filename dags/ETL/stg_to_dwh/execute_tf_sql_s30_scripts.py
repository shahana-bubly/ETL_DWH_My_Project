import os
import psycopg2
import logging

# Define PostgreSQL connection details
postgres_config = {
    "host": "pg_ip_address",
    "port": "5432",
    "database": "dwh",
    "user": "XX",
    "password": "YY"
}

# List of directories containing SQL scripts
sql_directories = [
    'dags/sql/tf_sql/s30/'
]

# Define the function to execute all SQL scripts in a given directory
def execute_sql_scripts_in_directory(directory):
    # Configure logging
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    logger.debug(f"Starting the execution of all SQL scripts in directory {directory}")

    # Get list of SQL files in the directory
    sql_files = [f for f in os.listdir(directory) if f.endswith('.sql')]
    logger.debug(f"Found SQL files: {sql_files}")

    # Check if there are no SQL files
    if not sql_files:
        logger.warning(f"No SQL files found in {directory}")
        return

    # PostgreSQL connection
    try:
        connection = psycopg2.connect(
            host=postgres_config['host'],
            port=postgres_config['port'],
            dbname=postgres_config['database'],
            user=postgres_config['user'],
            password=postgres_config['password']
        )
        cursor = connection.cursor()
        logger.debug("Successfully connected to PostgreSQL")

        for sql_file in sql_files:
            sql_file_path = os.path.join(directory, sql_file)
            logger.debug(f"Reading SQL script from {sql_file_path}")

            with open(sql_file_path, 'r') as file:
                sql_script = file.read()

            logger.debug(f"Executing SQL script {sql_file}")
            try:
                cursor.execute(sql_script)
                logger.debug(f"SQL script {sql_file} executed successfully")
            except Exception as e:
                logger.error(f"Error executing SQL script {sql_file}: {e}")
                connection.rollback()  # Rollback the transaction on error
                raise  # Reraise the exception to mark the task as failed

            connection.commit()
            logger.info(f"SQL script {sql_file} committed")

    except Exception as e:
        logger.error(f"Error during SQL script execution: {e}")
        raise  # Reraise the exception to mark the task as failed

    finally:
        if connection:
            cursor.close()
            connection.close()
            logger.debug("PostgreSQL connection closed")

# Define the Python callable to iterate over directories and execute SQL scripts
def execute_s30_tf_sql_scripts():
    for directory in sql_directories:
        execute_sql_scripts_in_directory(directory)
