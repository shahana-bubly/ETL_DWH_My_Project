import os
from datetime import datetime, timezone
import psycopg2
import traceback
from db_connections.mssql_pgsql_connection import mssql_config_s38, postgres_config
from db_connections.spark_session import get_spark_session
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def insert_monitoring_data(conn, database, table, start_time, end_time, status, comment):
    """
    Insert monitoring data into the PostgreSQL database.
    Args:
        conn (psycopg2.connection): Connection to the PostgreSQL database.
        database (str): Name of the database.
        table (str): Name of the table.
        start_time (datetime): Start time of the operation.
        end_time (datetime): End time of the operation.
        status (int): Status code indicating success (1) or failure (0).
        comment (str): Additional comment or error message.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO s29.monitoring (database_name, table_name, start_time, end_time, status, comment)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (database, table, start_time, end_time, status, comment)
            )
        conn.commit()
        logging.info(f"Inserted monitoring data for {table} with status {status}.")
    except Exception as e:
        logging.error(f"Error inserting monitoring data for {table}: {e}")

def truncate_table(conn, schema_name, table_name):
    """
    Truncate a table in the PostgreSQL database.
    Args:
        conn (psycopg2.connection): Connection to the PostgreSQL database.
        schema_name (str): Name of the schema.
        table_name (str): Name of the table.
    """
    try:
        with conn.cursor() as cursor:
            truncate_query = f"TRUNCATE TABLE {schema_name}.{table_name};"
            cursor.execute(truncate_query)
        conn.commit()
        logging.info(f"Table {schema_name}.{table_name} truncated successfully.")
    except Exception as e:
        logging.error(f"Error truncating table {schema_name}.{table_name}: {e}")

def transfer_table(sql_file_path, schema_name, table_name, spark, postgres_db, conn_pg):
    """
    Transfer data from MSSQL to PostgreSQL and insert monitoring data.
    Args:
        sql_file_path (str): Path to the SQL file.
        schema_name (str): Name of the schema in PostgreSQL.
        table_name (str): Name of the table in PostgreSQL.
        spark (SparkSession): Spark session for data processing.
        postgres_db (str): Name of the PostgreSQL database.
        conn_pg (psycopg2.connection): Connection to the PostgreSQL database.
    """
    start_time = datetime.now(timezone.utc)
    try:
        # Load SQL query from file
        with open(sql_file_path, 'r') as file:
            sql_query = file.read().strip()

        # Debug: Print SQL query to ensure it's correct
        #logging.info(f"Executing SQL query from {sql_file_path}:\n{sql_query}")

        # Truncate the table before loading new data
        truncate_table(conn_pg, schema_name, table_name)

        # Read data from MSSQL using Spark
        df = spark.read.format("jdbc") \
            .option("url", f"jdbc:sqlserver://{mssql_config_s38['server']};databaseName={mssql_config_s38['database']}") \
            .option("dbtable", f"({sql_query}) as tmp") \
            .option("user", mssql_config_s38['user']) \
            .option("password", mssql_config_s38['password']) \
            .option("driver", mssql_config_s38['driver']) \
            .load()
         # Print debug information
        total_records = df.count()
        print(f"Table name: {table_name}, Total records: {total_records}")

        # Write data to PostgreSQL using append mode
        df.write.format("jdbc") \
            .option("url", postgres_config['jdbc_url']) \
            .option("dbtable", f"{schema_name}.{table_name}") \
            .option("user", postgres_config['user']) \
            .option("password", postgres_config['password']) \
            .option("driver", postgres_config['driver']) \
            .option("batchsize", "10000") \
            .mode("append") \
            .save()

        # Insert monitoring data
        end_time = datetime.now(timezone.utc)
        insert_monitoring_data(conn_pg, postgres_db, f"{schema_name}.{table_name}", start_time, end_time, 1, "Table operation successful.")

    except Exception as e:
        end_time = datetime.now(timezone.utc)
        insert_monitoring_data(conn_pg, postgres_db, f"{schema_name}.{table_name}", start_time, end_time, 0, f"Error: {e}\n{traceback.format_exc()}")
        logging.error(f"Error transferring table {table_name}: {e}")
        raise  # Re-raise the exception to ensure it is caught by the calling process

def stg_s38():
    """
    Main function to set up the Spark session and transfer data for the 's38' schema.
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))  # Get current script directory
    sql_folder_path = os.path.join(current_dir, "..", "..", "sql", "stg_sql", "s38")  # Construct SQL folder path

    conn_pg = None
    spark = None

    try:
        conn_pg = psycopg2.connect(
            host=postgres_config['host'],
            port=postgres_config['port'],
            database=postgres_config['database'],
            user=postgres_config['user'],
            password=postgres_config['password']
        )

        spark = get_spark_session("MSSQL to PostgreSQL Transfer")

        for sql_file in os.listdir(sql_folder_path):
            if sql_file.endswith(".sql"):
                sql_file_path = os.path.join(sql_folder_path, sql_file)
                table_name = os.path.splitext(sql_file)[0]
                schema_name = 's38_stg'

                # No try-except block here to ensure process stops on failure
                transfer_table(sql_file_path, schema_name, table_name, spark, "dwh_schweigerbier", conn_pg)

    except Exception as e:
        # Catch any errors during connection, Spark setup, or table transfer
        logging.error(f"Error in stg_s38: {e}")
        raise  # Re-raise the exception to ensure it is caught by the calling process

    finally:
        # Ensure that resources are cleaned up
        if conn_pg:
            try:
                conn_pg.close()
                logging.info("PostgreSQL connection closed.")
            except Exception as e:
                logging.error(f"Error closing PostgreSQL connection: {e}")

        if spark:
            try:
                spark.stop()
                logging.info("Spark session stopped.")
            except Exception as e:
                logging.error(f"Error stopping Spark session: {e}")

if __name__ == "__main__":
    stg_s38()
