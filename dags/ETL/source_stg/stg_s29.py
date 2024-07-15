import os
from datetime import datetime, timezone
import psycopg2
import traceback
from db_connections.mssql_pgsql_connection import mssql_config_s29, postgres_config
from db_connections.spark_session import get_spark_session

def insert_monitoring_data(conn, database, table, start_time, end_time, status, comment):
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO s29.monitoring (database_name, table_name, start_time, end_time, status, comment) VALUES (%s, %s, %s, %s, %s, %s)",
                (database, table, start_time, end_time, status, comment)
            )
        conn.commit()
    except Exception as e:
        print("Error inserting monitoring data:", e)

def truncate_table(conn, schema_name, table_name):
    try:
        with conn.cursor() as cursor:
            truncate_query = f"TRUNCATE TABLE {schema_name}.{table_name};"
            cursor.execute(truncate_query)
        conn.commit()
        print(f"Table {schema_name}.{table_name} truncated successfully.")
    except Exception as e:
        print(f"Error truncating table {schema_name}.{table_name}:", e)

def transfer_table(sql_file_path, schema_name, table_name, spark, postgres_db, conn_pg):
    try:
        # Load SQL query from file
        with open(sql_file_path, 'r') as file:
            sql_query = file.read().strip()

        # Debug: Print SQL query to ensure it's correct
        #print(f"Executing SQL query from {sql_file_path}:\n{sql_query}")

        # Truncate the table before loading new data
        truncate_table(conn_pg, schema_name, table_name)

        # Read data from MSSQL using Spark
        df = spark.read.format("jdbc") \
            .option("url", mssql_config_s29['url']) \
            .option("dbtable", f"({sql_query}) as tmp") \
            .option("user", mssql_config_s29['user']) \
            .option("password", mssql_config_s29['password']) \
            .option("driver", mssql_config_s29['driver']) \
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
        insert_monitoring_data(conn_pg, postgres_db, f"{schema_name}.{table_name}", end_time, end_time, 1,
                               "Table operation successful.")

    except Exception as e:
        end_time = datetime.now(timezone.utc)
        insert_monitoring_data(conn_pg, postgres_db, f"{schema_name}.{table_name}", end_time, end_time, 0,
                               f"Error: {e}\n{traceback.format_exc()}")
        print("Error transferring table:", e)
        raise  # Re-raise the exception to ensure it is caught by Airflow

def stg_s29():
    current_dir = os.path.dirname(os.path.abspath(__file__))  # Get current script directory
    sql_folder_path = os.path.join(current_dir, "..", "..", "sql", "stg_sql", "s29")  # Construct SQL folder path

    conn_pg = psycopg2.connect(
        host=postgres_config['host'],
        port=postgres_config['port'],
        database=postgres_config['database'],
        user=postgres_config['user'],
        password=postgres_config['password']
    )

    try:
        spark = get_spark_session("MSSQL to PostgreSQL Transfer")

        for sql_file in os.listdir(sql_folder_path):
            if sql_file.endswith(".sql"):
                sql_file_path = os.path.join(sql_folder_path, sql_file)
                table_name = os.path.splitext(sql_file)[0]
                schema_name = 's29_stg'

                transfer_table(sql_file_path, schema_name, table_name, spark, "dwh_schweigerbier", conn_pg)

    finally:
        conn_pg.close()
        spark.stop()