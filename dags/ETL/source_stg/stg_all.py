import os
import logging
from datetime import datetime, timezone
import psycopg2
import traceback
from db_connections.mssql_pgsql_connection import mssql_config_s29, mssql_config_s30, mssql_config_s38, postgres_config
from db_connections.spark_session import get_spark_session

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def insert_monitoring_data(conn, database, table, start_time, end_time, status, comment):
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
    try:
        with conn.cursor() as cursor:
            truncate_query = f"TRUNCATE TABLE {schema_name}.{table_name};"
            cursor.execute(truncate_query)
        conn.commit()
        logging.info(f"Table {schema_name}.{table_name} truncated successfully.")
    except Exception as e:
        logging.error(f"Error truncating table {schema_name}.{table_name}: {e}")

def transfer_table(sql_file_path, schema_name, table_name, spark, postgres_db, conn_pg, mssql_config):
    start_time = datetime.now(timezone.utc)
    try:
        # Truncate the target table before inserting new data
        truncate_table(conn_pg, schema_name, table_name)

        # Load SQL query from file
        with open(sql_file_path, 'r') as file:
            sql_query = file.read().strip()

        # Determine MSSQL connection URL
        if 'url' in mssql_config:
            jdbc_url = mssql_config['url']
        else:
            jdbc_url = f"jdbc:sqlserver://{mssql_config['server']};databaseName={mssql_config['database']}"

        # Read data from MSSQL using Spark
        df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"({sql_query}) as tmp") \
            .option("user", mssql_config['user']) \
            .option("password", mssql_config['password']) \
            .option("driver", mssql_config['driver']) \
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
        raise

def process_schema(schema, mssql_config, schema_name):
    current_dir = os.path.dirname(os.path.abspath(__file__))  # Get current script directory
    sql_folder_path = os.path.join(current_dir, "..", "..", "sql_all", "stg_sql", schema)  # Construct SQL folder path

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

                transfer_table(sql_file_path, schema_name, table_name, spark, "dwh_schweigerbier", conn_pg, mssql_config)

    except Exception as e:
        logging.error(f"Fatal error in {schema}: {e}")
        traceback.print_exc()
        raise

    finally:
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

def main():
    process_schema("s29", mssql_config_s29, 's29_stg')
    process_schema("s30", mssql_config_s30, 's30_stg')
    process_schema("s38", mssql_config_s38, 's38_stg')

if __name__ == "__main__":
    main()
