# dags/db_connections/spark_session.py
from pyspark.sql import SparkSession

def get_spark_session(app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", "/opt/airflow/jars/mssql-jdbc-9.4.0.jre8.jar,/opt/airflow/jars/postgresql-42.2.20.jar") \
        .config("spark.executor.memory", "32g") \
        .config("spark.executor.cores", "8") \
        .config("spark.executor.instances", "8") \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()
    return spark
