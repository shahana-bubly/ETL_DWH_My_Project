# dags/db_connections/mssql_pgsql_connection.py

mssql_config_s29 = {
    "url": "jdbc:sqlserver://ip_address:1433;databaseName=DB_name",
    "user": "YY",
    "password": "XX",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

mssql_config = {
    "url": "jdbc:sqlserver://ip_address:1433;databaseName=DB_name",
    "user": "YY",
    "password": "XX",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

mssql_config_sXX = {
    "server": "ip_address\\YX",
    "database": "DB_name2",
    "user": "XX",
    "password": "YY",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

mssql_config_sYY = {
    "url": "jdbc:sqlserver://ip_address:1433;databaseName=DB_name3",
    "user": "XX",
    "password": "YY",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

postgres_config = {
    "host": "pg_ip_address",
    "port": "5432",
    "database": "dwh",
    "user": "XX",
    "password": "YY",
    "jdbc_url": "jdbc:postgresql://pg_ip_address:5432/dwh",
    "driver": "org.postgresql.Driver"
}
