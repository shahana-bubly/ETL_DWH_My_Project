# Building a Central Data Warehouse for a Beer Producing Company
## Project Overview
#### This project involved building a centralized data warehouse for a beer-producing company. The goal was to migrate data from multiple MSSQL databases into a PostgreSQL data warehouse for safer and more efficient reporting. The project also aimed to automate the entire ETL (Extract, Transform, Load) process, ensuring that data was kept up-to-date without impacting production systems.

## Technologies Used 

Database: PostgreSQL
Scripting: Python
ETL Automation: Apache Airflow
Data Integration: PySpark
Containerization: Docker
Development Environment: Visual Studio Code

## Project Structure

![ETL for DM](https://raw.githubusercontent.com/shahana-bubly/ETL_DWH_My_Project/main/ETL%20for%20DM.png)

## The project follows a typical ETL pipeline structure:

1. Staging Layer:
Data is extracted from multiple MSSQL databases and loaded into staging tables in PostgreSQL.
All columns are defined as VARCHAR to avoid type conversion issues during the initial load.
Data loading is optimized using bulk loading and incremental loads.
2. Transformation Layer:
Data in the staging tables is transformed by converting data types (e.g., VARCHAR to INT, DECIMAL, BOOLEAN).
Data is cleaned (duplicates removed, errors handled) and prepared for final storage.
3. Data Warehouse Layer:
Transformed data is loaded into the final PostgreSQL tables, structured according to the client’s reporting needs.
4. Automation with Airflow:
Airflow workflows automate the ETL process, ensuring that data is always up-to-date.
Scheduled jobs handle extraction, transformation, and loading tasks with monitoring for errors.

