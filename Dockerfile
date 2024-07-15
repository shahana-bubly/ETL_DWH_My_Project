# Use Apache Airflow base image
FROM apache/airflow:2.9.0

# Switch to root user for installation
USER root

# Install necessary packages including the ODBC Driver for SQL Server
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    unixodbc-dev \
    gcc \
    procps \
    openjdk-17-jre-headless \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables for Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Copy the JDBC driver to the image
RUN mkdir -p /opt/airflow/jars
ADD https://github.com/microsoft/mssql-jdbc/releases/download/v9.4.0/mssql-jdbc-9.4.0.jre8.jar /opt/airflow/jars/
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.20/postgresql-42.2.20.jar /opt/airflow/jars/

# Set permissions for the JDBC drivers
RUN chmod 644 /opt/airflow/jars/mssql-jdbc-9.4.0.jre8.jar \
    && chmod 644 /opt/airflow/jars/postgresql-42.2.20.jar

# Switch to the airflow user to install Python dependencies
USER airflow

# Copy requirements.txt and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt && \
    pip install pyspark==3.3.2 findspark

# Clean up unnecessary files
USER root
RUN rm /requirements.txt

# Switch back to the airflow user
USER airflow
