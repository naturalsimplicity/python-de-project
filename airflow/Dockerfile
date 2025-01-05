# Use the official Airflow image as the base
FROM apache/airflow:2.7.2

# Set the AIRFLOW_HOME environment variable
ENV AIRFLOW_HOME=/usr/local/airflow

# Additional setup for pyspark
ENV JAVA_HOME=/opt/java/openjdk
COPY --from=eclipse-temurin:17-jre $JAVA_HOME $JAVA_HOME
ENV PATH="${JAVA_HOME}/bin:${PATH}"
RUN pip install pyspark==3.5.3

# Switch to the root user
USER root

# Create the AIRFLOW_HOME directory and change its ownership to the airflow user
RUN mkdir -p ${AIRFLOW_HOME} && chown -R airflow: ${AIRFLOW_HOME}

# Switch back to the airflow user
USER airflow

# Initialize the Airflow database
RUN airflow db init
