FROM python:3.10

ENV JAVA_HOME=/opt/java/openjdk
COPY --from=eclipse-temurin:17-jre $JAVA_HOME $JAVA_HOME
ENV PATH="${JAVA_HOME}/bin:${PATH}"
RUN pip install pyspark sqlalchemy mysql-connector-python

WORKDIR consumer
COPY . .

CMD python ./main.py
