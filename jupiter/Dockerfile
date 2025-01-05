FROM quay.io/jupyter/base-notebook
ENV JAVA_HOME=/opt/java/openjdk
COPY --from=eclipse-temurin:17-jre $JAVA_HOME $JAVA_HOME
ENV PATH="${JAVA_HOME}/bin:${PATH}"
RUN pip install pyspark==3.5.3
