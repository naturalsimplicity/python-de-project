FROM python:3.12

WORKDIR producer
RUN pip install confluent-kafka sqlalchemy psycopg2-binary numpy faker

COPY . .

CMD python ./main.py
