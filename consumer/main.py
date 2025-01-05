from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, IntegerType, StringType, TimestampType, DecimalType, ArrayType
import pyspark.sql.functions as f
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.mysql:mysql-connector-j:9.1.0 pyspark-shell'


# DOCKER_PORT=kafka:9093, HOST_PORT=localhost:9092
kafka_host = 'kafka:9093'

users_schema = StructType() \
    .add('user_id', IntegerType()) \
    .add('first_name', StringType()) \
    .add('last_name', StringType()) \
    .add('email', StringType()) \
    .add('phone_number', StringType()) \
    .add('registration_date', TimestampType()) \
    .add('loyalty_status', StringType())

products_schema = StructType() \
    .add('product_id', IntegerType()) \
    .add('name', StringType()) \
    .add('description', StringType()) \
    .add('category_id', IntegerType()) \
    .add('price', DecimalType(19, 2)) \
    .add('stock_quantity', IntegerType()) \
    .add('creation_date', TimestampType())

product_categories_schema = StructType() \
    .add('category_id', IntegerType()) \
    .add('name', StringType()) \
    .add('parent_category_id', IntegerType())

orders_schema = StructType() \
    .add('order_id', IntegerType()) \
    .add('user_id', IntegerType()) \
    .add('order_date', TimestampType()) \
    .add('total_amount', DecimalType(19, 2)) \
    .add('status', StringType()) \
    .add('delivery_date', TimestampType()) \
    .add('order_details', ArrayType(StringType()))

order_details_schema = StructType() \
    .add('order_detail_id', IntegerType()) \
    .add('order_id', IntegerType()) \
    .add('product_id', IntegerType()) \
    .add('quantity', IntegerType()) \
    .add('price_per_unit', DecimalType(19, 2)) \
    .add('total_amount', DecimalType(19, 2))

# DOCKER_PORT: jdbc:mysql://mysql:3306/mysql_db, HOST_PORT: jdbc:mysql://localhost:3307/mysql_db
def get_batch_processor(table_name):
    def _process_batch(df, epoch_id):
        df.show()
        df.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", "jdbc:mysql://mysql:3306/mysql_db") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", table_name) \
            .option("user", "mysql_user") \
            .option("password", "mysql_password") \
            .save()
    return _process_batch

def get_df_from_stream(
    session: SparkSession,
    topic: str,
    schema: StructType
) -> DataFrame:
    return session \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_host) \
        .option("subscribe", topic) \
        .option("failOnDataLoss", False) \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(
            f.col("timestamp").cast('string'),
            f.from_json(f.col('value').cast('string'), schema).alias('value')
        )

def main():
    session = SparkSession \
        .builder \
        .appName('SparkStreamingKafka') \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # .option("checkpointLocation", f"checkpoint_{topic}") \
    get_df_from_stream(session, "products", products_schema) \
        .select(
            f.col('value.product_id').alias('product_id'),
            f.col('value.name').alias('name'),
            f.col('value.description').alias('description'),
            f.col('value.category_id').alias('category_id'),
            f.col('value.price').alias('price'),
            f.col('value.stock_quantity').alias('stock_quantity'),
            f.col('value.creation_date').alias('creation_date')) \
        .writeStream \
        .foreachBatch(get_batch_processor("products")) \
        .start()

    get_df_from_stream(session, "product_categories", product_categories_schema) \
        .select(
            f.col('value.category_id').alias('category_id'),
            f.col('value.name').alias('name'),
            f.col('value.parent_category_id').alias('parent_category_id')) \
        .writeStream \
        .foreachBatch(get_batch_processor("product_categories")) \
        .start()

    get_df_from_stream(session, "users", users_schema) \
        .select(
            f.col('value.user_id').alias('user_id'),
            f.col('value.first_name').alias('first_name'),
            f.col('value.last_name').alias('last_name'),
            f.col('value.email').alias('email'),
            f.col('value.phone_number').alias('phone_number'),
            f.col('value.registration_date').alias('registration_date'),
            f.col('value.loyalty_status').alias('loyalty_status')) \
        .writeStream \
        .foreachBatch(get_batch_processor("users")) \
        .start()

    orders_stream = get_df_from_stream(session, "orders", orders_schema)

    orders_stream \
        .select(
            # f.col('timestamp'),
            f.col('value.order_id').alias('order_id'),
            f.col('value.user_id').alias('user_id'),
            f.col('value.order_date').alias('order_date'),
            f.col('value.total_amount').alias('total_amount'),
            f.col('value.status').alias('status'),
            f.col('value.delivery_date').alias('delivery_date')) \
        .writeStream \
        .foreachBatch(get_batch_processor("orders")) \
        .start()

    # DEBUG
    # orders_stream \
    #     .select(f.explode(f.col('value.order_details')).alias('value')) \
    #     .writeStream \
    #     .format("console") \
    #     .start()

    orders_stream \
        .select(f.explode(f.col('value.order_details')).alias('value')) \
        .select(f.from_json(f.col('value'), order_details_schema).alias('value')) \
        .select(
            f.col('value.order_detail_id').alias('order_detail_id'),
            f.col('value.order_id').alias('order_id'),
            f.col('value.product_id').alias('product_id'),
            f.col('value.quantity').alias('quantity'),
            f.col('value.price_per_unit').alias('price_per_unit'),
            f.col('value.total_amount').alias('total_amount')) \
        .writeStream \
        .foreachBatch(get_batch_processor("order_details")) \
        .start()

    session.streams.awaitAnyTermination()

if __name__ == '__main__':
    from sqlalchemy import create_engine
    from models import Base
    engine = create_engine("mysql+mysqlconnector://mysql_user:mysql_password@mysql:3306/mysql_db")
    Base.metadata.create_all(engine)

    main()

