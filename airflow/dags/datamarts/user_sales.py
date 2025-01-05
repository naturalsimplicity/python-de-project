from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.mysql:mysql-connector-j:9.1.0 pyspark-shell'

def update_user_sales():
    session = SparkSession \
        .builder \
        .appName('SparkDatamarts') \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    users = session.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/mysql_db") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "users") \
        .option("user", "mysql_user") \
        .option("password", "mysql_password") \
        .load() \
        .alias("users")

    orders = session.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/mysql_db") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "orders") \
        .option("user", "mysql_user") \
        .option("password", "mysql_password") \
        .load() \
        .alias("orders")

    user_sales = orders \
        .join(users, orders.user_id == users.user_id, "inner") \
        .withColumn("order_month", f.date_trunc("MM", orders.order_date)) \
        .withColumn("delivery_period", f.date_diff("delivery_date", "order_date")) \
        .select(["order_month", "order_id", orders.user_id, "loyalty_status", "total_amount",
                 "status", "delivery_period"]) \
        .groupBy(['order_month', 'loyalty_status']) \
        .agg(f.count("order_id").alias("orders_cnt"),
             f.count(f.when(f.col("status") == "Paid", f.col("order_id"))).alias("paid_orders"),
             f.count(f.when(f.col("status") == "Completed", f.col("order_id"))).alias("completed_orders"),
             f.count(f.when(f.col("status") == "Pending", f.col("order_id"))).alias("pending_orders"),
             f.count(f.when(f.col("status") == "Aborted", f.col("order_id"))).alias("aborted_orders"),
             f.countDistinct(orders.user_id).alias("unique_users"),
             f.sum("total_amount").alias("total_order_amount"),
             f.avg("total_amount").alias("avg_order_amount"),
             f.sum(f.when(f.col("status") != "Aborted", f.col("total_amount"))).alias("total_revenue"),
             f.avg("delivery_period").alias("avg_delivery_period")) \
        .orderBy(["order_month", "loyalty_status"])

    user_sales.show(10)

    user_sales.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url", "jdbc:mysql://mysql:3306/mysql_db") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "user_sales") \
        .option("user", "mysql_user") \
        .option("password", "mysql_password") \
        .save()
