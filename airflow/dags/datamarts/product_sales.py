from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.mysql:mysql-connector-j:9.1.0 pyspark-shell'

def update_product_sales():
    session = SparkSession \
        .builder \
        .appName('SparkDatamarts') \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    products = session.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/mysql_db") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "products") \
        .option("user", "mysql_user") \
        .option("password", "mysql_password") \
        .load() \
        .alias("products")

    product_categories = session.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/mysql_db") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "product_categories") \
        .option("user", "mysql_user") \
        .option("password", "mysql_password") \
        .load() \
        .alias("product_categories")

    orders = session.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/mysql_db") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "orders") \
        .option("user", "mysql_user") \
        .option("password", "mysql_password") \
        .load() \
        .alias("orders")

    order_details = session.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/mysql_db") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "order_details") \
        .option("user", "mysql_user") \
        .option("password", "mysql_password") \
        .load() \
        .alias("order_details")

    product_sales = orders \
        .join(order_details, orders.order_id == order_details.order_id, "inner") \
        .join(products, order_details.product_id == products.product_id, "inner") \
        .join(product_categories, products.category_id == product_categories.category_id, "inner") \
        .withColumn("order_month", f.date_trunc("MM", orders.order_date)) \
        .select([f.col("orders.order_id").alias("order_id"), "order_month", "status",
                 f.col("product_categories.name").alias("product_category"),
                 f.col("products.name").alias("product_name"),
                 f.col("order_details.total_amount").alias("total_amount"), "quantity"]) \
        .groupBy(["order_month", "product_category", "product_name"]) \
        .agg(f.countDistinct("order_id").alias("order_cnt"),
             f.sum("quantity").alias("ordered_quantity"),
             f.sum(f.when(f.col("status") == "Aborted", f.col("quantity"))).alias("aborted_quantity"),
             f.sum(f.when(f.col("status") == "Paid", f.col("quantity"))).alias("paid_quantity"),
             f.sum(f.when(f.col("status") == "Pending", f.col("quantity"))).alias("pending_quantity"),
             f.sum(f.when(f.col("status") == "Completed", f.col("quantity"))).alias("completed_quantity"),
             f.sum("total_amount").alias("total_amount"),
             f.sum(f.when(f.col("status") != "Aborted", f.col("total_amount"))).alias("total_revenue"),
             ) \
        .orderBy(["order_month", "product_category", "product_name"])

    product_sales.show(10)

    product_sales.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url", "jdbc:mysql://mysql:3306/mysql_db") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "product_sales") \
        .option("user", "mysql_user") \
        .option("password", "mysql_password") \
        .save()
