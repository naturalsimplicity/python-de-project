from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from datamarts.user_sales import update_user_sales
from datamarts.product_sales import update_product_sales


default_args = {
    'owner': 'matvey',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id="wf_sales_datamarts_v1.0",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, 12),
    schedule_interval='15 * * * *',  # every 15 minutes
    catchup=False
)
def taskflow():

    user_sales = PythonOperator(
        task_id="user_sales",
        python_callable=update_user_sales
    )

    product_sales = PythonOperator(
        task_id="product_sales",
        python_callable=update_product_sales
    )

    user_sales >> product_sales


taskflow()
