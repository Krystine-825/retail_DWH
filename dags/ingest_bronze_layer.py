from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text

# Chuỗi kết nối tới database retail_dwh (PostgreSQL)
# User/Pass: airflow/airflow. Host: postgres (tên service trong docker-compose)
DB_CONN = "postgresql+psycopg2://airflow:airflow@postgres:5432/retail_dwh"

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 10, 31),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def load_csv_to_bronze(file_name, table_name):
    """
    Hàm đọc file CSV bằng Pandas và ghi vào schema bronze của PostgreSQL
    """
    engine = create_engine(DB_CONN)
    
    # Dùng engine.begin() để tự động commit thay vì gọi conn.commit() thủ công
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))

    # Đường dẫn file bên trong container Airflow đã được mount từ máy host
    file_path = f"/opt/airflow/data/{file_name}"
    
    # Đọc dữ liệu
    print(f"Reading {file_name}...")
    df = pd.read_csv(file_path)
    
    # Load vào Database
    print(f"Loading {len(df)} rows to bronze.{table_name}...")
    df.to_sql(name=table_name, con=engine, schema='bronze', if_exists='replace', index=False)
    print("Load hoàn tất!")

with DAG(
    'ingest_to_bronze_layer',
    default_args=default_args,
    description='ETL DAG: Đọc dữ liệu từ CSV vào thư mục Bronze của Data Warehouse',
    schedule_interval='@daily',
    catchup=False,
    tags=['retail', 'bronze'],
) as dag:

    # Khởi tạo 3 Task tương ứng với 3 nguồn dữ liệu
    task_load_website = PythonOperator(
        task_id='load_website_orders',
        python_callable=load_csv_to_bronze,
        op_kwargs={'file_name': 'website_orders.csv', 'table_name': 'website_orders'}
    )

    task_load_shopee = PythonOperator(
        task_id='load_shopee_orders',
        python_callable=load_csv_to_bronze,
        op_kwargs={'file_name': 'shopee_orders.csv', 'table_name': 'shopee_orders'}
    )

    task_load_pos = PythonOperator(
        task_id='load_pos_transactions',
        python_callable=load_csv_to_bronze,
        op_kwargs={'file_name': 'pos_transactions.csv', 'table_name': 'pos_transactions'}
    )

    # Do 3 nguồn này độc lập, ta có thể cho chúng chạy song song
    [task_load_website, task_load_shopee, task_load_pos]