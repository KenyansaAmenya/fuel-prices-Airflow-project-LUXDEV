from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import json
import pandas as pd
import http.client

# Database/postgreSQL Configuration
DB_NAME = "airflow_db"
DB_USER = "peter"
DB_PASSWORD = "1234"
DB_HOST = "194.180.176.173"
DB_PORT = "5432"

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG defination
dag = DAG(
    'gas_prices_etl_with_schema',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
    tags=['gas', 'postgres', 'etl']
)

# ETL
# Step 1. Extraction
def extract_gas_price():
    conn = http.client.HTTPConnection("api.collectapi.com")
    headers = {
    'content-type': "application/json",
    'authorization': "apikey *******"
    }
    conn.request("GET", "/gasPrice/fromCity?city=nairobi", headers=headers)
    res = conn.getresponse()
    data = res.read().decode("utf-8")

    with open('/tmp/gas_price_data.json', 'w') as f:
        f.write(data)


# Step 2. Transformation
def transform():
    with open('/tmp/gas_price_data.json', 'w') as f:
        raw_data = json.load(f)

        records = []
        for item in raw_data.get("result", []):
            price_clean = float(item["price"].replace(" TL", "").replace(",", "."))
            records.append({
                "fuel_type": item["name"],
                "price": price_clean,
                "currency": item["currency"],
                "timestamp": datetime.now()
            })

            df = pd.DataFrame(records)
            df.to_csv('/tmp/gas_price_transformed.csv', index=False)
            print(df)


# Step 3. Load
def load_to_postgress():
    df = pd.read_csv('/tmp/gas_price_transformed.csv')

    conn = psycopg2.connect(
        dbname = DB_NAME,
        user = DB_USER,
        password = DB_PASSWORD,
        host = DB_HOST,
        port = DB_PORT
    )
    cursor = conn.cursor()

    # create table if  not exists
    cursor.execute("""
         CREATE SCHEMA IF NOT EXISTS nairobi;
         CREATE TABLE IF NOT EXISTS nairobi.gas_prices (
                   id SERIAL PRIMARY KEY,
                   fuel_type TEXT,
                   price NUMERIC,
                   currency TEXT,
                   timestamp TIMESTAMP
        );          
    """)
# INSERT DATA
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO nairobi.gas_prices (fuel_type, price, currency, timestamp)
        VALUES (%s, %s, %s, %s);           
""", (row['fuel_type'], row['price'], row['currency'], row['timestamp']))
    
conn.commit()
cursor.close()
conn.close()
print("Data loaded into gas_prices table.")


# Task Definations
extract_task = PythonOperator(
    task_id = 'extract_gas_price',
    python_callable=extract_gas_price,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgress,
    dag=dag
)

# Task Dependencies
extract_task >> transform_task >> load_task
