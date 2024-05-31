'''
=================================================
Milestone 3

Nama  : Dendy Dwinanda
Batch : FTDS-030-RMT

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. Adapun dataset yang dipakai adalah dataset mengenai penjualan mobil di Indonesia selama tahun 2020.
=================================================
'''


from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from elasticsearch import Elasticsearch
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch

import datetime as dt
from datetime import timedelta

import pandas as pd

def ambil_data():
    '''fetch data dari postgres'''
    database = "airflow"
    username = "airflow"
    password = "airflow"
    host = "postgres"

    # Membuat URL koneksi PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Gunakan URL ini saat membuat koneksi SQLAlchemy
    engine = create_engine(postgres_url)
    conn = engine.connect()

    df = pd.read_sql_query("select * from tabel_m3", conn)
    # Save DataFrame to CSV
    df.to_csv('/opt/airflow/dags/P2M3_dendy_dwinanda_data_raw_load.csv', sep=',', index=False)

def clean_dataframe():
    ''' fungsi untuk membersihkan data'''
    # pembisihan data
    data = pd.read_csv("/opt/airflow/dags/P2M3_dendy_dwinanda_data_raw_load.csv")
    
    # drop duplicate data
    data = data.drop_duplicates()
    
    # remove row contain missing value
    data = data.dropna()

    # remove special character
    data['app'] = data['app'].str.replace(';', ' ').str.replace('&', '').str.replace('-', '').str.replace('  ', ' ')

    # remove special character
    data['app'] = data['app'].str.replace(' ', '')

    # mengubah column kedalam tipe data integer
    data['review'] = data['review'].str.replace('.', '').str.replace('M', '000000').astype(int)

    # Remove special characters from the 'Size' column and convert it to numeric
    data['size'] = data['size'].str.replace('M', '000000').str.replace('k', '000').str.replace('+', '').str.replace('Varies with device', 'NaN').str.replace(',', '').astype(float)
    
    # Remove special characters from the 'Installs' column and convert it to numeric
    data['installs'] = data['installs'].str.replace('+', '').str.replace(',', '').astype(int)

    # Remove special characters from 'price' and convert to float
    data['price'] = data['price'].str.replace('$', '').astype(float)

    # remove special character
    data['genres'] = data['genres'].str.replace(';', ' ').str.replace('&', '')
    
    # Convert 'Last Updated' column to datetime format
    data['last_updated'] = pd.to_datetime(data['last_updated'])
    
    # remove special character
    data['android_ver'] = data['android_ver'].str.replace(' and up', '')
    
    # remove row contain missing value
    data = data.dropna()
    
    # save data
    data.to_csv('/opt/airflow/dags/P2M3_dendy_dwinanda_data_clean.csv', index=False)


def upload_to_elasticsearch():
    ''' fungsi untuk mengupload data ke dalam elascticsearch'''
    es = Elasticsearch("http://elasticsearch:9200")
    data = pd.read_csv('/opt/airflow/dags/P2M3_dendy_dwinanda_data_clean.csv')
    
    for i, r in data.iterrows():
        doc = r.to_dict()  # Convert the row to a dictionary
        res = es.index(index="milestone_3", id=i+1, body=doc)
        print(f"Response from Elasticsearch: {res}")
        
        
default_args = {
    'owner': 'dendy',
    'start_date': dt.datetime(2024, 5, 20, 14, 30, 0) - dt.timedelta(hours=7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}


with DAG(
    'P2M3_dendy_dwinanda', #atur sesuai nama project kalian
    description='Milestone_3',
    schedule_interval=timedelta(minutes=60), #atur schedule untuk menjalankan airflow pada 06:30.
    default_args=default_args,
    catchup=False
) as dag:
    # Task : 1
    '''  Fungsi ini ditujukan untuk mengambil data dari postgres.'''
    ambil_data_pg = PythonOperator(
        task_id='ambil_data_postgres',
        python_callable=ambil_data)    

    # Task: 2
    '''  Fungsi ini ditujukan untuk menjalankan pembersihan data.'''
    edit_data = PythonOperator(
        task_id='edit_data',
        python_callable=clean_dataframe)

    # Task: 3
    '''  Fungsi ini ditujukan untuk menyimpan data kedalam elasticsearch.'''
    upload_data = PythonOperator(
        task_id='upload_data_elastic',
        python_callable=upload_to_elasticsearch)

    #proses untuk menjalankan di airflow
    ambil_data_pg >> edit_data >> upload_data