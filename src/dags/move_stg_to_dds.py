import datetime
import time
import psycopg2
import requests
import json
import pandas as pd
import numpy as np
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

def move_restaurants_to_dds():
    conn = psycopg2.connect(f"dbname='de' port='5432' user='jovyan' host='localhost' password='jovyan'")
    cur = conn.cursor()
    cur.execute('''
    INSERT INTO dds.restaurants (restaurant_id, restaurant_name) 
    select 
    restaurant_id,
    restaurant_name 
    from stg.restaurants;
    ''')
    conn.commit()
    cur.close()
    conn.close()

def move_couriers_to_dds():
    conn = psycopg2.connect(f"dbname='de' port='5432' user='jovyan' host='localhost' password='jovyan'")
    cur = conn.cursor()
    cur.execute('''
    INSERT INTO dds.couriers ( courier_id, courier_name) 
    select 
    distinct
    courier_id ,
    courier_name  
    from stg.couriers;
    ''')
    conn.commit()
    cur.close()
    conn.close()


def move_timestamp_to_dds():
    conn = psycopg2.connect(f"dbname='de' port='5432' user='jovyan' host='localhost' password='jovyan'")
    cur = conn.cursor()
    cur.execute('''
    INSERT INTO dds.timestamps ( ts, year, month, day, time, date) 
    select 
    distinct
    delivery_ts as ts,
    extract (year from delivery_ts) as "year",
    extract (month from delivery_ts) as "month",
    extract (day from delivery_ts) as "day",
    delivery_ts:: time as "time",
    delivery_ts:: date as "date"
    from stg.deliveries ;
    ''')
    conn.commit()
    cur.close()
    conn.close()

def move_deliveries_to_dds():
    conn = psycopg2.connect(f"dbname='de' port='5432' user='jovyan' host='localhost' password='jovyan'")
    cur = conn.cursor()
    cur.execute('''
    INSERT INTO dds.deliveries ( order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum) 
    select 
    distinct
    order_id,
    order_ts ,
    delivery_id ,
    courier_id ,
    address ,
    delivery_ts ,
    rate ,
    "sum",
    tip_sum 
    from stg.deliveries;
    ''')
    conn.commit()
    cur.close()
    conn.close()

with DAG(dag_id="move_from_stg_to_dds", schedule_interval="@monthly",  
    catchup=True, start_date=pendulum.datetime(2022, 10, 10, tz="UTC")) as dag:
    
    move_restaurants_to_dds_s = PythonOperator( task_id='move_restaurants_to_dds',
                                    python_callable=move_restaurants_to_dds,
                                    dag=dag)
    move_couriers_to_dds_s = PythonOperator( task_id='move_couriers_to_dds',
                                    python_callable=move_couriers_to_dds,
                                    dag=dag)
    move_timestamp_to_dds_s = PythonOperator( task_id='move_timestamp_to_dds',
                                    python_callable=move_timestamp_to_dds,
                                    dag=dag)
    move_deliveries_to_dds_s = PythonOperator( task_id='move_deliveries_to_dds',
                                    python_callable=move_deliveries_to_dds,
                                    dag=dag)
    
    move_restaurants_to_dds_s >> move_couriers_to_dds_s >> move_timestamp_to_dds_s >> move_deliveries_to_dds_s
