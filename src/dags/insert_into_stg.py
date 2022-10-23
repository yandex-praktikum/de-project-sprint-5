import datetime
import time
import psycopg2
import requests
import json
import pandas as pd
import numpy as np
from airflow import DAG
from airflow.operators.python import PythonOperator
import   pendulum


def upload_to_stg():
    sort_field = 'id'
    sort_direction = 'asc'
    limit = 50
    offset = 0
    get_report_response_restaurants = requests.get(
        f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}", 
        headers={
        "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
        "X-Nickname": "andreiten",
        "X-Cohort": "5"
        }
    ).json()
    restaurants = pd.DataFrame(get_report_response_restaurants)
    restaurants = restaurants.rename(columns = {'_id' : 'restaurant_id', 'name' : 'restaurant_name'})

    offsets = [0, 50]
    couriers_full = pd.DataFrame()
    for offset in offsets:  
        get_report_response_couriers = requests.get(
            f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}", 
            headers={
            "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
            "X-Nickname": "andreiten",
            "X-Cohort": "5"
            }
        ).json()
        couriers = pd.DataFrame(get_report_response_couriers)
        couriers = couriers.rename(columns = {'_id': 'courier_id', 'name':'courier_name'})
        couriers_full = pd.concat([couriers_full, couriers], ignore_index=True)

    deliveries_full = pd.DataFrame()
    for restaurant in restaurants['restaurant_id']:
        sort_field = 'id'
        sort_direction = 'asc'
        limit = 50
        offsets = [0, 50]
        from_1 = '2020-01-01 00:00:00'
        to = '2022-12-31 00:00:00'
        
        for offset in offsets:
            get_report_response_deliveries = requests.get(
                f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?restaurant_id={restaurant}&from={from_1}&to={to}&sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={limit}", 
                headers={
                "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
                "X-Nickname": "andreiten",
                "X-Cohort": "5"
                }
            ).json()
            deliveries = pd.DataFrame(get_report_response_deliveries)
            deliveries['delivery_ts'] = pd.to_datetime(deliveries['delivery_ts'])
            deliveries['order_ts'] = pd.to_datetime(deliveries['order_ts'])
            deliveries_full = pd.concat([deliveries_full, deliveries], ignore_index=True)
    
    conn2 = psycopg2.connect(f"dbname='de' port='5432' user='jovyan' host='localhost' password='jovyan'")
    cur = conn2.cursor()

    cols = ','.join(list(restaurants.columns))
    insert_stmt = f"INSERT INTO stg.restaurants ({cols}) VALUES %s"
    psycopg2.extras.execute_values(cur, insert_stmt, restaurants.values)

    cols = ','.join(list(couriers_full.columns))
    insert_stmt = f"INSERT INTO stg.couriers ({cols}) VALUES %s"
    psycopg2.extras.execute_values(cur, insert_stmt, couriers_full.values)

    cols = ','.join(list(deliveries_full.columns))
    insert_stmt = f"INSERT INTO stg.deliveries ({cols}) VALUES %s"
    psycopg2.extras.execute_values(cur, insert_stmt, deliveries_full.values)
    conn2.commit()

    cur.close()
    conn2.close()

with DAG(dag_id="insert_into_stg", schedule_interval="@monthly",  
    catchup=True, start_date=pendulum.datetime(2022, 10, 10, tz="UTC")) as dag:
    
    upload_to_stg_s = PythonOperator( task_id='upload_to_stg',
                                    python_callable=upload_to_stg,
                                    dag=dag)
    
    upload_to_stg_s
