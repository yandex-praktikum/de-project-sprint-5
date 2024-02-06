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

def fill_cdm():
    conn = psycopg2.connect(f"dbname='de' port='5432' user='jovyan' host='localhost' password='jovyan'")
    cur = conn.cursor()
    cur.execute('''
    INSERT INTO cdm.settlement_report (courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum) 
    with a as (
    select
    d.courier_id,
    c.courier_name,
    t."year"  as settlement_year,
    t."month" as settlement_month ,
    count(d.order_id) as orders_count,
    sum(d."sum") as orders_total_sum,
    avg(d.rate) as rate_avg,
    sum(d."sum") * 0.25 as order_processing_fee,
    sum(d.tip_sum) as courier_tips_sum 
    from dds.deliveries d 
    left join dds.couriers c on d.courier_id = c.courier_id 
    left join dds.timestamps t on d.delivery_ts = t.ts 
    group by d.courier_id, c.courier_name, t."year",  t."month" ),
    b as (
    select 
    s.courier_id ,
    a.rate_avg,
    s."sum",
    case
        when a.rate_avg < 4 then s."sum" * 0.05
        when a.rate_avg >= 4 and a.rate_avg < 4.5 then s."sum" * 0.07
        when a.rate_avg >= 4.5 and a.rate_avg <4.9 then s."sum" * 0.08
        when a.rate_avg >= 4.9 then s."sum" * 0.1 
    end courier_order_sum 
    from dds.deliveries s
    left join a on a.courier_id = s.courier_id),
    c as (
    select 
    courier_id,
    rate_avg,
    "sum",
    courier_order_sum,
    case 
        when b.rate_avg < 4 and courier_order_sum <100 then 100
        when b.rate_avg >= 4 and b.rate_avg < 4.5 and  courier_order_sum < 150 then 150 
        when b.rate_avg >= 4.5 and b.rate_avg <4.9 and  courier_order_sum < 175 then 175  
        when b.rate_avg >= 4.9 and  courier_order_sum < 200 then 200 
    else courier_order_sum
    end courier_order_sum_actual
    from b),
    d as (
    select 
    courier_id ,
    sum(courier_order_sum_actual) as courier_order_sum
    from c
    group by courier_id)
    select
    a.courier_id,
    a.courier_name,
    a.settlement_year,
    a.settlement_month,
    a.orders_count,
    a.orders_total_sum,
    a.rate_avg,
    a.order_processing_fee,
    d.courier_order_sum,
    a.courier_tips_sum,
    (d.courier_order_sum + a.courier_tips_sum) * 0.95 as courier_reward_sum 
    from a
    left join d on a.courier_id = d.courier_id 

    ''')
    conn.commit()
    cur.close()
    conn.close()

with DAG(dag_id="fill_cdm", schedule_interval="@monthly",  
    catchup=True, start_date=pendulum.datetime(2022, 10, 10, tz="UTC")) as dag:
    fill_cdm_s = PythonOperator( task_id='fill_cdm',
                                    python_callable=fill_cdm,
                                    dag=dag)
    fill_cdm_s
