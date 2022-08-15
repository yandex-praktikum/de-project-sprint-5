import logging
import pendulum
import psycopg2
from datetime import datetime
from requests import request
import json
import jinja2

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

#constants
NICKNAME = 'nkurdyubov'
COHORT = '1'
API_KEY = '25c27781-8fde-4b30-a22e-524044a7580f'

DATABASE='de'
HOST='localhost'
USER='jovyan'
PASSWORD='jovyan'
PORT='5432'

log = logging.getLogger(__name__)


#variables
list_of_datasets = ['deliveries', 'couriers']

log.info('connection to db')
source_to = psycopg2.connect(database=DATABASE,
                             host=HOST,
                             user=USER,
                             password=PASSWORD,
                             port=PORT)

log.info('creating jinja template')
environment = jinja2.Environment()
template = environment.from_string('''insert into stg.deliverysystem_{{table}} (json) values
                                        {%for i in data%} 
                                        ('{{jsondumps(i) }}') {{"," if not loop.last else ""}}
                                        {%endfor%}''')


#Класс для работы с апи
class ApiWorker:
    
    def __init__(self,
                 nickname,
                 cohort,
                 api_key,
                 base_url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'):
        self.headers = {'X-Nickname': nickname,
                        'X-Cohort': cohort,
                        'X-API-KEY': api_key}
        self.base_url = base_url
    
    def get_data(self,
                 api_method,
                 sort_field='id',
                 sort_direction='asc', 
                 limit='50', 
                 offset='0'):
        url = f'{self.base_url}/{api_method}?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'
        response = request('GET', url, headers = self.headers).json()
        return response

#Функция для передачи в джинджу
def jsondumps(i):
    return json.dumps(i, ensure_ascii=False)

#Функция для дага
def import_data(method, api_worker, conn, template):

    cur_to = conn.cursor()
    response_len = 50
    while response_len == 50:
        #Получаем оффсет
        log.info(f'getting offsset for {method}')
        cur_to.execute(f'select max(offset_data) from stg.{method}_offset')
        offset = cur_to.fetchall()[0][0]
        if not offset:
            offset = 0
        log.info(f'offsset: {offset}')
        #Получаем данные
        log.info(f'getting data {method}')
        data = api_worker.get_data(api_method=method, offset=str(offset))
        if data == []:
            log.info(f'No new records for {method}')
            return
        response_len = len(data)
        log.info(f'Got data {method}: {response_len} rows')
        #Записываем
        log.info(f'Insertion {method}')
        cur_to.execute(template.render(data=data, jsondumps=jsondumps, table=method))
        source_to.commit()
        #Обновляем оффсет
        log.info(f'Updating offset for {method}')
        cur_to.execute(f'insert into stg.{method}_offset select max(id), now() from stg.deliverysystem_{method}')
        source_to.commit()
        
with DAG(
    'project',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 8, 6, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'project'],
    is_paused_upon_creation=False
) as dag:
    for i in list_of_datasets:
        get_data = PythonOperator(
            task_id=f'get_data_{i}',
            python_callable=import_data,
            op_kwargs={'method': i,
                       'api_worker': ApiWorker(NICKNAME, COHORT, API_KEY),
                       'conn': source_to,
                       'template': template})
        get_data
