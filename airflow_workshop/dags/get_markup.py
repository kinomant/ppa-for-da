import requests
import pandas as pd
import pendulum
import json
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

url = "https://op.itmo.ru/auth/token/login"
username = Variable.get ("username")
password = Variable.get ("password")
auth_data = {"username": username, "password": password}

token_txt = requests.post(url, auth_data).text
token = json.loads(token_txt)["auth_token"]
headers = {'Content-Type': "application/json", 'Authorization': "Token " + token}

def get_markup():
    ids = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_records(
    """
    select distinct discipline_code from dds.wp_markup wm 
    left join dds.wp_up wu 
    on wu.wp_id = wm.id 
    where (prerequisites = '[]' or outcomes = '[]') 
    and  wu.up_id in 
    (select id from dds.up u where u.selection_year > '2018')
    and wm.id not in 
    (select wp_id from dds.wp where wp_status = 4)
    order by discipline_code desc
    """)
    # ids = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_records(
    # """
    # select distinct discipline_code from dds.wp_markup wm 
    # left join dds.wp_up wu 
    # on wu.wp_id = wm.id 
    # where (prerequisites = '[]' or outcomes = '[]') 
    # and  wu.up_id in 
    # (select id from dds.up u where u.selection_year = '2022')
    # and wm.id not in 
    # (select wp_id from dds.wp where wp_status = 4)
    # order by discipline_code desc
    # """)
    # ids = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_records(
    # """
    # with t as (select 
    #     json_array_elements(wp_in_academic_plan::json)->>'discipline_code' as discipline_code
    # from stg.work_programs wp)
    # select distinct discipline_code from t
    # where discipline_code > (select max(discipline_code)::text from stg.wp_markup)
    # order by 1
    # """)
    # ids = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_records(
    # """
    # with t as (
    # select distinct (dby.work_programs::json)->>'discipline_code' as discipline_code from stg.disc_by_year dby 
    # order by 1)
    # select * from t
    # """)
    url_down = 'https://op.itmo.ru/api/workprogram/items_isu/'
    for wp_id in ids:
        wp_id = str(wp_id[0])
        print (wp_id)
        url = url_down + wp_id + '?format=json'
        page = requests.get(url, headers=headers)
        df = pd.DataFrame.from_dict(page.json(), orient='index')
        df = df.T
        df['prerequisites'] = df[~df['prerequisites'].isna()]["prerequisites"].apply(lambda st_dict: json.dumps(st_dict))
        df['outcomes'] = df[~df['outcomes'].isna()]["outcomes"].apply(lambda st_dict: json.dumps(st_dict))
        if len(df)>0:
            PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.wp_markup', df.values, target_fields=df.columns.tolist(), replace=True, replace_index='id')

with DAG(dag_id='get_markup', start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), schedule_interval='0 3 * * *', catchup=False) as dag:
    t1 = PythonOperator(
    task_id='get_markup',
    python_callable=get_markup
    )

t1 