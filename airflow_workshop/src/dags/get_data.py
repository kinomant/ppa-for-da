import requests
import pandas as pd
import pendulum
import json
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

# эти значения надо убрать в Variables
url = "https://op.itmo.ru/auth/token/login"
auth_data = {"username": "analytic", "password": "datatest"}

token_txt = requests.post(url, auth_data).text
token = json.loads(token_txt)["auth_token"]
headers = {'Content-Type': "application/json", 'Authorization': "Token " + token}


def get_descriptions():
    df = pd.DataFrame(columns = ['НАШ_ИД_УП', 'ИД_УП', 'ОБРАЗОВАТЕЛЬНАЯ_ПРОГРАММА', 'НАШ_ИД','DISC_DISC_ID', 'ДИСЦИПЛИНА', 'ОПИСАНИЕ'])
    url_down = 'https://op.itmo.ru/api/record/academic_plan/academic_wp_description/all?format=json&page=1'
    page = requests.get(url_down, headers=headers)
    res = json.loads(page.text)['results']
    for r in res:
        line = [r['id']]
        print (r['id'])
        s = r['academic_plan_in_field_of_study']  
        line.append (s[0]['ap_isu_id'])
        line.append (s[0]['title'])
        descriptions = r["wp_in_academic_plan"]
        for des in descriptions:
            linedisc = line.copy()
            linedisc.append(des['id'])
            linedisc.append(des['discipline_code'])
            linedisc.append(des['title'])
            linedisc.append(des['description'])
            df.loc[len(df.index)] = linedisc
    if len(df) > 0:
        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('wp_description', df.values, target_fields=df.columns.tolist())


with DAG(dag_id='api_to_stg', start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), schedule_interval="@daily", catchup=False) as dag:
    t1 = PythonOperator(
    task_id='get_descriptions',
    python_callable=get_descriptions
    )
 
t1 