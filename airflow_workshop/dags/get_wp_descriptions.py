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

target_fields = ['cop_id_up', 'isu_id_up', 'isu_op_name', 'cop_id_rpd', 'isu_id_rpd', 'cop_disc_name', 'cop_disc_description', 'status', 'update_ts']

df = pd.DataFrame(columns = ['НАШ_ИД_УП', 'ИД_УП', 'ОБРАЗОВАТЕЛЬНАЯ_ПРОГРАММА', 'НАШ_ИД','DISC_DISC_ID', 'ДИСЦИПЛИНА', 'ОПИСАНИЕ', 'СТАТУС', 'ВРЕМЯ'])

def get_descriptions():
    url_down = 'https://op.itmo.ru/api/record/academic_plan/academic_wp_description/all?format=json&page=1'
    page = requests.get(url_down, headers=headers)
    c = json.loads(page.text)['count']
    dt = pendulum.now().to_iso8601_string()
    for p in range(1,c//10+2):
        url_down = 'https://op.itmo.ru/api/record/academic_plan/academic_wp_description/all?format=json&page=' + str(p)
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
                linedisc.append(des['status'])
                linedisc.append(dt)
                df.loc[len(df.index)] = linedisc
    if len(df) > 0:
        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.wp_description', df.values, target_fields = target_fields)


with DAG(dag_id='get_wp_descriptions', start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), schedule_interval="@daily", catchup=False) as dag:
    t1 = PythonOperator(
    task_id='get_descriptions',
    python_callable=get_descriptions
    )
 
t1 