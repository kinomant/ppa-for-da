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

target_fields = ['fak_id', 'fak_title', 'wp_id', 'wp_title', 'wp_discipline_code', 'editor_id', 'editor_username', 'editor_first_name', 'editor_last_name', 'editor_email', 'editor_isu_number']

df_su = pd.DataFrame(columns = ['ИД_ФАКУЛЬТЕТА', 'НАЗВАНИЕ', 'WP_ID', 'WP_title', 'WP_discipline_code', 'Ed_ID', 'Ed_Uname', 'Ed_first_name', 'Ed_last_name', 'Ed_email', 'Ed_isu_number'])

def get_structural_units():
    url_down = 'https://op.itmo.ru/api/record/structural/workprogram'
    page = requests.get(url_down, headers=headers)
    res = json.loads(page.text)
    for r in res:
        line = [r['id']]
        line.append (r['title'])
        wps = r['work_programs']
        for wp in wps:
            linewp = line.copy()
            linewp.append(wp['id'])
            linewp.append(wp['title'])
            linewp.append(wp['discipline_code'])
            editors = wp['editors']
            for ed in editors:
                lineed = linewp.copy()
                lineed.append(ed['id'])
                lineed.append(ed['username'])
                lineed.append(ed['first_name'])
                lineed.append(ed['last_name'])
                lineed.append(ed['email'])
                lineed.append(ed['isu_number'])
                df_su.loc[len(df_su.index)] = lineed
                # PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.stuctural_units', df_su.values, target_fields = target_fields)
    if len(df_su) > 0:
        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.structural_units', df_su.values, target_fields = target_fields)


with DAG(dag_id='get_structural_units', start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), schedule_interval="@weekly", catchup=False) as dag:
    t1 = PythonOperator(
    task_id='get_structural_units',
    python_callable=get_structural_units
    )
 
t1 