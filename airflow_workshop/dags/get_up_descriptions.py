import requests
import pandas as pd
import pendulum
import json
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

url = "https://id.itmo.ru/auth/realms/itmo/protocol/openid-connect/token"
client_id = Variable.get ("client_id")
client_secret = Variable.get ("client_secret")
grant_type = Variable.get ("grant_type")

auth_data = {"client_id": client_id, "client_secret": client_secret, "grant_type": grant_type}
token_txt = requests.post(url, auth_data).text
token = json.loads(token_txt)["access_token"]
headers = {'Content-Type': "application/json", 'Authorization': "Token " + token}

dt = pendulum.now().to_iso8601_string()
    
def get_up(up_id):
    up_id = str(up_id)
    url = 'https://disc.itmo.su/api/v1/academic_plans/' + up_id
    page = requests.get(url, headers=headers)
    df = pd.DataFrame(page.json()['result'])
    df = df.drop(['disciplines_blocks'], axis=1)
    if len(df)>0:
        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.up_description', df.values, target_fields=df.columns.tolist(), replace=True, replace_index='id')

def get_up_description():
    ids = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_records(
    """
    select distinct isu_id_up from stg.wp_description
    """)
    for i in ids:
        get_up(i[0])

with DAG(dag_id='get_up_descriptions', start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), schedule_interval="@monthly", catchup=False) as dag:
    t1 = PythonOperator(
    task_id='get_up_description',
    python_callable=get_up_description
    )
 
t1 