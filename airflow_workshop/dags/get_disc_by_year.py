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

def get_disc_by_year():
    ids = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_records(
    """
with t as (
select 
(json_array_elements(academic_plan_in_field_of_study::json)->>'ap_isu_id') :: integer as isu_id,
id
from stg.work_programs wp)
select id from t
where isu_id in
(
select id from stg.up_description ud 
where ((training_period = '2') and (selection_year > '2020'))
   or ((training_period = '4') and (selection_year > '2018'))
) and
id < 7256
order by id
    """)
    for up_id in ids:
        up_id = str(up_id[0])
        print (up_id)
        url = 'https://op.itmo.ru/api/record/academicplan/get_wp_by_year/' + up_id + '?year=2022/2023'
        page = requests.get(url, headers=headers)
        df = pd.DataFrame.from_dict(page.json())
        df['work_programs'] = df[~df['work_programs'].isna()]["work_programs"].apply(lambda st_dict: json.dumps(st_dict))
        if len(df)>0:
            PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.disc_by_year', df.values, target_fields=df.columns.tolist())

with DAG(dag_id='get_disc_by_year', start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), schedule_interval='0 3 * * *', catchup=False) as dag:
    t1 = PythonOperator(
    task_id='get_disc_by_year',
    python_callable=get_disc_by_year
    )

t1 