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

def get_up_detail():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    truncate stg.up_detail  restart identity cascade;
    """)
    ids = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_records(
    """
    select id as op_id
    from stg.work_programs wp
    
    """)
    url_down = 'https://op.itmo.ru/api/academicplan/detail/'
    target_fields = ['id', 'ap_isu_id', 'on_check', 'laboriousness', 'academic_plan_in_field_of_study']
    for op_id in ids:
        op_id = str(op_id[0])
        print (op_id)
        url = url_down + op_id + '?format=json'
        page = requests.get(url, headers=headers)
        df = pd.DataFrame.from_dict(page.json(), orient='index')
        df = df.T
        df['academic_plan_in_field_of_study'] = df[~df['academic_plan_in_field_of_study'].isna()]["academic_plan_in_field_of_study"].apply(lambda st_dict: json.dumps(st_dict))
        df = df[target_fields]
        PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.up_detail', df.values, target_fields = target_fields)

#     page = requests.get(url_down, headers=headers)
#     res = list(json.loads(page.text))
#     for su in res:
#         df = pd.DataFrame.from_dict(su)
#         # превращаем последний столбец в json
#         df['academic_plan_in_field_of_study'] = df[~df['academic_plan_in_field_of_study'].isna()]["academic_plan_in_field_of_study"].apply(lambda st_dict: json.dumps(st_dict))
#         PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.up_detail', df.values, target_fields = target_fields)

# url_down = 'https://op.itmo.ru/api/workprogram/items_isu/'
#     for wp_id in ids:
#         wp_id = str(wp_id[0])
#         print (wp_id)
#         url = url_down + wp_id + '?format=json'
#         page = requests.get(url, headers=headers)
#         df = pd.DataFrame.from_dict(page.json(), orient='index')
#         df = df.T
#         df['prerequisites'] = df[~df['prerequisites'].isna()]["prerequisites"].apply(lambda st_dict: json.dumps(st_dict))
#         df['outcomes'] = df[~df['outcomes'].isna()]["outcomes"].apply(lambda st_dict: json.dumps(st_dict))
#         if len(df)>0:
#             PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').insert_rows('stg.wp_markup', df.values, target_fields=df.columns.tolist(), replace=True, replace_index='id')

# "id": 6796,
# "ap_isu_id": 10572,
# "on_check": "in_work",
# "laboriousness": 393,
# "academic_plan_in_field_of_study": [
#         {
#             "id": 6859,
#             "year": 2018,
#             "qualification": "bachelor",
#             "title": "Нанофотоника и квантовая оптика",
#             "field_of_study": [
#                 {
#                     "number": "16.03.01",
#                     "id": 15772,
#                     "title": "Техническая физика",
#                     "qualification": "bachelor",
#                     "educational_profile": null,
#                     "faculty": null
#                 }
#             ],
#             "plan_type": "base",
#             "training_period": 0,
#             "structural_unit": null,
#             "total_intensity": 0,
#             "military_department": false,
#             "university_partner": [],
#             "editors": []
#         }
    

with DAG(dag_id='get_up_detail', start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), schedule_interval='0 1 * * *', catchup=False) as dag:
    t1 = PythonOperator(
    task_id='get_up_detail',
    python_callable=get_up_detail
    )

t1
