import pendulum
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator

def directions ():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO dds.directions (direction_id, direction_code, direction_name)
    select distinct direction_id::integer, direction_code, direction_name from stg.up_description ud 
    """)

def levels ():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO dds.levels (training_period, level_name)
    with t as (select distinct training_period from stg.up_description ud)
    select training_period, 
        case when training_period = '2'   then 'магистратура' 
                when training_period = '4'   then 'бакалавриат'
                else 'специалитет'
        end as level_name
    from t
    """)

def editors ():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO dds.editors (id, username, first_name, last_name, email, isu_number)
    select distinct (json_array_elements(wp_list::json->'editors')::json->>'id')::integer as editor_id, 
        (json_array_elements(wp_list::json->'editors')::json->>'username') as username,
        (json_array_elements(wp_list::json->'editors')::json->>'first_name') as first_name,
        (json_array_elements(wp_list::json->'editors')::json->>'last_name') as last_name,
        (json_array_elements(wp_list::json->'editors')::json->>'email') as email,
        (json_array_elements(wp_list::json->'editors')::json->>'isu_number') as isu_number
    from stg.su_wp sw 
    """)

def states ():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO dds.states (cop_state, state_name)
    with t as (select distinct (json_array_elements(wp_in_academic_plan::json)->>'status') as cop_states from stg.work_programs wp)
    select cop_states, 
        case when cop_states ='AC' then 'одобрено' 
                when cop_states ='AR' then 'архив'
                when cop_states ='EX' then 'на экспертизе'
                when cop_states ='RE' then 'на доработке'
                else 'в работе'
        end as state_name
    from t
    """)

def units ():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO dds.units (id, unit_title, faculty_id)
    select 
        distinct sw.fak_id, 
        sw.fak_title, 
        ud.faculty_id::integer
    from stg.su_wp sw 
    left join stg.up_description ud 
    on sw.fak_title = ud.faculty_name 
    """)

def up ():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO dds.up (id, plan_type, direction_id, ns_id, edu_program_id, edu_program_name, unit_id, level_id, university_partner , up_country, lang, military_department, selection_year)
    select ud.id, 
        ud.plan_type, 
        d.direction_id,
        ud.ns_id::integer,
        ud.edu_program_id::integer,
        ud.edu_program_name,
        u.id as unit_id,
        l.id as level_id,
        ud.university_partner, 
        ud.up_country, 
        ud.lang, 
        ud.military_department, 
        ud.selection_year::integer
    from stg.up_description ud 
    left join dds.directions d 
    on d.direction_code = ud.direction_code 
    left join dds.units u 
    on u.unit_title  = ud.faculty_name 
    left join dds.levels l 
    on ud.training_period = l.training_period 
    """)

def wp ():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO dds.wp (wp_id, discipline_code, wp_title, wp_status, unit_id, wp_description)
    with wp_desc as (
    select 
        distinct json_array_elements(wp_in_academic_plan::json)->>'id' as wp_id,
        json_array_elements(wp_in_academic_plan::json)->>'discipline_code' as discipline_code,
        json_array_elements(wp_in_academic_plan::json)->>'description' as wp_description,
        json_array_elements(wp_in_academic_plan::json)->>'status' as wp_status
    from stg.work_programs wp),
    wp_unit as (
    select fak_id,
        wp_list::json->>'id' as wp_id,
        wp_list::json->>'title' as wp_title,
        wp_list::json->>'discipline_code' as discipline_code
    from stg.su_wp sw)
    select wp_desc.wp_id::integer, 
        wp_desc.discipline_code::integer,
        wp_unit.wp_title,
        s.id as wp_status, 
        wp_unit.fak_id::integer as unit_id,
        wp_desc.wp_description
    from wp_desc
    left join wp_unit
    on wp_desc.discipline_code = wp_unit.discipline_code
    left join dds.states s 
    on wp_desc.wp_status = s.cop_state;
    """)

def wp_inter ():
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO dds.wp_editor (wp_id, editor_id)
    select (wp_list::json->>'id')::integer as wp_id,
        (json_array_elements(wp_list::json->'editors')::json->>'id')::integer as editor_id
    from stg.su_wp
    """)
    PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').run(
    """
    INSERT INTO dds.wp_up (wp_id, up_id)
    with t as (
    select id,
        (json_array_elements(wp_in_academic_plan::json)->>'id')::integer as wp_id,
        json_array_elements(academic_plan_in_field_of_study::json)->>'ap_isu_id' as up_id
    from stg.work_programs wp)
    select t.wp_id, (json_array_elements(wp.academic_plan_in_field_of_study::json)->>'ap_isu_id')::integer as up_id from t
    join stg.work_programs wp
    on t.id = wp.id
    """)

with DAG(dag_id='stg_to_dds', start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), schedule_interval='0 4 * * *', catchup=False) as dag:
    t1 = PythonOperator(
    task_id='directions',
    python_callable=directions 
    ) 
    t2 = PythonOperator(
    task_id='levels',
    python_callable=levels 
    ) 
    t3 = PythonOperator(
    task_id='editors',
    python_callable=editors 
    ) 
    t4 = PythonOperator(
    task_id='states',
    python_callable=states 
    ) 
    t5 = PythonOperator(
    task_id='units',
    python_callable=units 
    ) 
    t6 = PythonOperator(
    task_id='up',
    python_callable=up 
    ) 
    t7 = PythonOperator(
    task_id='wp',
    python_callable=wp 
    ) 
    t8 = PythonOperator(
    task_id='wp_inter',
    python_callable=wp_inter 
    ) 

[t1, t2, t3, t4, t5] >> t6 >> t7 >> t8