create table cdm.up_wp_statuses
(edu_program_name text,
       selection_year integer,
       unit_title text,
       level_name text,
       up_id integer,
       state_name text,
       state_count integer,
       annotated integer);
insert into cdm.up_wp_statuses
(edu_program_name, selection_year, unit_title, level_name, up_id, state_name, state_count, annotated)
with t as (
select u.edu_program_name,
       u.selection_year,
       u2.unit_title, 
       l.level_name,
       u.id as up_id,
       wu.wp_id 
from dds.up u
join dds.levels l
on u.level_id = l.id 
join dds.wp_up wu 
on wu.up_id = u.id
left join dds.units u2 
on u2.id = u.unit_id
),
t2 as (
select t.edu_program_name, 
       t.selection_year,
       t.unit_title,
       t.level_name,
       t.up_id,
       w.discipline_code,
       w.wp_title,
       case when w.wp_description is null then 0
       else 1 end as description_flag,
       s.state_name 
from t
join dds.wp w
on t.wp_id = w.wp_id
join dds.states s 
on w.wp_status = s.id),
t3 as (
select edu_program_name,
       selection_year::integer,
       unit_title,
       level_name,
       up_id,
       state_name,
       count(distinct discipline_code) as state_count,
       sum (description_flag) as annotated
from t2
group by
       edu_program_name,
       selection_year,
       unit_title,
       level_name,
       up_id,
       state_name)
 select *
 from t3
-- status
create table cdm.su_wp_statuses
(wp_id integer,
discipline_code integer,
wp_title text,
state_name text,
unit_title text,
description_flag smallint,
number_od_editors integer
)
insert into cdm.su_wp_statuses
(wp_id,
discipline_code,
wp_title,
state_name,
unit_title,
description_flag,
number_od_editors
)
with t as (select wp.wp_id,
       wp.discipline_code,
       wp.wp_title,
       s.state_name,
       u2.unit_title,
       case when wp.wp_description is null then 0
       else 1 end as description_flag,
       we.editor_id
from dds.wp wp
join dds.states s 
on wp.wp_status = s.id
left join dds.units u2 
on u2.id = wp.unit_id
left join dds.wp_editor we 
on wp.wp_id = we.wp_id)
select wp_id,
discipline_code,
wp_title,
state_name,
unit_title,
description_flag,
count (editor_id) as number_od_editors
from t
group by wp_id,
discipline_code,
wp_title,
state_name,
unit_title,
description_flag
-- для офиса
with t as (
select u.id as up_id,
       u.edu_program_name,
       w.discipline_code,
       w.wp_title,
       u2.unit_title,
       s.state_name,
       case when w.wp_description is null then 0
       else 1 end as description_flag,
       we.editor_id,
       wm.prerequisites_cnt,
       wm.outcomes_cnt,
       'op.itmo.ru/work-program/' || wu.wp_id::text as link
from dds.up u
left join dds.wp_up wu 
on wu.up_id = u.id 
left join dds.wp w 
on w.wp_id = wu.wp_id 
left join dds.states s 
on w.wp_status = s.id
left join dds.units u2 
on u2.id = w.unit_id
left join dds.wp_editor we 
on w.wp_id = we.wp_id
left join dds.wp_markup wm 
on w.discipline_code = wm.discipline_code 
where u.selection_year = '2022'
order by 1, 3)
select up_id,
edu_program_name,
discipline_code,
wp_title,
unit_title,
state_name,
description_flag,
prerequisites_cnt,
outcomes_cnt,
count (editor_id) as number_od_editors,
link
from t
group by 
up_id,
edu_program_name,
discipline_code,
wp_title,
unit_title,
state_name,
description_flag,
prerequisites_cnt,
outcomes_cnt,
link

-- онлайн-курсы
select u.id, u.edu_program_name, u.selection_year,
wp.discipline_code, wp.wp_title,        
oc.title as online_course_title, oc.institution       
from dds.online_courses oc 
join dds.wp wp
on wp.discipline_code = oc.discipline_code 
join dds.wp_up wu 
on wu.wp_id = wp.wp_id
join dds.up u 
on u.id = wu.up_id 
group by 1,2,3,4,5,6,7
order by 1, 4

-- evaluation tools (для Джавлах)
create table cdm.evaluation_tools
(up_id integer,
edu_program_name text,
level_name text,
selection_year integer,
up_unit text,
discipline_code integer,
wp_title text,
disc_unit text,
state_name text,
tool_type text,
tool_id integer
)
insert into cdm.evaluation_tools
(up_id,
edu_program_name ,
level_name ,
selection_year ,
up_unit ,
discipline_code ,
wp_title ,
disc_unit ,
state_name ,
tool_type,
tool_id
)
with t as (select u.id as up_id,
       u.unit_id as up_unit,
       u.edu_program_name,
       lev.level_name,
       u.selection_year,
       w.discipline_code,
       w.wp_title,
       u2.unit_title as disc_unit,
       s.state_name,
       et.type as tool_type,
       et.id as tool_id
from dds.up u
left join dds.wp_up wu 
on wu.up_id = u.id 
left join dds.wp w 
on w.wp_id = wu.wp_id 
left join dds.states s 
on w.wp_status = s.id
left join dds.units u2 
on u2.id = w.unit_id
left join dds.wp_editor we 
on w.wp_id = we.wp_id
left join dds.wp_markup wm 
on w.discipline_code = wm.discipline_code 
left join dds.levels lev 
on u.level_id = lev.id 
left join stg.evaluation_tools et
on w.wp_id = et.wp_id 
where ((lev.level_name = 'бакалавриат') and (u.selection_year > '2018')) 
or (lev.level_name = 'специалитет')
or ((lev.level_name = 'магистратура') and (u.selection_year > '2020')) 
order by 1, 3)
select t.up_id, 
       t.edu_program_name,
       t.level_name,
       t.selection_year::integer,
       u2.unit_title as up_unit,
       t.discipline_code,
       t.wp_title,
       t.disc_unit,
       t.state_name,
       t.tool_type,
       t.tool_id
from t
left join dds.units u2 
on u2.id = t.up_unit

