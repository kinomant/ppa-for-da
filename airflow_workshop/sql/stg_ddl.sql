create table stg.work_programs
(id integer, academic_plan_in_field_of_study text, wp_in_academic_plan text, update_ts timestamp);
create table stg.up_description
(id integer, plan_type text, direction_id text, ns_id text, direction_code text, direction_name text, edu_program_id text, edu_program_name text, 
faculty_id text, faculty_name text, training_period text, university_partner text, up_country text, lang text, military_department boolean, total_intensity text, 
ognp_id text, ognp_name text, selection_year text);
ALTER TABLE stg.up_description ADD CONSTRAINT up_description_uindex UNIQUE (id);
create table stg.su_wp
(fak_id integer, fak_title text, wp_list text);integer);