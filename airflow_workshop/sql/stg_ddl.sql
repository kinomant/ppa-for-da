create table stg.wp_description
(id serial, cop_id_up integer, isu_id_up integer, isu_op_name text, cop_id_rpd integer, isu_id_rpd integer, cop_disc_name text, cop_disc_description text, "status" varchar(2), update_ts timestamp);
create table stg.up_description
(id integer, plan_type text, direction_id text, ns_id text, direction_code text, direction_name text, edu_program_id text, edu_program_name text, 
faculty_id text, faculty_name text, training_period text, university_partner text, up_country text, lang text, military_department boolean, total_intensity text, 
ognp_id text, ognp_name text, selection_year text);
ALTER TABLE stg.up_description ADD CONSTRAINT up_description_uindex UNIQUE (id);
create table stg.su_wp
(fak_id integer, fak_title text, wp_list text);
-- create table stg.structural_units
-- (fak_id integer, fak_title text, wp_id integer, wp_title text, wp_discipline_code varchar, editor_id integer, editor_username text, editor_first_name text, editor_last_name text, editor_email text, editor_isu_number integer);