create table stg.work_programs
(id integer, academic_plan_in_field_of_study text, wp_in_academic_plan text, update_ts timestamp);
create table stg.up_description
(id integer, plan_type text, direction_id text, ns_id text, direction_code text, direction_name text, edu_program_id text, edu_program_name text, 
faculty_id text, faculty_name text, training_period text, university_partner text, up_country text, lang text, military_department boolean, total_intensity text, 
ognp_id text, ognp_name text, selection_year text);
ALTER TABLE stg.up_description ADD CONSTRAINT up_description_uindex UNIQUE (id);
create table stg.su_wp
(fak_id integer, fak_title text, wp_list text);
create table stg.wp_markup
(id integer, title text, discipline_code integer, prerequisites text, outcomes text);
ALTER TABLE stg.wp_markup ADD CONSTRAINT wp_id_uindex UNIQUE (id);
create table stg.online_courses
(id integer, institution text, title text, topic_with_online_course text);
create table stg.evaluation_tools
(id integer, type text, "name" text, description text, check_point bool, deadline integer, semester integer, 
"min" numeric, "max" numeric, descipline_sections text, evaluation_criteria text, wp_id integer);
ALTER TABLE stg.evaluation_tools ADD CONSTRAINT et_id_uindex UNIQUE (id);
create table stg.disc_by_year
(id integer, ap_isu_id integer, title text, work_programs text);
create table stg.up_detail
(id integer, ap_isu_id integer, on_check varchar(20), laboriousness integer, academic_plan_in_field_of_study text);
create table stg.up_isu
(id integer, plan_type text, direction_id text, ns_id text, direction_code text, direction_name text, edu_program_id text, edu_program_name text, 
faculty_id text, faculty_name text, training_period text, university_partner text, up_country text, lang text, military_department boolean, total_intensity text, 
ognp_id text, ognp_name text, selection_year text, disciplines_blocks text);
create table stg.wp_detail
(id integer, discipline_code varchar(20), title text, description text, structural_unit varchar(100), prerequisites text, discipline_sections text, bibliographic_reference text, outcomes text, certification_evaluation_tools text, expertise_status varchar(3));
create table stg.practice
(id integer, discipline_code varchar(20), title text, year text, authors text, op_leader text, language varchar(20), qualification text, kind_of_practice text,
type_of_practice text, way_of_doing_practice text, format_practice text, features_content_and_internship text, features_internship text, additional_reporting_materials text, form_of_certification_tools text,
passed_great_mark text, passed_good_mark text, passed_satisfactorily_mark text, not_passed_mark text, evaluation_tools_current_control text, prac_isu_id text, ze_v_sem varchar(30), 
evaluation_tools_v_sem text, number_of_semesters text, practice_base varchar(20),  structural_unit varchar(30), editors text, bibliographic_reference text, prerequisites text, outcomes text);
