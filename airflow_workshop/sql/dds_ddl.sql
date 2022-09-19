-- нужно добавить ограничения
create table dds.states
(id serial, cop_state varchar(2), state_name varchar(20));
create table dds.directions
(id serial, direction_id integer, direction_code varchar(10), direction_name varchar(100));
create table dds.levels
(id serial, training_period varchar(5), level_name varchar(20));
create table dds.editors
(id integer, username varchar(50), first_name varchar(50), last_name varchar(50), email varchar(50), isu_number varchar(6));
create table dds.units
(id integer, unit_title varchar(100), faculty_id integer);
create table dds.up
(id integer, plan_type varchar(8), direction_id integer, ns_id integer, edu_program_id integer, edu_program_name text, unit_id integer, level_id integer, university_partner text, up_country text, lang text, military_department boolean, selection_year integer);
create table dds.wp
(wp_id integer, discipline_code integer, wp_title text, wp_status integer, unit_id integer, wp_description text);
create table dds.wp_editor
(wp_id integer, editor_id integer);
create table dds.wp_up
(wp_id integer, up_id integer);