-- считаем дисциплины без разметки
select count (distinct discipline_code) from dds.wp_markup wm 
left join dds.wp_up wu 
on wu.wp_id = wm.id 
where (prerequisites = '[]' or outcomes = '[]') and  wu.up_id in 
(select id from dds.up u where u.selection_year = '2022')
-- id дисциплин для обновления разметки
select distinct discipline_code from dds.wp_markup wm 
left join dds.wp_up wu 
on wu.wp_id = wm.id 
where (prerequisites = '[]' or outcomes = '[]') and  wu.up_id in 
(select id from dds.up u where u.selection_year = '2022')
and wm.id not in 
(select wp_id from dds.wp where wp_status = 4)