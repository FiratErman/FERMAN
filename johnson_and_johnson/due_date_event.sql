CREATE TABLE IF NOT EXISTS johnson_and_johnson.due_date_per_supervisor_geo AS
SELECT DISTINCT * FROM 
(SELECT 
    t2.functional_area,
    t1.supervisor_leader,
    t1.site_name,
	t1.due_date
FROM johnson_and_johnson.event_database t1
LEFT JOIN johnson_and_johnson.labs t2
on t1.site_name=t2.site_name)t3