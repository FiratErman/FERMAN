CREATE TABLE IF NOT EXISTS johnson_and_johnson.event_database_enhanced AS 
SELECT 
	t1.*,
    t2.release_date,
	t2.batch_id,
    CASE 
		WHEN t2.release_date<t1.date_created THEN "True" 
        ELSE "False"
    END AS release_before_event
FROM johnson_and_johnson.event_database t1
LEFT JOIN johnson_and_johnson.batches_table t2
ON t1.id = t2.id