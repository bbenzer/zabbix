select XXX.* from 
(SELECT
table_schema as `Database`,
table_name AS `Table`,
round(((data_length + index_length) / 1024 / 1024 / 1024), 2) `SIZE_GB`
FROM information_schema.TABLES
WHERE 1=1
ORDER BY (data_length + index_length) DESC) XXX
where 1=1
and XXX.SIZE_GB > 0.002
ORDER BY XXX.SIZE_GB DESC;
