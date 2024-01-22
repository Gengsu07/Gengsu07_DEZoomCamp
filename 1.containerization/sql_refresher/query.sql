SELECT 
	t.lpep_pickup_datetime,
	t.lpep_dropoff_datetime,
	t.total_amount,
	concat(zpu.borough,'-',zpu.zone) AS "pickup_loc",
	concat(zdo.borough,'-',zdo.zone) AS "dropoff_loc"
FROM 
	taxi_data t,
	zone zpu,
	zone zdo
WHERE
	t."PULocationID" = zpu.locationid and 
	t."DOLocationID" = zdo.locationid