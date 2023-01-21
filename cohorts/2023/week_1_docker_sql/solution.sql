-- Quesiton 3
select count(*) from yellow_taxi_strips_2019 t
where t.lpep_pickup_datetime::date >= '2019-01-15 00:00:00'
and t.lpep_dropoff_datetime::date <= '2019-01-15 23:59:59'

-- Question 4
select t.lpep_pickup_datetime from yellow_taxi_strips_2019 t
order by t.trip_distance desc
limit 1

-- Question 5
select t.passenger_count, count(*) from yellow_taxi_strips_2019 t
where t.lpep_pickup_datetime::date = '2019-01-01' and (t.passenger_count = 2 or t.passenger_count = 3)
group by t.passenger_count

-- Question 6
select t."DOLocationID", dozone."Zone" from yellow_taxi_strips_2019 t
left join zones as puzone on t."PULocationID" = puzone."LocationID"
left join zones as dozone on t."DOLocationID" = dozone."LocationID"
where puzone."Zone" like '%Astoria%'
order by t.tip_amount desc
limit 1
