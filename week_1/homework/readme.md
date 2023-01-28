# Week1 Homework

## Answers to Homework Questions:
1. In `docker --help`, which tag has the text `"Write the image ID to the file"`?
- executed `docker build --help`
- Anser: `--iidfile string`

2. Run docker with the `python:3.9` image in an interactive mode and the entrypoint of bash. Now check the python modules that are installed ( use pip list). How many python packages/modules are installed?
- executed `docker run -it python:3.9 /bin/bash`
- in the docker interactive terminal, executed `pip list`
- output:
```
Package    Version
---------- -------
pip        22.0.4
setuptools 58.1.0
wheel      0.38.4

```
- Answer: 3

3. How many taxi trips were totally made on January 15?
- Anser: 20689
SQL:
```
select count(1)
from green_trip_data
where lpep_pickup_datetime::date = '2019-01-15';
```

4. Which was the day with the largest trip distance Use the pick up time for your calculations.
- Answer: 2019-01-15
SQL:
```
select * from
(
	select
		*
		,row_number() over (order by trip_distance desc) as trip_rank
	from green_trip_data
) as trips
where trips.trip_rank = 1;
```

5. In 2019-01-01 how many trips had 2 and 3 passengers?
- Answer: 2: 1282, 3: 254
SQL:
```
select passenger_count, count(passenger_count) TOTAL
from green_trip_data
where passenger_count in ('2', '3')
and lpep_pickup_datetime::date = '2019-01-01'
group by passenger_count;
```

6. For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip? We want the name of the zone, not the id.
- Answer: Long Island City/Queens Plaza
SQL:
```
with astoria_location_id as
(
	select locationid, zone
	from taxi_zone_data
	where zone like '%Astoria%'
),

top_tip as
(
	select gt.*, al.*,
		row_number() over (partition by al.zone order by gt.tip_amount desc) as tip_rank
	from green_trip_data gt
	inner join astoria_location_id al
	on gt.pulocationid = al.locationid
	where gt.tip_amount > 0
)

select tt.zone as pu_zone, tz.zone as do_zone, tt.tip_amount
from top_tip tt
inner join taxi_zone_data tz
on tt.dolocationid = tz.locationid
where tip_rank = 1;
```
