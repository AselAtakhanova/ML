/*задание1*/

select city, count(airport_code)
from airports a
group by city
having count(airport_code)>1

/*задание2*/

select distinct airport_name
from airports a
	join flights f on f.departure_airport = a.airport_code
	join aircrafts a2 on a2.aircraft_code =f.aircraft_code
where a2."range" = (select max(a3.range) from aircrafts a3)
order by airport_name

4.3
/*задание3*/
select f.flight_no, f.actual_departure - f.scheduled_departure as Задержка
from flights f
where f.actual_departure is not null
and f.scheduled_departure is not null
order by Задержка desc
limit 10

4.4
/*задание4 рассматриваем один из кейсов - открыта регистрация на рейс и еще не завершена*/

select count(*)
from(
	select t.book_ref 
	from tickets t 
	join tickets_flights tf using (ticket_no)
	join flights f using (flight_id)
	where f.status = 'On Time'
	except
	select t2.book_ref
	from tickets t2
	join boarding_passes bp using (ticket_no)
	group by t2.book_ref ) t
	
/*задание5*/
select f.flight_no, t1.col_of_passes, t2.maximum_seats,
(t2.maximum_seats-t1.col_of_passes)*100/ t2.maximum_seats as percentage,
sum(t1.col_of_passes
	over(participation by f.departure_airport
	order by f.actual_departure) as col_of_parrengers
from flights f
join (
	select bp.flight_id, MAX(bp.boarding_no) as col_of_passes 
	from boarding_passes bp
	group by bp.flight.id) t1 using(flight_id)
join (
select aircraft_code, count(*) as maximum_seats
from seats s 
group by aircraft_code) t2 using (aircraft_code)


/*задание6*/

    select distinct a.model as aircraft_model,
round(
count(*) over (partition by aircraft code)* 100/count(*) over(),0) as percentage
from flights f
inner join aircrafts a using (aircraft_code)

/*задание7*/
with cte_b as (
select a.city as city_out, a2.city as city_in, MIN(tf.amount)
from flights f
	inner join airports a on a.airport_code=f.departure_airport
	inner join airports a2 on a2.airport_code=f.arrival_airport
	inner join tickets_flights tf using (flight_id)
where tf.fare_conditions = "Business"
group by city_out, city_in
),
cte_e as (
select a3.city as city_out, a4.city as city_in, MAX(tf2.amount)
from flights f2
	join airports a3 on a3.airport_code=f2.departure_airport
	join airports a4 on a4.airport_code=f2.arrival_airport
	join tickets_flights tf2 using (flight_id)
where tf2.fare_conditions = "Economy"
group by city_out, city_in
)
select b.city_in as Gorod, count(*) as Kolichestvo
from cte_b b, cte_e e
where b.city_in=e.city_in
and b.city_out=e.city_out
and b.min < e.max
group by b.city_in


/*задание8*/
select a.city as Gorod, a2.city as Gorod_2
from (select distinct city from airports) a
cross join (select distinct city from airports) a2 
where a.city !=a2.city 
except
select a3.city, a4.city
from flights f
inner join airports a3 on a3.airport_code=f.departure_airport
	inner join airports a4 on a4.airport_code=f.arrival_airport
group by a3.city, a4.city
