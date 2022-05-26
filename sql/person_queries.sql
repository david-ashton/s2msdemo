-- totals by state
select  state, 
        count(1) as tot_users, 
        sum(likes_sports) as like_sports, 
        sum(likes_vegas) as like_vegas, 
        sum(likes_theatre) as like_theatre, 
        sum(likes_concerts) as like_concerts,
        sum(likes_cruises) as like_cruises,
        sum(likes_travel) as like_travel 
from person
group by state;

-- totals by state for rowstore
select  state, 
        count(1) as tot_users, 
        sum(likes_sports) as like_sports, 
        sum(likes_vegas) as like_vegas, 
        sum(likes_theatre) as like_theatre, 
        sum(likes_concerts) as like_concerts,
        sum(likes_cruises) as like_cruises,
        sum(likes_travel) as like_travel 
from person_rs
group by state;




-- totals by state & city for columnstore
select  state, city,
        count(1) as tot_users, 
        sum(likes_sports) as like_sports, 
        sum(likes_vegas) as like_vegas, 
        sum(likes_theatre) as like_theatre, 
        sum(likes_concerts) as like_concerts,
        sum(likes_cruises) as like_cruises,
        sum(likes_travel) as like_travel 
from person
group by state, city;

-- totals by state & city for rowstore
select  state, city,
        sum(likes_sports) as like_sports, 
        sum(likes_vegas) as like_vegas, 
        sum(likes_theatre) as like_theatre, 
        sum(likes_concerts) as like_concerts,
        sum(likes_cruises) as like_cruises,
        sum(likes_travel) as like_travel 
from person_rs
group by state, city;



-- totals by state for specific state
select  state, 
        sum(likes_sports) as like_sports, 
        sum(likes_vegas) as like_vegas, 
        sum(likes_theatre) as like_theatre, 
        sum(likes_concerts) as like_concerts,
        sum(likes_cruises) as like_cruises,
        sum(likes_travel) as like_travel 
from person
where state = 'CA' 
group by state;

-- totals by state for specific state
select  state, 
        sum(likes_sports) as like_sports, 
        sum(likes_vegas) as like_vegas, 
        sum(likes_theatre) as like_theatre, 
        sum(likes_concerts) as like_concerts,
        sum(likes_cruises) as like_cruises,
        sum(likes_travel) as like_travel 
from person_rs
where state = 'CA' 
group by state;




-- totals by state for list of states
select  state, city,
        sum(likes_sports) as like_sports, 
        sum(likes_vegas) as like_vegas, 
        sum(likes_theatre) as like_theatre, 
        sum(likes_concerts) as like_concerts,
        sum(likes_cruises) as like_cruises,
        sum(likes_travel) as like_travel 
from person
where state in ('NY', 'CA', 'GA', 'WA') 
group by state, city;

-- totals by state for list of states
select  state, city, 
        sum(likes_sports) as like_sports, 
        sum(likes_vegas) as like_vegas, 
        sum(likes_theatre) as like_theatre, 
        sum(likes_concerts) as like_concerts,
        sum(likes_cruises) as like_cruises,
        sum(likes_travel) as like_travel 
from person_rs
where state in ('NY', 'CA', 'GA', 'WA') 
group by state, city;





-- totals by state for specific states and interest
select  state, 
        count(1) as tot_users, 
        sum(likes_sports) as like_sports
from person
where state in ('NY', 'CA', 'GA', 'WA') 
group by state;


-- total sport lovers by state for a given state and neighboring states
select p.state, sum(likes_sports) as sport_lovers, s.degree 
from    person p, 
        states s 
where p.state=s.next_state 
and s.state='GA'
group by p.state;


-- cities within 50 miles (80 kms) of Dunwoody GA - ZIP 30338
select * from (
select c.city, c.state, c.zip, round(geography_distance(c.location,c2.location)/1600) as miles
from city_state c, city_state c2
where c2.zip = '30338'
and geography_within_distance(c.location,c2.location, 80000)) t1
order by t1.miles;



-- promo candidates for people who like sport within 50 miles of event zipcode
select t1.state, t1.city, t1.zip, t1.miles_from_event, sum(likes_sports) as sport_promo_cadidates
from person p
inner join 
(select c.city, c.state, c.zip, round(geography_distance(c.location,c2.location)/1600) as miles_from_event
from city_state c, city_state c2
where c2.zip = '30338'
and geography_within_distance(c.location,c2.location, 80000)) t1
on p.zip=t1.zip
group by t1.state, t1.city, t1.zip, t1.miles_from_event
order by t1.miles_from_event;

-- promo candidates for people who like sport within 50 miles of event zipcode
select t1.state, t1.city, t1.zip, t1.miles_from_event, count(1) as promo_candidates
from person p
inner join 
(select c.city, c.state, c.zip, round(geography_distance(c.location,c2.location)/1600) as miles_from_event
from city_state c, city_state c2
where c2.zip = '30338'
and geography_within_distance(c.location,c2.location, 80000)) t1
on p.zip=t1.zip
where p.likes_vegas=1
group by t1.state, t1.city, t1.zip, t1.miles_from_event
order by t1.miles_from_event;

-- promo candidates for people who like sport within 50 miles of event zipcode
select t1.state, t1.city, t1.zip, t1.miles_from_event, count(1) as promo_candidates
from person p
inner join 
(select c.city, c.state, c.zip, round(geography_distance(c.location,c2.location)/1600) as miles_from_event
from city_state c, city_state c2
where c2.zip = '30338'
and geography_within_distance(c.location,c2.location, 80000)) t1
on p.zip=t1.zip
where p.likes_vegas=1
group by t1.state, t1.city, t1.zip, t1.miles_from_event
order by t1.miles_from_event;


-- promo candidates for people who like sport within 50 miles of event zipcode
-- USING the promo_event row
select t1.event_code, t1.event_date, t1.event_desc, t1.state, t1.city, t1.miles_from_event, count(1) as promo_candidates
from person p
inner join 
(
select pe.event_code, pe.event_date, pe.event_desc, c.city, c.state, c.zip, 
        round(geography_distance(c.location,c2.location)/1600) as miles_from_event
from city_state c, city_state c2,
(select * from promo_event where event_code='E2' limit 1) pe
where c2.zip = pe.event_zip
and geography_within_distance(c.location,c2.location, 80000)
) t1
on p.zip=t1.zip
and p.city=t1.city 
and p.state=t1.state 
where p.
group by t1.event_code, t1.event_date, t1.event_desc, t1.state, t1.city, t1.miles_from_event
order by t1.miles_from_event;


-- grouped by CITY --> publish to KAFKA as JSON
WITH temp AS
  ( 
        select t1.event_code, t1.event_date, t1.event_desc, t1.state, t1.city, t1.miles_from_event, count(1) as promo_candidates
        from person_rs p
        inner join 
        (
        select pe.event_code, pe.event_date, pe.event_desc, c.city, c.state, c.zip, 
                round(geography_distance(c.location,c2.location)/1600) as miles_from_event
        from city_state c, city_state c2,
        (select * from promo_event where event_code='E2' limit 1) pe
        where c2.zip = pe.event_zip
        and geography_within_distance(c.location,c2.location, 160000)
        order by state, city, zip
        ) t1
        on p.zip=t1.zip
        and p.city=t1.city 
        and p.state=t1.state 
        where p.likes_sports = 1 
        group by t1.event_code, t1.event_date, t1.event_desc, t1.state, t1.city, t1.miles_from_event
        order by t1.miles_from_event
  )
SELECT JSON_AGG(temp.*)
FROM temp
into KAFKA
'3.238.247.139/test';
