DROP PROCEDURE NEW_PROMO_EVENT;

DELIMITER //

CREATE OR REPLACE PROCEDURE NEW_PROMO_EVENT ( 
    batch QUERY(event_code varchar(50) not null,
                event_desc text,
                event_date date,
                event_zip varchar(10),
                promo_radius_miles integer,
                cat_sports boolean,
                cat_theatre boolean,
                cat_concerts boolean,
                cat_vegas boolean,
                cat_cruises boolean,
                cat_travel boolean ))
AS
DECLARE
    qry QUERY(  event_code varchar(50) not null,
                event_desc text,
                event_date date,
                event_zip varchar(10),
                promo_radius_miles integer,
                cat_sports boolean,
                cat_theatre boolean,
                cat_concerts boolean,
                cat_vegas boolean,
                cat_cruises boolean,
                cat_travel boolean  ) = 
            select event_code,
                event_desc,
                event_date,
                event_zip,
                promo_radius_miles,
                cat_sports,
                cat_theatre,
                cat_concerts,
                cat_vegas,
                cat_cruises,
                cat_travel
            from batch;
    promo_array ARRAY( RECORD(
                event_code varchar(50) not null,
                event_desc text,
                event_date date,
                event_zip varchar(10),
                promo_radius_miles integer,
                cat_sports boolean,
                cat_theatre boolean,
                cat_concerts boolean,
                cat_vegas boolean,
                cat_cruises boolean,
                cat_travel boolean 
            ) );
    baseQuery TEXT = 
"WITH temp AS ( 
    select t1.event_code, t1.event_date, t1.event_desc, t1.state, t1.city, t1.miles_from_event, count(1) as promo_candidates
    from person p
    inner join 
    (
        select pe.event_code, pe.event_date, pe.event_desc, c.city, c.state, c.zip, 
                round(geography_distance(c.location,c2.location)/1600) as miles_from_event
        from city_state c, city_state c2,
        (select * from promo_event where event_code='||EVENT_CODE||' limit 1) pe
        where c2.zip = pe.event_zip
        and geography_within_distance(c.location,c2.location, ||METERS||)
    ) t1
    on p.zip=t1.zip
    and p.city=t1.city 
    and p.state=t1.state 
    ||PREDICATE||
    group by t1.event_code, t1.event_date, t1.event_desc, t1.state, t1.city, t1.miles_from_event
    order by t1.miles_from_event
)
SELECT JSON_AGG(temp.*)
FROM temp
into KAFKA
'3.236.47.229:9092/PromoOut'";

    currentQuery TEXT;
    pred_string TEXT;
    likes_array ARRAY(boolean);
    pred_array ARRAY(TEXT) = ["p.likes_sports = 1" , "p.likes_theatre = 1" , "p.likes_concerts = 1" , "p.likes_vegas = 1" , "p.likes_cruises = 1" , "p.likes_travel = 1"];

BEGIN
    -- insert to promo_event table
    -- then evaluate to determine number of promo candidates

    INSERT INTO promo_event( event_code,event_desc,event_date,event_zip,promo_radius_miles,
                cat_sports,cat_theatre,cat_concerts,cat_vegas,cat_cruises,cat_travel)
      SELECT event_code,event_desc,event_date,event_zip,promo_radius_miles,
            cat_sports,cat_theatre,cat_concerts,cat_vegas,cat_cruises,cat_travel
      FROM batch;

    promo_array = COLLECT( qry );
    FOR x in promo_array LOOP
        -- iterate over promos, store them, 
        -- and select into KAFKA the number of potential promo candidates based on defined radius & likes
        -- send as JSON, with results of all locations & candidates in a single KAFKA message
        pred_string = "";
        likes_array = CREATE_ARRAY(6);
        likes_array = [x.cat_sports , x.cat_theatre, x.cat_concerts, x.cat_vegas , x.cat_cruises , x.cat_travel];
        FOR cindex IN 0 .. LENGTH(likes_array) - 1 LOOP
            IF likes_array[cindex] = 1 then
                if LENGTH(pred_string) = 0 THEN
                    pred_string = concat("where " , pred_array[cindex] , " ");
                ELSE
                    pred_string = concat("and " , pred_array[cindex] , " ");  
                END IF;
            END IF;
        END LOOP;
        currentQuery = REPLACE( REPLACE( REPLACE( baseQuery , "||EVENT_CODE||" , x.event_code ) 
                                        , "||METERS||" , x.promo_radius_miles * 1600 )
                                        , "||PREDICATE||" , pred_string );
        INSERT INTO info(msg) values(currentQuery);

        EXECUTE IMMEDIATE currentQuery;
    END LOOP;

    INSERT INTO info(msg) select concat(count(1) , ' new Promo Events (promo_event) created and evaluated') from batch;

END //

DELIMITER ;


CREATE PIPELINE `new_promo_pl`
AS LOAD DATA KAFKA '127.0.0.1/PromoIn'
BATCH_INTERVAL 2500
INTO PROCEDURE `NEW_PROMO_EVENT`
FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '\\'
LINES TERMINATED BY '\n' STARTING BY ''
(
    `event_code`,
    `event_desc`,
    `event_date`,
    `event_zip`,
    `promo_radius_miles`,
    `cat_sports`,
    `cat_theatre`,
    `cat_concerts`,
    `cat_vegas`,
    `cat_cruises`,
    `cat_travel`
);