use msdemo;

-- LOAD FILE for city_state & states - FROM COMMAND LINE CLIENT with --local-infile option
-- ***************************************************************************************

load data local infile '/home/ec2-user/projects/s2msdemo/sql/data/uscities_clean.csv'
into table city_state
fields terminated by ','
ignore 1 lines;

load data local infile '/home/ec2-user/projects/s2msdemo/sql/data/states.csv'
into table states
fields terminated by ',';

-- LOAD FILE for city_state & states - VIA S3 PIPELINE
-- ***************************************************
-- create pipeline from S3
CREATE OR REPLACE PIPELINE city_state_pl
AS LOAD DATA S3 's3://dashton-test/s2msdemo/other_data/uscities_clean.csv'
CONFIG '{"region": "us-east-1"}'
SKIP DUPLICATE KEY ERRORS
INTO TABLE `city_state`
FIELDS TERMINATED BY ','
IGNORE 1 LINES;

CREATE OR REPLACE PIPELINE states_pl
AS LOAD DATA S3 's3://dashton-test/s2msdemo/other_data/states.csv'
CONFIG '{"region": "us-east-1"}'
SKIP DUPLICATE KEY ERRORS
INTO TABLE `states`
FIELDS TERMINATED BY ',';

test pipeline city_state_pl;
test pipeline states_pl;

start pipeline city_state_pl foreground;
start pipeline states_pl foreground;

drop pipeline city_state_pl;
drop pipeline states_pl;


-- LOAD FILE commands for person & person_rs
-- *****************************************

-- load commands for person table from csv file
load data local infile '/tmp/files/person/20M/csv1/person_0.csv'
into table person
fields terminated by ','
ignore 1 lines;

-- load commands for person table from csv file
load data local infile '/tmp/files/person/20M/csv1/person_0.csv'
SKIP DUPLICATE KEY ERRORS
into table person_rs
fields terminated by ','
ignore 1 lines;




-- FS PIPELINE commands for person & person_rs
-- *******************************************

-- load via pipeline from all files at location
CREATE OR REPLACE PIPELINE person_pl
AS LOAD DATA FS '/tmp/files/person/20M/csv24/person_*.csv'
INTO TABLE `person`
FIELDS TERMINATED BY ','
IGNORE 1 LINES;

-- load person_rs via pipeline from all files at location
CREATE OR REPLACE PIPELINE person_rs_pl
AS LOAD DATA FS '/tmp/files/person/1M/csv24/person_*.csv'
SKIP DUPLICATE KEY ERRORS
INTO TABLE `person_rs`
FIELDS TERMINATED BY ','
IGNORE 1 LINES;

-- find what files are queued up for pipeline
SELECT * FROM information_schema.PIPELINES_FILES;

-- test & start pipelines
TEST PIPELINE person_pl limit 100;
START PIPELINE person_pl FOREGROUND;

TEST PIPELINE person_rs_pl limit 100;
START PIPELINE person_rs_pl FOREGROUND;



-- ***********************
-- *                     *
-- * S3 Pipelines        *
-- *                     *
-- ***********************

-- S3 Pipelines for loading data from the appropriate S3 bucket & folders
-- all files gzipped
-- available folders for different number of person rows, and number of separate files
-- substitute appropriate one in the CREATE PIPELINE command for desired demo
-- personid ranges differ for the different number of rows
--      i.e. 20M load file personid values do not clash with 10M row files, etc..
-- as 1 file:
--      500k rows:  s3://dashton-test/s2msdemo/person/csv1/500k/person_*
--      1M rows:  s3://dashton-test/s2msdemo/person/csv1/1M/person_*
--      5M rows:  s3://dashton-test/s2msdemo/person/csv1/5M/person_*
--      10M rows:  s3://dashton-test/s2msdemo/person/csv1/10M/person_*
--      20M rows:  s3://dashton-test/s2msdemo/person/csv1/20M/person_*
-- as 8 files:
--      500k rows:  s3://dashton-test/s2msdemo/person/csv8/500k/person_*
--      1M rows:  s3://dashton-test/s2msdemo/person/csv8/1M/person_*
--      5M rows:  s3://dashton-test/s2msdemo/person/csv8/5M/person_*
--      10M rows:  s3://dashton-test/s2msdemo/person/csv8/10M/person_*
--      20M rows:  s3://dashton-test/s2msdemo/person/csv8/20M/person_*
-- as 24 files:
--      5M rows:  s3://dashton-test/s2msdemo/person/csv24/5M/person_*
--      10M rows:  s3://dashton-test/s2msdemo/person/csv24/10M/person_*
--      20M rows:  s3://dashton-test/s2msdemo/person/csv24/20M/person_*
-- as 48 files:
--      20M rows:  s3://dashton-test/s2msdemo/person/csv48/20M/person_*


drop pipeline person_pl_s3;
-- create pipeline from S3
CREATE OR REPLACE PIPELINE person_pl_s3
AS LOAD DATA S3 's3://dashton-test/s2msdemo/person/csv24/20M/person_*'
CONFIG '{"region": "us-east-1"}'
INTO TABLE `person`
FIELDS TERMINATED BY ','
IGNORE 1 LINES;


drop pipeline person_rs_pl_s3;
-- create pipeline from S3
CREATE OR REPLACE PIPELINE person_rs_pl_s3
AS LOAD DATA S3 's3://dashton-test/s2msdemo/person/csv24/20M/person_*'
CONFIG '{"region": "us-east-1"}'
SKIP DUPLICATE KEY ERRORS
INTO TABLE `person_rs`
FIELDS TERMINATED BY ','
IGNORE 1 LINES;


-- start & start pipelines
TEST PIPELINE person_pl_s3 limit 100;
START PIPELINE person_pl_s3 FOREGROUND;

TEST PIPELINE person_rs_pl_s3 limit 100;
START PIPELINE person_rs_pl_s3 FOREGROUND;




-- ***********************
-- *                     *
-- * KAFKA Pipelines     *
-- *                     *
-- ***********************

-- will need to change the kafka server/ip to that where KAFKA is running for your demo

-- Public: 3.239.115.49  Private:  172.31.58.11
-- 
CREATE PIPELINE `kafka_person_pl`
AS LOAD DATA KAFKA '3.239.115.49:9092/person'
INTO TABLE `person`
FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '\\'
LINES TERMINATED BY '\n' STARTING BY ''
(
    `personid`,
    `first_name`,
    `last_name`,
    `city`,
    `state`,
    `zip`,
    `likes_sports`,
    `likes_theatre`,
    `likes_concerts`,
    `likes_vegas`,
    `likes_cruises`,
    `likes_travel`
);


DELIMITER //

CREATE OR REPLACE PROCEDURE PERSON_INGEST1 ( batch QUERY(
                                personid BIGINT not null,
                                first_name varchar(100),
                                last_name varchar(100),
                                city varchar(50),
                                state varchar(2),
                                zip varchar(10),
                                likes_sports boolean,
                                likes_theatre boolean,
                                likes_concerts boolean,
                                likes_vegas boolean,
                                likes_cruises boolean,
                                likes_travel boolean ))
AS
BEGIN
    -- inserts from pipeline into 2 tables - person & person_rs

    INSERT INTO person( personid, first_name, last_name, city, state, zip,
                        likes_sports, likes_theatre, likes_concerts, likes_vegas, likes_cruises, likes_travel)
      SELECT personid, first_name, last_name, city, state, zip,
            likes_sports, likes_theatre, likes_concerts, likes_vegas, likes_cruises, likes_travel
      FROM batch;

    INSERT INTO person_rs( personid, first_name, last_name, city, state, zip,
                        likes_sports, likes_theatre, likes_concerts, likes_vegas, likes_cruises, likes_travel)
      SELECT personid, first_name, last_name, city, state, zip,
            likes_sports, likes_theatre, likes_concerts, likes_vegas, likes_cruises, likes_travel
      FROM batch;

    INSERT INTO info(msg) select concat(count(1) , ' rows inserted into person and person_rs') from batch;

END //

DELIMITER ;



-- KAFKA Pipeline for Person
CREATE PIPELINE `kafka_person_pl`
AS LOAD DATA KAFKA '3.235.144.165/person2'
BATCH_INTERVAL 2500
INTO PROCEDURE PERSON_INGEST1
FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '\\'
LINES TERMINATED BY '\n' STARTING BY ''
(
    `personid`,
    `first_name`,
    `last_name`,
    `city`,
    `state`,
    `zip`,
    `likes_sports`,
    `likes_theatre`,
    `likes_concerts`,
    `likes_vegas`,
    `likes_cruises`,
    `likes_travel`
);


/*
-- load via pipeline from all files at location
CREATE OR REPLACE PIPELINE person_pl
AS LOAD DATA FS '/tmp/files/person_*'
INTO TABLE `person`
FIELDS TERMINATED BY ','
IGNORE 1 LINES;



ECHO select t1.state, t1.city, t1.zip, t1.miles_from_event, count(1) as promo_candidates 
    from person p 
    inner join 
    (select c.city, c.state, c.zip, round(geography_distance(c.location,c2.location)/1600) as miles_from_event 
    from city_state c, city_state c2 
    where c2.zip = '||ZIP||' 
    and geography_within_distance(c.location,c2.location, ||METERS||)) t1 
    on p.zip=t1.zip 
    ||PREDICATE|| 
    group by t1.state, t1.city, t1.zip, t1.miles_from_event 
    order by t1.miles_from_event;
*/
