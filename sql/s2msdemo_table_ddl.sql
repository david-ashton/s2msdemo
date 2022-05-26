use msdemo;

drop table if exists person;
create table person (
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
    likes_travel boolean,
    key (personid) using clustered columnstore,
    shard key(personid),
    key (zip) using hash
);

drop table if exists person_rs;
create table person_rs (
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
    likes_travel boolean,
    primary key (personid),
    shard key (personid),
    index (zip) 
);


drop table if exists city_state;
create reference table city_state(
    city varchar(50) not null,
    state varchar(2) not null,
    state_name varchar(50),
    county varchar(50),
    lat decimal(7,4),
    lng decimal(7,4),
    population int,
    zip varchar(10),
    location AS GEOGRAPHY_POINT(lng,lat) PERSISTED GEOGRAPHYPOINT,
    primary key(state, city, zip),
    index(location),
    index (zip)
);

drop table if exists states;
create reference table states (
    state varchar(2) not null,
    next_state varchar(2) not null,
    degree integer,
    primary key (state, next_state)
);

drop table if exists promo_event;
create table promo_event (
    event_code varchar(50) not null,
    event_desc text,
    event_date date,
    event_zip varchar(10),
    promo_radius_miles integer,
    cat_sports boolean default 0,
    cat_theatre boolean default 0,
    cat_concerts boolean default 0,
    cat_vegas boolean default 0,
    cat_cruises boolean default 0,
    cat_travel boolean default 0,
    create_ts timestamp default CURRENT_TIMESTAMP,
    key (event_code)
);

drop table if exists info;
CREATE TABLE info (
  `msg` text CHARACTER SET utf8 COLLATE utf8_general_ci,
  `update_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP
);

/*

-- sample data for promo_event

INSERT INTO promo_event( event_code,event_desc,event_date,event_zip,promo_radius_miles,
            cat_sports,cat_theatre,cat_concerts,cat_vegas,cat_cruises,cat_travel)
VALUES ('E1','USA vs Germany Soccer','2021-03-01','30338',50,1,0,0,0,0,0);
INSERT INTO promo_event( event_code,event_desc,event_date,event_zip,promo_radius_miles,
            cat_sports,cat_theatre,cat_concerts,cat_vegas,cat_cruises,cat_travel)
VALUES ('E2','Hamilton in Vegas','2021-03-02','89109',100,0,1,0,1,0,0);
select * from promo_event;

*/


