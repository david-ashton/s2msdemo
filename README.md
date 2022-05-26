# s2msdemo

Includes artifacts for SingleStore Demos.  
While the primary target is for demonstration of Managed Service (S2MS), demos are equally applicable for S2DB

## DB Artifacts

> ~/sql directory

- Table creation DDL
- code for demonstratiing various load options
- Promo Event Stored procdure code
- example demo queries

## Contextual Data

> ~/sql/data directory

Includes reference data sets including 
- location data like city/state/zip/lng-lat
- promo_event data fo demonstration of promo event processing pipeline

## Python Demonstration Scripts

> ~/src directory

Python demonstration scripts
- agg_query.py
    - script to demonstrate multi-processing QPS performance achievable for aggregate queries

- email_promo_out.py
    - receives promo candidate info from kafka topic published to by NEW_PROMO_EVENT Procedure and sends email

- kafka-commands.txt
    - cheat sheet of Kafka commands required to setup from kafka-related demo use-cases

- person_insert.py
    - multi-processing client for benchmarking bulk inserts to **person** or **person_rs** tables

- person_query.py
    - multi-processing client to test QPS achievable against either **person** or **person_rs** table with different levels of concurrency

- promos_kafka_pub.py
    - script for slowly publishing peomo_events to Kafka topic for demoing the promo-event use-case

- s2db_conn_test.py
    - script for testing s2db connectivity from python

- spool_file.py
    - script for spooling content from file at defined rate per period
    - used in conjunction with kafkacat to pipe data to Kafka topic for demo purposes

- write_to_kafka.sh
    - bash shell script that uses *spool_file.py* and *kafkacat* to pipe data to kafka topic from file


## Utility Scripts

> ~/utils directory

Including python script for generating randonized person test data-sets of varying sizes and number of data-files

## C# Demo Programs

> ~/csharp

C# equivalents of the Python demo programs in this project to demonstrate multi-threaded batch inserts to SingleStore from .NET / C#
Uses MySQL .Net connector, and includes:
 - S2dbPersonInsert - inserts from csv files with separate thread per csv file
 - S2dbPersonQuery - multi-threaded query benchmark program

 Note that the C# demo programs don't take command-line arguments at this time
 - so you'll need to modify and recompile to use different parameters for your specific test
 - modifying to use externally provided parameters is on the to-do list 

## Java Demo Programs

> ~/java

Java equivalents of the Python demo programs.  
This includes the project java source under the com.singlestore.s2dbdemo directory.
These use the mariadb JDBC drivers.
The java_run directory has the shell scripts to run from command line.
Properties files have the benchmark run parameters for each test program (Insert & Query).
