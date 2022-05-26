#!/bin/python3

import time
import csv
import sys
from json import dumps
from kafka import KafkaProducer
import argparse

#pylint: disable=no-member

NL="\n"
TOPIC = "PromoIn"
#KAFKA_SVR = "172.31.58.11:9092"
KAFKA_SVR = "127.0.0.1:9092"

SLEEP = 30
CSV_FILE = "/home/ec2-user/projects/s2msdemo/sql/data/promo_events.csv"

#get arguments
parser = argparse.ArgumentParser()
parser.add_argument("-k","--kafka_svr",default=KAFKA_SVR,type=str,help="Kafka host:port (ex: 127.0.0.1:9092)",required=False)
parser.add_argument("-t","--topic",default=TOPIC,type=str,help="Kafka Cunsumer Topic (ex: "+TOPIC+")",required=False)
parser.add_argument("-f","--file",default=CSV_FILE,type=str,help="promo_event csv file (ex: promo_events.csv)",required=False)
parser.add_argument("-i","--sleep_interval",default=SLEEP,type=int,help="sleep interval in seconds (ex: "+str(SLEEP)+")",required=False)
args = parser.parse_args()
print()
print("Runtime Arguments:")
argstring = " kafka_svr:  "+args.kafka_svr+NL+" topic:      "+args.topic+NL+ \
            " file:       "+args.file+NL+     " sleep_int:  "+str(args.sleep_interval)
print(argstring)

producer = KafkaProducer(bootstrap_servers=args.kafka_svr)
print( "Connected to KAFKA on:" , args.topic , " at " , args.kafka_svr , "...")

def publish_promos_to_kafka( ):

    f = open(args.file,"rt", newline='\n')
    myreader = csv.reader(f, delimiter=',' , quotechar='"')
    # skip first row
    next(myreader)

    values = ""
    for row in myreader:
        l_vals = tuple( row )
        l_promoid = row[0]
   
        l_key = bytes( "%s" % ( l_promoid ), "utf-8" )
        #l_vals = ( l_id , first_nm , last_nm , l_city , l_state , l_zip , l_likes_sports , l_likes_theatre , l_likes_concerts , l_likes_vegas )
        msg = bytes( "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % l_vals , "utf-8" )
        producer.send( args.topic, key=l_key, value=msg )
        print("Published to Topic: " , args.topic , ", Msg: ",msg)
        if args.sleep_interval != 0:
            time.sleep(args.sleep_interval)

if __name__ == '__main__':

    publish_promos_to_kafka()
