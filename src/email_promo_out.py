#!/usr/bin/python

import binascii
from time import sleep
from json import dumps
from kafka import KafkaConsumer
import json
import smtplib
import operator
import argparse

NL="\n"
TOPIC = "PromoOut"
KAFKA_HOST_PORT = "localhost:9092"
#HOST = '172.17.0.2:9092'

SMTP_HOST = "localhost"

sender      = 'msdemo@localhost'
receiver    = 'msdemo@localhost'


#get arguments
parser = argparse.ArgumentParser()
parser.add_argument("--smtp_host",default=SMTP_HOST,type=str,help="email host (ex: localhost)",required=False)
parser.add_argument("-k","--kafka_svr",default=KAFKA_HOST_PORT,type=str,help="Kafka host:port (ex: localhost:9092)",required=False)
parser.add_argument("-t","--topic",default=TOPIC,type=str,help="Kafka Consumer Topic (ex: "+TOPIC+")",required=False)
parser.add_argument("-s","--sender",default=sender,type=str,help="email sender (ex: "+sender+")",required=False)
parser.add_argument("-r","--receiver",default=receiver,type=str,help="email receiver (ex: "+receiver+")",required=False)
args = parser.parse_args()
print()
print("Runtime Arguments:")
argstring = " smtp_host:  "+args.smtp_host+NL+" kafka_svr:  "+args.kafka_svr+NL+" topic:      "+args.topic+NL+ \
            " sender:     "+args.sender+NL+   " receiver:   "+args.receiver
print(argstring)

receivers   = receiver.split(',')

BASE_EMAIL = """Subject: New Promo {EVENT_CODE}

*** New Event Registered ***

Event Code:     {EVENT_CODE}
Event Date:     {EVENT_DATE}
Description:    {EVENT_DESC}

Event Promotion Candidates within {EVENT_MILES} miles.

{EVENT_CANDIDATES}

"""

consumer = KafkaConsumer( args.topic ,bootstrap_servers=args.kafka_svr)

print( "Connected to KAFKA on:" , args.topic , " at " , args.kafka_svr , "...")

headers1 = ["State" , "City" , "Miles From" , "Promo"]
headers2 = ["" , "" , "Event" , "Candidates"]

for message in consumer:
    print( message )
    msg_string = message.value.decode("utf-8")

    print ("\n\n\n\n")
    suspects = json.loads(msg_string)
    suspects.sort(key=operator.itemgetter('miles_from_event'))

    event_code = suspects[0].get("event_code")
    event_date = suspects[0].get("event_date")
    event_desc = suspects[0].get("event_desc")

    print("EVENT CODE:" , event_code, ", EVENT DATE: " , event_date )
    print("EVENT DESC:" , event_desc )
    print()

    city_list = ""
    miles = "0"
    grid = []
    grid.append(headers1)
    grid.append(headers2)
    for city_dict in suspects:
        #print(city_dict)
        row = [city_dict.get("state"), city_dict.get("city"), str(city_dict.get("miles_from_event")), \
                '{:,}'.format(city_dict.get("promo_candidates"))]  
        grid.append(row)
        miles = row[2]

    col_width = max(len(word) for row in grid for word in row) + 2  # padding
    widths = [10,col_width,15,10]
    for row in grid:
        city_list = city_list + row[0].ljust(widths[0]) + row[1].ljust(widths[1]) + row[2].ljust(widths[2]) + row[3].rjust(widths[3]) + "\n"

    emsg = BASE_EMAIL.replace("{EVENT_CODE}",event_code).replace("{EVENT_CANDIDATES}",city_list).replace("{EVENT_DATE}",event_date).replace("{EVENT_DESC}",event_desc).replace("{EVENT_MILES}",miles)
    print(emsg)

    try:
        smtpObj = smtplib.SMTP(args.smtp_host)
        smtpObj.sendmail(sender, receivers, emsg)         
        print("Successfully sent email")
    except smtplib.SMTPException:
        print("Error: unable to send email")
