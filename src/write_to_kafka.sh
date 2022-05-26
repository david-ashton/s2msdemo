#!/bin/bash

CSVFILE=/tmp/files/person/20M/csv1/person_0.csv
python spool_file.py --file $CSVFILE --batch_count 100000 --interval 1 | kafkacat -b localhost:9092 -P -t person2 & 

echo "Started Publishing File $CSVFILE to Kafka Topic PromoIn..."
wait
