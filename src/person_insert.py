#!/bin/python3

import time
import multiprocessing
import csv
import glob
import sys
import argparse
import traceback

from memsql.common import database

ERROR = "***ERROR***"
DEBUG = False
NL = "\n"
# 3-node cluster - aggs = "172.31.57.177:3306,172.31.59.251:3306,172.31.53.236:3306"

DEFAULT_FILES="/tmp/files/person/20M/csv48/person_*.csv"

#get arguments
parser = argparse.ArgumentParser()
#parser.add_argument("-a","--aggs",default='127.0.0.1:3306',type=str,help="comma separated host:port list (ex: host1:3306,host2:3306)",required=False)
parser.add_argument("-a","--aggs",default='172.31.57.177:3306,172.31.59.251:3306,172.31.53.236:3306',type=str,help="comma separated host:port list (ex: host1:3306,host2:3306)",required=False)
parser.add_argument("-f","--files",default=DEFAULT_FILES,type=str,help="filenames to read and insert to DB (ex: '"+DEFAULT_FILES+"')",required=False)
parser.add_argument("-b","--batch_size",default=2000,type=int,help="number of records to include in each batch insert (ex: 2000)",required=False)
parser.add_argument("-u","--user",default='root',type=str,help="database user (ex: root)",required=False)
parser.add_argument("-p","--password",default='defaultpw',type=str,help="database password",required=False)
parser.add_argument("-d","--database",default='msdemo',type=str,help="database name (ex: msdemo)",required=False)
parser.add_argument("-t","--table",default='person',type=str,help="table to insert (ex: person or person_rs)",required=False)
args = parser.parse_args()
print()
print("Runtime Arguments:")
pwstr = "*" * len(args.password)
argstring = " aggs:       "+args.aggs+NL+" files:      "+args.files+NL+" batch_size: "+str(args.batch_size)+NL+" database:   "+args.database+NL+ \
            " table:      "+args.table+NL+" user:       "+args.user+NL+" password:   "+pwstr
print(argstring)

QRY_INSERT =  'insert into '+args.table+' ( personid, first_name, last_name, city, state, zip, ' \
                'likes_sports, likes_theatre, likes_concerts, likes_vegas, likes_cruises, likes_travel ) values '
QRY_VALUES = '("%s","%s","%s","%s","%s","%s",%s,%s,%s,%s,%s,%s) '

hosts = []
ports = []

# queue used to pass data from separate sub-processes back to parent process
q = multiprocessing.Queue()

def get_connection(db=args.database, thread_id=0):
    """ Returns a new connection to the database. """
    c_options = {'auth_plugin':'mysql_native_password'}
    
    # cycle through hosts to load connections across different aggregators
    host = hosts[ thread_id % len(hosts) ]
    port = ports[ thread_id % len(ports) ]
    if DEBUG:
        print( "Connection Host: " , host )
    return database.connect(host=host, port=port, user=args.user, password=args.password, database=db, options=c_options)


def insert_process( csvfile, pworkers, pworker, tqueue ):
    """ A sub-process which constructs and executes batch inserts in a loop until all rows from file are exhausted """

    if DEBUG:
        print("Init Separate Process: ",pworker)

    conn = get_connection(thread_id=pworker)

    thread_start = time.time()
    insert_count = 0
    insert_time = 0.0

    f = open(csvfile,"rt", newline='\n')
    myreader = csv.reader(f, delimiter=',' , quotechar='"')
    # skip first row
    next(myreader)

    values = ""
    for row in myreader:
        insert_count = insert_count + 1
        if len(values) == 0:
            values = QRY_VALUES % tuple(row)
        else:
            values = values + "," + QRY_VALUES % tuple(row)
        
        # execute insert on batch boundary
        if insert_count % args.batch_size == 0:
            qry_text = QRY_INSERT + values + ";"
            values = ""
            if DEBUG:
                print(qry_text)
            try:
                resp = conn.execute(qry_text)
            except:
                e = sys.exc_info()[0]
                print(ERROR, e)


    # at end of file - insert any remaining partial batch
    if len(values) > 0:
        qry_text = QRY_INSERT + values + ";"
        values = ""
        if DEBUG:
            print(qry_text)
        try:
            resp = conn.execute(qry_text)
        except:
            e = sys.exc_info()[0]
            print(ERROR, e)

    thread_end = time.time()
    duration = round(thread_end - thread_start,4)
    
    # push count and duration result for this thread to queue so results can be aggregated
    tqueue.put( str(insert_count)+"|"+str(duration) )

    qry_rate = str(int(insert_count / duration))
    prline = "Stats for file: " + csvfile +" - Inserts: " + str(insert_count) +", Duration (secs): " + str(round(duration,4)) + \
                ", Rate: " + qry_rate
    print(prline)

    # close connection at end of run
    conn.close()


def run_benchmark( csvfiles ):

    start = time.time()
    # print banner
    print()
    print( "="*60 )
    print("Benchmark Run - Db:", args.database , ", Table:" , args.table , ", Files = ", len(csvfiles) )
    print( "="*60 )
    print()
    
    """ Run a set of workers and record their performance. """
    workers = [ multiprocessing.Process(target=insert_process, 
                args=( csvfiles[workernum] , len(csvfiles) , workernum ,q )) for workernum in range( len(csvfiles) ) ]
    
    start = time.time()
    for worker in workers:
        worker.start()
    
    # wait for worker threads to complete
    [ worker.join() for worker in workers ]

    end = time.time()
    tot_inserts = 0
    tot_duration = 0    
    while not(q.empty()):
        q_msg = q.get_nowait()
        t_insertcount = int(q_msg.split("|")[0])
        t_insertsecs = float(q_msg.split("|")[1])

        tot_inserts = tot_inserts + t_insertcount
        tot_duration = max( tot_duration , t_insertsecs)

    print( "="*60 )
    print("Table:           ", args.table)
    print("Start Time:      ", round(start,3))
    print("End Time:        ", round(end,3))
    print("Duration (secs): ", round(end - start,3))
    rate = tot_inserts / tot_duration

    print( format(tot_inserts,',d') , "records inserted at avg. rate" , format(int(rate),',d') , "rows per sec")
    print( "="*60 )


def config_extract_aggs( p_aggs , p_hosts , p_ports ):
    """
        extract aggregator host & ports to use in connections from aggs arg passed in
    """
    aggs = p_aggs.split(",")
    for agg in aggs:
        p_hosts.append( agg.split(":")[0])
        p_ports.append( agg.split(":")[1])
    if len(aggs) == 0:
        raise Exception(ERROR+" - Invalid aggs configuration - expect 'aggs = host:port,host:port' type configuration")


def cleanup():
    """ Do any required cleanup here """

if __name__ == '__main__':

    try:

        config_extract_aggs( args.aggs , hosts , ports )    # get aggregator host & ports to use in connections
        csvfiles = glob.glob( args.files )                  # get csv filenames based on file pattern provided
        if len(csvfiles) > 0:
            run_benchmark( csvfiles )
        else:
            print("No files matching pattern ",args.files )
    except KeyboardInterrupt:
        print("Interrupted... exiting...")
    except Exception as e:
        print(ERROR, "Exiting because of Exception...")
        traceback.print_exc()
    finally:
        cleanup()  
