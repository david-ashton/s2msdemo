#!/bin/python3

import datetime
import time
import multiprocessing
import random
import sys
import argparse

from memsql.common import database

STATE = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
ERROR = "***ERROR***"
DEBUG = False
NL = "\n"
DEFAULT_AGGS = "172.31.57.177:3306,172.31.59.251:3306,172.31.53.236:3306"
#DEFAULT_AGGS = "127.0.0.1:3306"

DEFAULT_QRY = """select  state, 
        sum(likes_sports) as like_sports, 
        sum(likes_vegas) as like_vegas, 
        sum(likes_theatre) as like_theatre, 
        sum(likes_concerts) as like_concerts,
        sum(likes_cruises) as like_cruises,
        sum(likes_travel) as like_travel 
from {table}
where state = '{s}' 
group by state"""

DEFAULT_QRY = """select t1.event_code, t1.event_date, t1.event_desc, t1.state, t1.city, t1.miles_from_event, count(1) as promo_candidates
    from {table} p
    inner join 
    (
        select pe.event_code, pe.event_date, pe.event_desc, c.city, c.state, c.zip, 
                round(geography_distance(c.location,c2.location)/1600) as miles_from_event
        from city_state c, city_state c2,
        (select * from promo_event where event_code='E10003' limit 1) pe
        where c2.zip = pe.event_zip
        and geography_within_distance(c.location,c2.location, 300000)
        order by state, city, zip
    ) t1
    on p.zip=t1.zip
    and p.city=t1.city 
    and p.state=t1.state 
    where p.likes_sports = 1 
    group by t1.event_code, t1.event_date, t1.event_desc, t1.state, t1.city, t1.miles_from_event
    order by t1.miles_from_event"""


#get arguments
parser = argparse.ArgumentParser()
parser.add_argument("-a","--aggs",default=DEFAULT_AGGS,type=str,help="comma separated host:port list (ex: host1:3306,host2:3306)",required=False)
parser.add_argument("-u","--user",default='root',type=str,help="database user (ex: root)",required=False)
parser.add_argument("-p","--password",default='defaultpw',type=str,help="database password",required=False)
parser.add_argument("-d","--database",default='msdemo',type=str,help="database name (ex: msdemo)",required=False)
parser.add_argument("-t","--table",default='person',type=str,help="table to substitute [table] in QRY (ex: person or person_rs)",required=False)
parser.add_argument("-q","--query",default=DEFAULT_QRY,type=str,help="query string (ex: select count(1) from {table} where state={s})",required=False)
parser.add_argument("-i","--iterations",default=2000,type=int,help="query iterations in run (ex: 2000)",required=False)
parser.add_argument("--threads",default="10",type=str,help="comma separated list of threads to run (ex: 1,10,30)",required=False)
args = parser.parse_args()
print()
print("Runtime Arguments:")
pwstr = "*" * len(args.password)
argstring = " aggs:       "+args.aggs+NL+" database:   "+args.database+NL+" table:      "+args.table+NL+ \
            " threads:    "+args.threads+NL+" user:       "+args.user+NL+" password:   "+pwstr
print(argstring)

hosts = []
ports = []
threads_per_run = args.threads.split(",")
ids = []
ID_COUNT = 1000

# Query & QueryDesc   
QRY = args.query.replace("{table}" , args.table).replace("{s}","%s")

PAUSE_SECONDS = 3
PROGRESS_COUNT = 1000


# queue used to pass data from separate sub-processes back to parent process
q = multiprocessing.Queue()

def get_connection(db=args.database, thread_id=0):
    """ Returns a new connection to the database. """
    c_options = {'auth_plugin':'mysql_native_password'}
    
    # cycle through hosts to load connections across different aggregators
    host = hosts[ thread_id % len(hosts) ]
    port = ports[ thread_id % len(hosts) ]
    if DEBUG:
        print( "Connection Host: " , host )
    return database.connect(host=host, port=port, user=args.user, password=args.password, database=db, options=c_options)


def query_process( pheader, pworkers, pworker, qry_template, tqueue ):
    """ 
        A sub-process which constructs and executes queries in a loop until all rows from file are exhausted 
    """

    if DEBUG:
        print("Init Separate Process: ",pworker)

    conn = get_connection(thread_id=pworker)

    t_start = time.time()
    for qindex in range(args.iterations):
        #qry_string = qry_template % ( STATE[random.randint(0,len(STATE)-1)] )
        qry_string = qry_template
        conn.execute(qry_string)
        if DEBUG:
            if qindex % PROGRESS_COUNT == 0:
                print("Queried ",qindex,"...")
    t_end = time.time()

    avg_latency = 1000*(t_end-t_start) / args.iterations
    duration = round(t_end - t_start,4)
    qry_rate = args.iterations / duration

    # push duration result for this thread to queue
    tqueue.put( duration )

    prline = args.database + "," + pheader + "," + str(len(hosts)) + "," + \
             str(pworkers) + "," + str(round(avg_latency,5)) + "," + str(round(qry_rate,2))
    print(prline)

    # close connection at end of run
    conn.close()


def run_benchmark( header , qry , pworkers ):

    # print banner
    print()
    print( "="*70 )
    print( "Benchmark Run:", header , ", Threads:",pworkers )
    print( "Aggregators:" , len(hosts), ", DB:" , args.database )
    print( "Query:\n" , qry)
    print( "="*70 )
    print("Database,QueryType,AggregatorNodes,ConnectionThreads,AvgLatency_ms,QueriesPerSec")   

    """ Run a set of QueryWorkers and record their performance. """

    workers = [ multiprocessing.Process(target=query_process, args=(header, pworkers, workernum, qry ,q )) for workernum in range( pworkers ) ]
    start = time.time()
    for worker in workers:
        worker.start()
    
    # wait for worker threads to complete
    [ worker.join() for worker in workers ]

    durations = []
    latencies = []
    avg_latency_ms = 0.0
    while not(q.empty()):
        l_duration = q.get_nowait()
        durations.append( l_duration )
        latencies.append( l_duration*1000 / args.iterations )
        avg_latency_ms = sum(latencies) / len(latencies)

    end = time.time()
    tot_queries = pworkers * args.iterations
    rate =  round( tot_queries / (end - start) , 3)

    print( "="*70 )
    print("Start Time: ",round(start,3))
    print("End Time: ",round(end,3))
    print("Elapsed: ",round(end - start,3))

    print( str(tot_queries) , "total queries,", str(rate) , " QPS, Approx. Avg Latency (ms): " , str (round(avg_latency_ms,5)) )
    print( "="*70 )


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
    """ Cleanup the database this benchmark is using. """

if __name__ == '__main__':

    config_extract_aggs( args.aggs , hosts , ports )    # get aggregator host & ports to use in connections
    try:
        for threadcount in threads_per_run:
            run_benchmark( "query "+args.table+" by personid" , QRY , int(threadcount) )
            time.sleep( PAUSE_SECONDS )

    except KeyboardInterrupt:
        print("Interrupted... exiting...")
    finally:
        cleanup()    