import csv
import random
import multiprocessing
from faker import Faker
import sys

#pylint: disable=no-member

FNAME_PREFIX = "../files/person_"
FNAME_SUFFIX = ".csv"

BOOL_IND_SPLIT = {'likesports' : 50 , 'liketheatre' : 10 , 'likeconcerts': 20 , 'likevegas' : 20, 'likecruises': 5 , 'liketravel' : 40 }


NUM_WORKERS = 48
ID_SEED = 1
RECORD_COUNT = 20000000
PROGRESS_COUNT = 10000
fake = Faker()

STATE = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
STATE_PC = [1.5, 1.8, 4, 4.9, 16.9, 18.7, 19.8, 20.1, 20.3, 26.8, 30, 30.4, 30.9, 34.8, 36.9, 37.9, 38.8, 40.2, 41.6, 42, 43.8, 45.9, 48.9, 50.6, 51.5, 53.4, 53.7, 54.3, 55.2, 55.6, 58.3, 58.9, 64.8, 68, 68.2, 71.8, 73, 74.3, 78.2, 78.5, 80.1, 80.4, 82.5, 91.3, 92.3, 92.6, 95.2, 97.5, 98, 99.8, 100]

CITIES_FN = "../sql/data/uscities_clean.csv"
KEY_STATE_POP = "state_pop"
KEY_CITY_ZIP = "city|zip"
KEY_CITY_POP = "city_cum_pop"

states = { }

def get_or_create_state( p_state ):
    global states

    if p_state not in states.keys():
        # do nothing - will just return value
        states[p_state] = { KEY_STATE_POP: 0,
                            KEY_CITY_ZIP: [],
                            KEY_CITY_POP: [] }
    return states.get(p_state)

def add_city_to_state( p_state , p_inrow ):
    # "city" , "state" , "state_name" , "county" , "lat" , "lng" , "population" , "zip"
    in_city = p_inrow[0]
    in_state = p_inrow[1]
    in_state_name = p_inrow[2]
    in_county = p_inrow[3]
    in_lat = p_inrow[4]
    in_lng = p_inrow[5]
    in_pop = int(p_inrow[6])
    in_zip = p_inrow[7]
    
    #load CITY|ZIP and cumulative population counts into the state dictionary

    prev_state_pop = p_state.get(KEY_STATE_POP)
    p_state[KEY_STATE_POP] = prev_state_pop + in_pop
    p_state[KEY_CITY_ZIP].append( in_city+"|"+in_zip )
    p_state[KEY_CITY_POP].append(prev_state_pop + in_pop)


def load_city_file():

    f_in = open(CITIES_FN, "r", newline='\n')
    reader = csv.reader( f_in )
    for inrow in reader:
        if inrow[0] != "city":
            state_dict = get_or_create_state(inrow[1])
            add_city_to_state( state_dict , inrow )
    
    # check states agains STATE
    for astate in STATE:
        if astate not in states.keys():
            # remove state from STATES
            print("removed "+astate+" from STATE list...")
            STATE.remove(astate)
        else:
            state_dict = states.get(astate)
            city_count = len(state_dict.get(KEY_CITY_ZIP))
            cum_count = state_dict.get(KEY_CITY_POP)[city_count-1]
            print("Loaded State:"+astate+", cities:"+str(city_count)+", pop:"+str(state_dict.get(KEY_STATE_POP))+"="+str(cum_count))


def rand_state( ):
    global STATE, STATE_PC
    rand_pc = random.randint(0,1000)/10
    lower = -1
    for statenum in range(len(STATE)):
        if rand_pc > lower and rand_pc <= STATE_PC[statenum]:
            #print( "'%s'" % (STATE[statenum]))
            return STATE[statenum]  


def rand_city():
    global states

    l_state = rand_state()
    """
    while True:
        l_state = rand_state()
        if l_state is not None:
            break
    """
    state_dict = get_or_create_state(l_state)
    city_pops = state_dict.get(KEY_CITY_POP)
    city_zip = state_dict.get(KEY_CITY_ZIP)     
    rand_pop = random.randint(0,state_dict.get(KEY_STATE_POP))
    lower = -1
    for citynum in range(len(city_pops)):
        if rand_pop > lower and rand_pop <= city_pops[citynum]:
            l_city = city_zip[citynum].split("|")[0]
            l_zip = city_zip[citynum].split("|")[1]
            return [l_state , l_city , l_zip ]
    # if for some reason we didn't get one picked just return first city for state
    print("*** catch all - "+l_state+", rand_pop="+rand_pop)
    return [l_state , city_zip[0].split("|")[0] , city_zip[0].split("|")[0]]



def rand_bool( ind_name ):
    global BOOL_IND_SPLIT
    bool_split = BOOL_IND_SPLIT.get(ind_name , 50)
    rand_pc = random.randint(0,1001)/10
    if rand_pc <= bool_split:
        return 1
    else:
        return 0


def create_csv_file( filenum , start_id , id_count ):
    fname = FNAME_PREFIX + str(filenum) + FNAME_SUFFIX
    with open(fname, 'w', newline='') as csvfile:
        fieldnames = ['personid', 'firstname', 'lastname', 'city','state', 'zip', 
                        'likesports', 'liketheatre','likeconcerts','likevegas','likecruises','liketravel']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for i in range(id_count):
            first_nm = fake.first_name()
            last_nm = fake.last_name()
            try:
                state_city_zip = rand_city()
                l_state = state_city_zip[0]
                l_city = state_city_zip[1]
                l_zip = state_city_zip[2]

                writer.writerow(
                    {
                        'personid': start_id + i,
                        'firstname': first_nm,
                        'lastname': last_nm,
                        'city': l_city,
                        'state': l_state,
                        'zip': l_zip,
                        'likesports': rand_bool('likesports'),
                        'liketheatre': rand_bool('liketheatre'),
                        'likeconcerts': rand_bool('likeconcerts'),
                        'likevegas': rand_bool('likevegas'),
                        'likecruises': rand_bool('likecruises'),
                        'liketravel': rand_bool('liketravel')
                    }        
                )
            except:
                print("ERROR - "+state_city_zip)
                e = sys.exc_info()[0]
                print(e)
            
            if (i % PROGRESS_COUNT) == 0:
                print(fname+": "+str(i)+"...")

if __name__ == '__main__':

    load_city_file()

    workers = []
    u_from = []
    u_count = []
    cum_count = 0

    for w in range(NUM_WORKERS):
        u_from.append( ID_SEED + cum_count )
        l_count = int( (RECORD_COUNT - cum_count) / (NUM_WORKERS - w) )
        u_count.append( l_count )
        cum_count += l_count

        workers.append( multiprocessing.Process(target=create_csv_file, args=(w, u_from[w], u_count[w])) )

    for worker in workers:
        worker.start()

    # wait for worker threads to complete
    [worker.join() for worker in workers]

    print()