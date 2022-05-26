import csv
import random

#pylint: disable=no-member

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
    in_city = p_inrow[0]
    in_state = p_inrow[1]
    in_state_name = p_inrow[2]
    in_county = p_inrow[3]
    in_lat = p_inrow[4]
    in_lng = p_inrow[5]
    in_pop = int(p_inrow[6])
    in_zip = p_inrow[7]
    
    prev_state_pop = p_state.get(KEY_STATE_POP)
    p_state[KEY_STATE_POP] = prev_state_pop + in_pop
    p_state[KEY_CITY_ZIP].append( in_city+"|"+in_zip )
    p_state[KEY_CITY_POP].append(prev_state_pop + in_pop)


def load_city_file():

    f_in = open(CITIES_FN, "r", newline='\n')
    reader = csv.reader( f_in )
    for inrow in reader:
        # "city" , "state" , "state_name" , "county" , "lat" , "lng" , "population" , "zip"
        """
        in_city = inrow[0]
        in_state = inrow[1]
        in_state_name = inrow[2]
        in_county = inrow[3]
        in_lat = inrow[4]
        in_lng = inrow[5]
        in_pop = inrow[6]
        in_zips = inrow[7]
        """
        if inrow[0] != "city":
            state_dict = get_or_create_state(inrow[1])
            add_city_to_state( state_dict , inrow )
    
    #print(states)

def rand_state( ):
    global STATE, STATE_PC
    rand_pc = random.randint(0,1001)/10
    lower = -1
    for statenum in range(len(STATE)):
        if rand_pc > lower and rand_pc <= STATE_PC[statenum]:
            return STATE[statenum]


def get_random_city():
    global states

    l_state = rand_state()
    state_dict = get_or_create_state(l_state)
    rand_pop = random.randint(0,state_dict.get(KEY_STATE_POP))
    lower = -1
    city_pops = state_dict.get(KEY_CITY_POP)
    city_zip = state_dict.get(KEY_CITY_ZIP)
    for citynum in range(len(city_pops)):
        if rand_pop > lower and rand_pop <= city_pops[citynum]:
            return [l_state , city_zip[citynum].split("|")[0] , city_zip[citynum].split("|")[1] ]

if __name__ == '__main__':

    # load cities
    load_city_file()
    for i in range(1000):
        print(get_random_city())
    # test random city selector