#!/bin/python3
import datetime
import time
import random
import csv

I_FN = "../sql/data/raw_uscities.csv"
O_FN = "../sql/data/uscities_clean.csv"

DEBUG = False

def extract_zips( ):

    f_in = open(I_FN, "r", newline='\n')
    reader = csv.reader( f_in )
    f_out = open(O_FN, "w")
    writer = csv.writer( f_out, lineterminator='\n' )

    for inrow in reader:
        # city,state_id,state_name,county_name,lat,lng,population,density,timezone,ranking,zip_count,zip_population,zips
        in_city = inrow[0]
        in_state_cd = inrow[1]
        in_state_name = inrow[2]
        in_county = inrow[3]
        in_lat = inrow[4]
        in_lng = inrow[5]
        in_pop = inrow[6]
        in_zipcount = inrow[10]
        in_zip_pop = inrow[11]
        in_zips = inrow[12]
        #print( inrow )

        zip_list = in_zips.split(' ')
        if in_city == "city":
            #print header
            writer.writerow( [ "city" , "state" , "state_name" , "county" , "lat" , "lng" , "population" , "zip" ] )
        else:
            for zipcd in zip_list:
                writer.writerow( [ in_city , in_state_cd , in_state_name , in_county , in_lat , in_lng , in_zip_pop , zipcd.strip() ] )
    f_out.flush()

def cleanup():
    """ Cleanup the database this benchmark is using. """

if __name__ == '__main__':

    try:
        extract_zips()
    except KeyboardInterrupt:
        print("Interrupted... exiting...")
    finally:
        cleanup()    