#!/usr/bin/python

import time
import sys
import os
import argparse
from subprocess import Popen, PIPE

DEFAULT_FILE = "/tmp/files/person/20M/csv1/person_0.csv"

#get arguments
parser = argparse.ArgumentParser()
parser.add_argument("-f","--file",default=DEFAULT_FILE,type=str,help="filename and path to spool (ex: "+DEFAULT_FILE+")",required=False)
parser.add_argument("-b","--batch_count",default=1000,type=int,help="number of records to include in each batch (ex: 1000)",required=False)
parser.add_argument("-i","--interval",default=1,type=int,help="time interval between each batch (ex: 1)",required=False)
parser.add_argument('--print_file_info', dest='fileinfo', default=False, action='store_true',help="prints file size and time to spool before. Useful for debugging. Bad for spooling to kafka",required=False)
args = parser.parse_args()

def main():

    output_string = ""
    count = 0

    if args.fileinfo == True: 
        file_length = get_file_length()
        print("records in file: "+str(file_length) +"\nseconds to fully spool file:  "+str((file_length / args.batch_count )*args.interval))

    for line in open(args.file,'r'):
            output_string += str(line)
            count+=1
            if count % args.batch_count == 0:
                print_out(output_string)
                output_string = ""
                time.sleep(args.interval)
    print_out(output_string) # print remainder of batch when it is less than a full batch size

        
def print_out(output_string):
    sys.stdout.write(output_string)


def get_file_length():
    return int(os.popen('wc -l '+args.file+' | awk \'{print $1}\' ' ).readline())


if __name__== "__main__":
  main()
