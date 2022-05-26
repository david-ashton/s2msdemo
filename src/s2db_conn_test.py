import argparse

from memsql.common import database

#get arguments
parser = argparse.ArgumentParser()
parser.add_argument("--host",default='127.0.0.1',type=str,help="db aggregator host (ex: 127.0.0.1)",required=False)
parser.add_argument("--port",default=3306,type=int,help="db aggregator port (ex: 3306)",required=False)
parser.add_argument("-u","--user",default='root',type=str,help="database user (ex: root)",required=False)
parser.add_argument("-p","--password",default='defaultpw',type=str,help="database password",required=False)
args = parser.parse_args()

NL="\n"
print()
print("Runtime Arguments:")
pwstr = "*" * len(args.password)
argstring = " host:       "+args.host+NL+" port:       "+str(args.port)+NL+" user:       "+args.user+NL+" password:   "+pwstr
print(argstring)


def get_connection():
    """ Returns a new connection to the database. """
    c_options = {'auth_plugin':'mysql_native_password'}
    
    return database.connect(host=args.host, port=args.port, user=args.user, password=args.password, options=c_options)

print()
print(get_connection().query("show databases"))
print()