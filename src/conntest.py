from memsql.common import database

HOST = '127.0.0.1'
PORT = 3306
USER = 'root'
PASSWORD = 'memsql'

def get_connection():
    """ Returns a new connection to the database. """
    c_options = {'auth_plugin':'mysql_native_password'}
    
    return database.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, options=c_options)

print()
print(get_connection().query("show databases"))
print()