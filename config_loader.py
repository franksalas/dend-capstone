import configparser
import os
config = configparser.ConfigParser()

abs_pathname = os.path.abspath("cap_config.cfg")
config.read_file(open(abs_pathname))  # config file
#abs_pathname = os.path.abspath("cap_config.cfg")
#config.read_file(open('cap_config.cfg'))  # config file

# global variables used
db_host = config['REDSHIFT']['DB_HOST']
db_name = config['REDSHIFT']['DB_NAME']
db_user = config['REDSHIFT']['DB_USER']
db_pass = config['REDSHIFT']['DB_PASS']
db_port = config['REDSHIFT']['DB_PORT']
