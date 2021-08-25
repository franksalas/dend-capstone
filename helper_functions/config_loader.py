import configparser

config = configparser.ConfigParser()

# get aws keys from config file
config.read_file(open('cap_config.cfg'))  # config file


db_host = config['REDSHIFT']['DB_HOST']
db_name = config['REDSHIFT']['DB_NAME']
db_user = config['REDSHIFT']['DB_USER']
db_pass = config['REDSHIFT']['DB_PASS']
db_port = config['REDSHIFT']['DB_PORT']
