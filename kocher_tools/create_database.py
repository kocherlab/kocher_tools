#!/usr/bin/env python
import os
import sys
import argparse
import logging
import getpass
import sqlite3

from kocher_tools.logger import startLogger, logArgs
from kocher_tools.config_file import ConfigDB
from kocher_tools.database import *

def confirmFile ():
	'''Custom action to confirm file exists'''
	class customAction(argparse.Action):
		def __call__(self, parser, args, value, option_string=None):
			if not os.path.isfile(value):
				raise IOError('%s not found' % value)
			setattr(args, self.dest, value)
	return customAction

# Define args
db_parser = argparse.ArgumentParser(formatter_class = argparse.ArgumentDefaultsHelpFormatter)
db_parser.add_argument('--yaml', help = "Database YAML config file", type = str, required = True, action = confirmFile())
db_parser.add_argument('--out-log', help = 'Filename of the log file', type = str, default = 'create_database.log')
db_args = db_parser.parse_args()

# Start a log file for this run
startLogger(log_filename = db_args.out_log)

# Log the arguments used
logArgs(db_args)

# Read in the YAML config file
db_config_data = ConfigDB.readConfig(db_args.yaml)

# Get the password, if not found
if db_config_data.passwd_required and not db_config_data.passwd:
	db_config_data.passwd = getpass.getpass("Password: ")
	
# Create the tables
sql_engine = createEngineFromConfig(db_config_data)
createAllFromConfig(db_config_data, sql_engine)