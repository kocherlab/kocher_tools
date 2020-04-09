import os
import sys
import argparse
import logging
import sqlite3

from kocher_tools.logger import startLogger, logArgs
from kocher_tools.config_file import readConfig
from kocher_tools.database import createTable2

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
db_config_data = readConfig(db_args.yaml)

# Connect to the sqlite database
sqlite_connection = sqlite3.connect(db_config_data.sql_database)

# Create the cursor
cursor = sqlite_connection.cursor()

# Loop the tables
for table in db_config_data:

	# Create the table
	createTable2(sqlite_file, table.name, table.assignment_str)

# Commit any changes
sqlite_connection.commit()

# Close the connection
cursor.close()