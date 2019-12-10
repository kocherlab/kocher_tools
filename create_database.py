import os
import sys
import sqlite3
import yaml
import argparse
from config_file import readConfig
from database import createTable

# Define args
db_parser = argparse.ArgumentParser(formatter_class = argparse.ArgumentDefaultsHelpFormatter)
db_parser.add_argument('--yaml', help = "Database YAML config file", type = str, default = "kocherDB.yml")
db_args = db_parser.parse_args()

# Read in the YAML config file
db_config_data = readConfig(db_args.yaml)

# Get filename from config file
sqlite_file = db_config_data.sql_database

# Loop the tables
for table in db_config_data:

	# Create the table
	createTable(sqlite_file, table.name, table.assignment_str)
