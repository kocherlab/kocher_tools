import os
import sys
import argparse
from collections import defaultdict

from output import entriesToScreen, entriesToFile
from assignment import assignSelectionDict, assignTables
from database import retrieveValues
from config_file import readConfig 

def retrieve_sample_parser ():
	'''
	Database Argument Parser 

	Raises
	------
	IOError
		If the input, or other specified files do not exist
	'''

	def parser_confirm_file ():
		'''Custom action to confirm file exists'''
		class customAction(argparse.Action):
			def __call__(self, parser, args, value, option_string=None):
				if not os.path.isfile(value):
					raise IOError('%s not found' % value)
				setattr(args, self.dest, value)
		return customAction

	def parser_add_to_list ():
		'''Custom action to add items to a list'''
		class customAction(argparse.Action):
			def __call__(self, parser, args, value_list, option_string=None):

				# Clean up any commas
				value_list = [item.strip(',') for item in value_list]

				if not getattr(args, self.dest):
					setattr(args, self.dest, value_list)
				else:
					getattr(args, self.dest).extend(value_list)
		return customAction

	def parser_to_dict_list ():
		'''Custom action to add items to a list'''
		class customAction(argparse.Action):
			def __call__(self, parser, args, value_list, option_string=None):

				# Clean up any commas
				value_list = [item.strip(',') for item in value_list]

				if not getattr(args, self.dest):
					# Create the dict
					value_dict = defaultdict(list)
					# Populate the dict
					value_dict[value_list[0]].append(value_list[1])
					setattr(args, self.dest, value_dict)
				else:
					getattr(args, self.dest)[value_list[0]].append(value_list[1])
		return customAction

	def metavar_list (var_list):
		'''Create a formmated metavar list for the help output'''
		return '{' + ', '.join(var_list) + '}'

	retrieve_parser = argparse.ArgumentParser(formatter_class = argparse.ArgumentDefaultsHelpFormatter)

	# Selection arguments
	retrieve_parser.add_argument('--table', help = 'Table to return', type = str, nargs = '+', action = parser_add_to_list(), required = True)
	retrieve_parser.add_argument('--include-species', help = 'Species to include in database updates', type = str, nargs = '+', action = parser_add_to_list())
	retrieve_parser.add_argument('--exclude-species', help = 'Species to exclude in database updates', type = str, nargs = '+', action = parser_add_to_list())
	retrieve_parser.add_argument('--include-ID', help = 'ID to include in database updates', type = str, nargs = '+', action = parser_add_to_list())
	retrieve_parser.add_argument('--exclude-ID', help = 'ID to exclude in database updates', type = str, nargs = '+', action = parser_add_to_list())
	retrieve_parser.add_argument('--include', metavar = ('column', 'value'), help = 'Column/value pair to include in database updates', type = str, nargs = 2, action = parser_to_dict_list())
	retrieve_parser.add_argument('--exclude', metavar = ('column', 'value'), help = 'Column/value pair to exclude in database updates', type = str, nargs = 2, action = parser_to_dict_list())

	# Output arguments
	out_formats = ['tsv', 'csv']
	out_default = 'tsv'
	retrieve_parser.add_argument('--out-format', metavar = metavar_list(out_formats), help = 'Desired output format', type = str, choices = out_formats, default = out_default)
	retrieve_parser.add_argument('--out-prefix', help = 'The output prefix (i.e. filename without file extension)', type = str,  default = 'out')
	retrieve_parser.add_argument('--out-columns', help = 'Columns to return in the output', type = str, nargs = '+', action = parser_add_to_list())
	retrieve_parser.add_argument('--stdout', help = 'Direct output to stdout', action = 'store_true')

	# Database arguments
	retrieve_parser.add_argument('--sqlite-db', help = 'Defines the sqlite database filename', type = str, default = 'kocherDB.sqlite', action = parser_confirm_file())
	retrieve_parser.add_argument('--yaml', help = 'Database YAML config file', type = str, default = 'kocherDB.yml', action = parser_confirm_file())

	return retrieve_parser.parse_args()

# Assign arguments
retrieve_args = retrieve_sample_parser()

# Read in the database config file
db_config_data = readConfig(retrieve_args.yaml)

# List to hold columns to be printed
columns_assignment_list = []

# Loop the list of specified tables
for table_str in retrieve_args.table:

	# Check if the database has the specified table
	if table_str not in db_config_data:

		# Print an error message
		raise Exception('Table (%s) not found. Please select from: %s' % (table_str, ', '.join(db_config_data.tables)))

	# Get the columns from the table and add to the column assignment list
	columns_assignment_list.extend(db_config_data[table_str].retieveColumnPaths())

# Assign a defaultdict with all the selection information
selection_dict = assignSelectionDict(db_config_data, **vars(retrieve_args))

# Assign the tables and keys that need to be joined
tables, join_by_keys = db_config_data.returnJoinLists(retrieve_args.table)

# Retrieve the selected entries from the database 
retrieved_entries = retrieveValues(retrieve_args.sqlite_db, tables, selection_dict, columns_assignment_list, join_tables_keys = join_by_keys)

# Set the deafult delimiter
delimiter = '\t'

# Check if the csv format was requested
if retrieve_args.out_format == 'csv':

	# Update the delimiter
	delimiter = ','

# Check if stdout was requested
if retrieve_args.stdout:

	# Print the entries to stdout
	entriesToScreen(retrieved_entries, delimiter)

else:

	# Write retrieved entries to a file
	entriesToFile(retrieved_entries, retrieve_args.out_prefix, retrieve_args.out_format, delimiter)
