import os
import sys
import argparse
import logging

from collections import defaultdict

from output import entriesToScreen, entriesToFile
from assignment import assignSelectionDict, assignTables
from database import retrieveValues
from config_file import readConfig
from logger import startLogger, logArgs

def retrieveSampleParser ():
	'''
	Database Argument Parser 

	Raises
	------
	IOError
		If the input, or other specified files do not exist
	'''

	def confirmFile ():
		'''Custom action to confirm file exists'''
		class customAction(argparse.Action):
			def __call__(self, parser, args, value, option_string=None):
				if not os.path.isfile(value):
					raise IOError('%s not found' % value)
				setattr(args, self.dest, value)
		return customAction

	def selectionList ():
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

	def selectionDict ():
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

	def metavarList (var_list):
		'''Create a formmated metavar list for the help output'''
		return '{' + ', '.join(var_list) + '}'

	retrieve_parser = argparse.ArgumentParser(formatter_class = argparse.ArgumentDefaultsHelpFormatter)

	# Selection arguments
	retrieve_select = retrieve_parser.add_mutually_exclusive_group(required = True)
	retrieve_select.add_argument('--table', help = 'Table(s) to return', type = str, nargs = '+', action = selectionList())
	retrieve_select.add_argument('--column', help = 'Column(s) to return', type = str, nargs = '+', action = selectionList())
	retrieve_parser.add_argument('--include-ID', help = 'ID to include in database retrievals', type = str, nargs = '+', action = selectionList())
	retrieve_parser.add_argument('--exclude-ID', help = 'ID to exclude in database retrievals', type = str, nargs = '+', action = selectionList())
	retrieve_parser.add_argument('--include-species', help = 'Species to include in database retrievals', type = str, nargs = '+', action = selectionList())
	retrieve_parser.add_argument('--exclude-species', help = 'Species to exclude in database retrievals', type = str, nargs = '+', action = selectionList())
	retrieve_parser.add_argument('--include-genus', help = 'Genus to include in database retrievals', type = str, nargs = '+', action = selectionList())
	retrieve_parser.add_argument('--exclude-genus', help = 'Genus to exclude in database retrievals', type = str, nargs = '+', action = selectionList())
	retrieve_parser.add_argument('--include-nests', help = 'Include samples from nests in database retrievals', action = 'store_true')
	retrieve_parser.add_argument('--exclude-nests', help = 'Exclude samples from nests in database retrievals', action = 'store_true')
	retrieve_parser.add_argument('--include', metavar = ('column', 'value'), help = 'Column/value pair to include in database retrievals', type = str, nargs = 2, action = selectionDict())
	retrieve_parser.add_argument('--exclude', metavar = ('column', 'value'), help = 'Column/value pair to exclude in database retrievals', type = str, nargs = 2, action = selectionDict())

	# Output arguments
	out_formats = ['tsv', 'csv']
	out_default = 'tsv'
	retrieve_parser.add_argument('--out-format', metavar = metavarList(out_formats), help = 'Desired output format', type = str, choices = out_formats, default = out_default)
	retrieve_parser.add_argument('--out-prefix', help = 'The output prefix (i.e. filename without file extension)', type = str,  default = 'out')
	retrieve_parser.add_argument('--out-columns', help = 'Columns to return in the output', type = str, nargs = '+', action = selectionList())
	retrieve_parser.add_argument('--out-log', help = 'Filename of the log file', type = str, default = 'retrieve_samples.log')
	retrieve_parser.add_argument('--stdout', help = 'Direct output to stdout', action = 'store_true')
	retrieve_parser.add_argument('--overwrite', help = 'Overwrite previous output', action = 'store_true')

	# Database arguments
	retrieve_parser.add_argument('--sqlite-db', help = 'Defines the sqlite database filename', type = str, default = 'kocherDB.sqlite', action = confirmFile())
	retrieve_parser.add_argument('--yaml', help = 'Database YAML config file', type = str, default = 'kocherDB.yml', action = confirmFile())

	return retrieve_parser.parse_args()

# Assign arguments
retrieve_args = retrieveSampleParser()

# Check that the output mode isn't stdout
if not retrieve_args.stdout:

	# Assign the expected out filename
	expected_out_filename = retrieve_args.out_prefix  + '.' + retrieve_args.out_format

	# Check if previous output should be overwritten
	if retrieve_args.overwrite:

		# Remove the previous output, if it exists
		if os.path.exists(expected_out_filename):
			os.remove(expected_out_filename)

	# Check if previous output shouldn't be overwritten
	else:

		# Check if previous output exists
		if os.path.exists(expected_out_filename):

			# Raise an exception
			raise Exception('Output already exists. Please alter the output arguments or use --overwrite')

# Read in the database config file
db_config_data = readConfig(retrieve_args.yaml)

# Start a log file for this run
startLogger(log_filename = retrieve_args.out_log)

# Log the arguments used
logArgs(retrieve_args)

# List to hold columns to be printed
columns_assignment_list = []

# List to hold table to be printed, required for printing single columns
table_assignment_list = []

# Check if the user specified a table
if retrieve_args.table:

	# Re-assign the tables
	table_assignment_list = retrieve_args.table

	# Loop the list of specified tables
	for table_str in table_assignment_list:

		# Check if the database has the specified table
		if table_str not in db_config_data:

			# Print an error message
			raise Exception('Table (%s) not found. Please select from: %s' % (table_str, ', '.join(db_config_data.tables)))

		# Get the columns from the table and add to the column assignment list
		columns_assignment_list.extend(db_config_data[table_str].retieveColumnPaths(keep_db_specific_columns = True))

		# Update log
		logging.info('Successfully assigned table(s) from command-line')

# Check if the user specified a column
elif retrieve_args.column:

	# Loop the list of specified column
	for column_str in retrieve_args.column:

		# Check if the column exists within the database
		if not db_config_data.hasColumn(column_str):

			# Print an error message
			raise Exception('Column (%s) not found' % column_str)

		# Loop the tables within the database
		for db_table in db_config_data:

			# Check if the current table has the column
			if column_str in db_table:

				# Assign the column path
				columns_assignment_list.append(db_table[column_str].path)

				# Assign the table
				table_assignment_list.append(str(db_table))

				break
	# Update log
	logging.info('Successfully assigned column(s) from command-line')

# Assign a defaultdict with all the selection information
selection_dict = assignSelectionDict(db_config_data, **vars(retrieve_args))

# Assign the tables that need to be updated
selection_tables = assignTables(db_config_data, **vars(retrieve_args))

# Assign the tables for retrieval
tables = table_assignment_list + selection_tables

# Check if only a single table is required
if len(tables) == 1:

	# Retrieve the selected entries from the database 
	retrieved_entries = retrieveValues(retrieve_args.sqlite_db, tables, selection_dict, columns_assignment_list)

# Otherwise, run the process for multiple tables
else:

	# Assign the tables and keys that need to be joined
	tables, join_by_columns = db_config_data.returnJoinLists(tables)

	# Retrieve the selected entries from the database 
	retrieved_entries = retrieveValues(retrieve_args.sqlite_db, tables, selection_dict, columns_assignment_list, join_table_columns = join_by_columns)

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