import os
import sys
import argparse
import copy

from collections import defaultdict

from collection import addAppCollectionToDatabase, updateAppCollectionToDatabase
from location import addLocFileToDatabase, updateLocFileToDatabase, addAppLocationsToDatabase, updateAppLocationsToDatabase
from sequence import addSeqFilesToDatabase, updateSeqFilesToDatabase
from assignment import assignSelectionDict, assignTables
from database import updateValues
from config_file import readConfig
from logger import startLogger

def upload_sample_parser ():
	'''
	Argument parser for adding samples

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

	def confirmFileList ():
		'''Custom action to confirm file exists in list'''
		class customAction(argparse.Action):
			def __call__(self, parser, args, value, option_string=None):
				# Loop the list
				for value_item in value:
					# Check if the file exists
					if not os.path.isfile(value_item):
						raise IOError('%s not found' % value_item)
				if not getattr(args, self.dest):
					setattr(args, self.dest, value)
				else:
					getattr(args, self.dest).extend(value)
		return customAction

	def confirmPairedFiles ():
		'''Custom action to confirm file exists in list'''
		class customAction(argparse.Action):
			def __call__(self, parser, args, value, option_string=None):
				# Loop the list
				for value_item in value:
					# Check if the file exists
					if not os.path.isfile(value_item):
						raise IOError('%s not found' % value_item)
				if not getattr(args, self.dest):
					setattr(args, self.dest, [value])
				else:
					getattr(args, self.dest).append(value)
		return customAction

	def updateDict ():
		'''Custom action to add items to a list'''
		class customAction(argparse.Action):
			def __call__(self, parser, args, value_list, option_string=None):

				# Clean up any commas
				value_list = [item.strip(',') for item in value_list]

				if not getattr(args, self.dest):
					# Create the dict
					value_dict = {}
					# Populate the dict
					value_dict[value_list[0]] = value_list[1]
					setattr(args, self.dest, value_dict)
				else:
					getattr(args, self.dest)[value_list[0]] = value_list[1]
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

	def metavar_list (var_list):
		'''Create a formmated metavar list for the help output'''
		return '{' + ', '.join(var_list) + '}'

	upload_parser = argparse.ArgumentParser(formatter_class = argparse.ArgumentDefaultsHelpFormatter)

	# Input arguments
	upload_parser.add_argument('--app-file', dest = 'app_files', help = "Defines the collection app filename", type = str, nargs = '+', action = confirmFileList())
	upload_methods = ('Collection', 'Location', 'Both')
	upload_parser.add_argument('--app-upload-method', metavar = metavar_list(upload_methods), help = 'Upload method for collection app files', choices = upload_methods, default = upload_methods[0])
	upload_parser.add_argument('--sequence-files', dest = 'seq_files', metavar = ('blast_file', 'fasta_file'), help = "Defines the BLAST and fasta output filename", type = str, nargs = 2, action = confirmPairedFiles())
	upload_parser.add_argument('--location-file', dest = 'loc_files', help = 'Defines the filename of a location file', type = str, nargs = '+', action = confirmFileList())
	upload_parser.add_argument('--storage-file', dest = 'storage_files', help = 'Defines the filename of a storage file ', type = str, nargs = '+', action = confirmFileList())
	#upload_parser.add_argument('--file', help = 'Defines a table filename', type = str, action = parser_confirm_file())
	#table_formats = ('tsv', 'csv')
	#upload_parser.add_argument('--file-format', metavar = metavar_list(table_formats), help = 'Defines the format of the table file', type = str, choices = table_formats)



	# Selection arguments
	upload_parser.add_argument('--include-ID', help = 'ID to include in database updates', type = str, nargs = '+', action = selectionList())
	upload_parser.add_argument('--exclude-ID', help = 'ID to exclude in database updates', type = str, nargs = '+', action = selectionList())
	upload_parser.add_argument('--include-species', help = 'Species to include in database updates', type = str, nargs = '+', action = selectionList())
	upload_parser.add_argument('--exclude-species', help = 'Species to exclude in database updates', type = str, nargs = '+', action = selectionList())
	upload_parser.add_argument('--include-genus', help = 'Species to include in database updates', type = str, nargs = '+', action = selectionList())
	upload_parser.add_argument('--exclude-genus', help = 'Species to exclude in database updates', type = str, nargs = '+', action = selectionList())
	upload_parser.add_argument('--include', metavar = ('column', 'value'), help = 'Column/value pair to include in database updates', type = str, nargs = 2, action = selectionDict())
	upload_parser.add_argument('--exclude', metavar = ('column', 'value'), help = 'Column/value pair to exclude in database updates', type = str, nargs = 2, action = selectionDict())

	# Update arguments
	upload_parser.add_argument('--update-with-file', help = 'Update database using with the entries within a table', action='store_true')
	upload_parser.add_argument('--update', metavar = ('column', 'value'), help = 'Column/value pair to update', type = str, nargs = 2, action = updateDict())

	# Database arguments
	upload_parser.add_argument('--sqlite-db', help = 'SQLite database filename', type = str, default = 'kocherDB.sqlite', action = confirmFile())
	upload_parser.add_argument('--yaml', help = 'Database YAML config file', type = str, default = 'kocherDB.yml', action = confirmFile())


	return upload_parser.parse_args()

def canUpdate (update_dict):

	# Create list of columns that shouldn't be altered
	locked_list = ['Site Code', 'Unique ID']

	# loop the update dict by columns
	for column in update_dict.keys():

		# Check if the column is within the locked list
		if column.strip().replace('"','') in locked_list:

			return False

	return True
			
# Assign arguments
upload_args = upload_sample_parser()

# Read in the database config file
db_config_data = readConfig(upload_args.yaml)

startLogger()

# Check if a collection app file has been specified
if upload_args.app_files:

	# Loop the collection app files
	for app_file in upload_args.app_files:

		# Check if a selection key has been specified
		if upload_args.update_with_file:

			# Check if the Collection should be updated
			if upload_args.app_upload_method in ['Collection', 'Both']:

				# Update the database with the file
				updateAppCollectionToDatabase(upload_args.sqlite_db, 'Collection', 'Unique ID', app_file)

			# Check if the Collection should be updated
			if upload_args.app_upload_method in ['Location', 'Both']:

				# Update the database with the file
				updateAppLocationsToDatabase(upload_args.sqlite_db, 'Location', 'Unique ID', app_file)

		else:

			# Check if the Collection should be added
			if upload_args.app_upload_method in ['Collection', 'Both']:

				# Add file to the database
				addAppCollectionToDatabase(upload_args.sqlite_db, 'Collection', app_file)

			# Check if the Collection should be updated
			if upload_args.app_upload_method in ['Location', 'Both']:

				# Add file to the database
				addAppLocationsToDatabase(upload_args.sqlite_db, 'Location', app_file)

# Check if sequencing files have been specified
if upload_args.seq_files:

	# Loop the collection app files
	for blast_file, fasta_file in upload_args.seq_files:

		# Check if a selection key has been specified
		if upload_args.update_with_file:

			# Update the database with the file
			updateSeqFilesToDatabase(upload_args.sqlite_db, 'Sequencing', 'Sequence ID', blast_file, fasta_file, 'Storage', 'Unique ID')

		else:

			# Add file to the database
			addSeqFilesToDatabase(upload_args.sqlite_db, 'Sequencing', blast_file, fasta_file, 'Storage', 'Unique ID')

# Check if a location file has been specified
if upload_args.loc_files:

	# Loop the location files
	for loc_file in upload_args.loc_files:

		# Check if a selection key has been specified
		if upload_args.update_with_file:

			# Update the database with the file
			updateLocFileToDatabase(upload_args.sqlite_db, 'Locations', 'Site Code', loc_file)

		else:

			# Add file to the database
			addLocFileToDatabase(upload_args.sqlite_db, 'Locations', loc_file)

# Check if update data has been specified
if upload_args.update:

	# Check if the update is impossible
	if not canUpdate(upload_args.update):
		raise Exception('Not allowed to alter: %s' % column)

	# Assign the tables that need to be updated
	selection_tables = assignTables(db_config_data, **vars(upload_args))

	# Assign a defaultdict with all the selection information
	selection_dict = assignSelectionDict(db_config_data, **vars(upload_args))

	# Create a list of the tables to update
	tables_to_update = db_config_data.returnTables(list(upload_args.update.keys()))
	
	# Loop the tables that need to be updated 
	for table_to_update in tables_to_update:

		# Create the update statement dict for the current table
		update_statement_dict = db_config_data.returnColumnDict(upload_args.update, table_to_update)

		# Check if the current table is within the tables to join
		if table_to_update in selection_tables and len(selection_tables) == 1:

			updateValues(upload_args.sqlite_db, table_to_update, selection_dict, update_statement_dict)

		# Run the expanded command if there are tables to join
		else:

			# Assign the key for the table to update
			join_by_key = db_config_data[table_to_update].join_by_key

			# Create a copy of the selection tables
			tables_to_join = copy.copy(selection_tables)

			# Add the table that is going to be updated
			tables_to_join.append(table_to_update)

			# Remove any duplicates
			tables_to_join = list(set(tables_to_join))

			# Assign the tables and keys that need to be joined
			join_by_names, join_by_keys = db_config_data.returnJoinLists(tables_to_join)

			updateValues(upload_args.sqlite_db, table_to_update, selection_dict, update_statement_dict, update_table_key = join_by_key, tables_to_join = join_by_names, join_tables_keys = join_by_keys)
