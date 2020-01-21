import os
import sys
import csv
import logging

from collections import OrderedDict 

def unquoteFields (db_entries):

	# Create list to hold unquoted data
	unquoted_db_entries = []

	# Loop the entries
	for db_entry in db_entries:

		# Create an empty dict
		db_entry_dict = OrderedDict()

		# Loop the headers and the values
		for column, value in zip(db_entry.keys(), db_entry):

			# Convert the data to string and unquote
			db_entry_dict[str(column).replace('"','')] = str(value).replace('"','')

		# Append the dict to the list
		unquoted_db_entries.append(db_entry_dict)

	return unquoted_db_entries

def entriesToScreen (db_entries, sep):

	# Unquote the DB entries
	unquoted_db_entries = unquoteFields(db_entries)

	# Get the header from the first entry
	header = unquoted_db_entries[0].keys()

	# Print the headers to stdout, seperated by sep
	print(sep.join(header))

	# Loop each database entry
	for db_entry_dict in unquoted_db_entries:

		# Print the values to stdout, seperated by sep
		print(sep.join(db_entry_dict.values()))

	# Update log
	logging.info('Retrieved results sent to stdout')

def entriesToFile (db_entries, out_filename, sep):

	# Unquote the DB entries
	unquoted_db_entries = unquoteFields(db_entries)

	# Get the header from the first entry
	header = unquoted_db_entries[0].keys()

	# Open the file
	with open(out_filename, 'w') as entries_file:

		# Setup the csv writer
		entries_writer = csv.DictWriter(entries_file, fieldnames = header, delimiter = sep)

		# Write the header
		entries_writer.writeheader()

		# Loop each database entry
		for db_entry_dict in unquoted_db_entries:

			# Write the database entry to the file
			entries_writer.writerow(db_entry_dict)

	# Update log
	logging.info('Retrieved results written to file (%s)' % out_filename)
