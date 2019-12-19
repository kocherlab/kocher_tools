import os
import sys
import csv

from common import readCommonFile
from database import insertValues, updateValues, retrieveValues

def convertPlateWell (database, table, key, plate, well):

	# Get the key data from the Storage table
	key_data = retrieveValues(database, [table], {'IN':{'Plate':[plate], 'Well':[well]}}, [key])

	# Check if more than one ID was found
	if len(key_data) > 1: 
		raise Exception('Unable to assign %s' % key)

	return key_data[0][0]

def addLocFileToDatabase (database, table, storage_file):

	# Loop the storage file by line
	for header, storage_data in readCommonFile(storage_file):

		# Insert the loc into the database
		insertValues(database, table, header, storage_data)

def updateLocFileToDatabase (database, table, select_key, storage_file):

	# Loop the storage file by line
	for header, storage_data in readCommonFile(storage_file):

		# Check if the selection key isn't among the headers
		if select_key not in header:
			raise Exception('Selection key (%s) not found. Please check the input file' % select_key)

		# Create an empty set dict
		storage_set_dict = {}

		# Create an empty selection dict
		storage_select_dict = {}
		storage_select_dict['IN'] = {}

		# Loop the header ans sample data
		for header_column, storage_value in zip(header, storage_data):

			# Check if the current column is the selection key
			if header_column == select_key:

				# Populate the selection dict
				storage_select_dict['IN'][header_column] = [storage_value]

			else:

				# Populate the selection dict
				storage_set_dict[header_column] = storage_value

		# Update the values for the selected value
		updateValues(database, table, storage_select_dict, storage_set_dict)
