import os
import sys
import csv
from database import insertValues, updateValues, retrieveValues

def convertLoc (database, table, loc_column, loc_value, cvt_column):
	
	# Retrieve the desired information from the database
	location_data = retrieveValues (database, [table], {loc_column:[loc_value]}, {}, [cvt_column])

	# Return the data
	return location_data[0].keys()[0], location_data[0][0]

def readLocFile (loc_filename):

	with open(loc_filename) as loc_file:

		# Read the data file
		loc_reader = csv.reader(loc_file, delimiter = '\t')

		if sys.version_info[0] == 3:

			# Save the header
			header = next(loc_reader)

		else:
		
			# Save the header
			header = loc_reader.next()

		# Loop the rows of data
		for row in loc_reader:

			# Return the header and row
			yield header, row

def addLocFileToDatabase (database, table, loc_file):

	# Loop the loc file by line
	for header, loc_data in readLocFile(loc_file):

		# Insert the loc into the database
		insertValues(database, table, header, loc_data)

def updateLocFileToDatabase (database, table, loc_file, select_key):

	# Loop the loc file by line
	for header, sample_data in readLocFile(loc_file):

		# Check if the selection key isn't among the headers
		if select_key not in header:
			raise Exception('Selection key (%s) not found. Please check the input file' % select_key)

		# Create an empty set dict
		loc_set_dict = {}

		# Create an empty selection dict
		loc_select_dict = {}

		# Loop the header ans sample data
		for header_column, loc_value in zip(header, loc_data):

			# Check if the current column is the selection key
			if header_column == select_key:

				# Populate the selection dict
				loc_select_dict[header_column] = [loc_value]

			else:

				# Populate the selection dict
				loc_set_dict[header_column] = loc_value

		# Update the values for the selected value
		updateValues(database, table, loc_select_dict, {}, loc_set_dict)


#convertLoc ('kocherDB.sqlite', 'Locations', 'Location', 'Venus', 'Site Code')