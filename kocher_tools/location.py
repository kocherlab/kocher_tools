import os
import sys
import csv
import logging

from kocher_tools.common import readCommonFile
from kocher_tools.database import insertValues, updateValues, retrieveValues

def convertLoc (database, table, loc_column, loc_value, cvt_column):
	
	# Retrieve the desired information from the database
	location_data = retrieveValues(database, [table], {loc_column:[loc_value]}, {}, [cvt_column])

	# Update log
	logging.info('Location conversion successful')

	# Return the data
	return location_data[0].keys()[0], location_data[0][0]

def addLocFileToDatabase (database, table, loc_file):

	# Update log
	logging.info('Uploading location file (%s) to database' % loc_file)

	# Loop the loc file by line
	for header, loc_data in readCommonFile(loc_file):

		# Insert the loc into the database
		insertValues(database, table, header, loc_data)

	# Update log
	logging.info('Upload successful')

def updateLocFileToDatabase (database, table, select_key, loc_file):

	# Update log
	logging.info('Uploading location file (%s) to database' % loc_file)

	# Loop the loc file by line
	for header, sample_data in readCommonFile(loc_file):

		# Check if the selection key isn't among the headers
		if select_key not in header:
			raise Exception('Selection key (%s) not found. Please check the input file' % select_key)

		# Create an empty set dict
		loc_set_dict = {}

		# Create an empty selection dict
		loc_select_dict = {}
		loc_select_dict['IN'] = {}

		# Loop the header ans sample data
		for header_column, loc_value in zip(header, loc_data):

			# Check if the current column is the selection key
			if header_column == select_key:

				# Populate the selection dict
				loc_select_dict['IN'][header_column] = [loc_value]

			else:

				# Populate the selection dict
				loc_set_dict[header_column] = loc_value

		# Update the values for the selected value
		updateValues(database, table, loc_select_dict, loc_set_dict)

	# Update log
	logging.info('Upload successful')

def readAppLocations (collection_filename):

	with open(collection_filename) as collection_file:

		# Read the data file
		collection_reader = csv.reader(collection_file)

		if sys.version_info[0] == 3:

			# Save the header
			header = next(collection_reader)

		else:
		
			# Save the header
			header = collection_reader.next()

		try:

			# Obtain the index of the GPS data
			gps_index = header.index('GPS')

		except:

			raise Exception('Unable to assign GPS index')

		try:

			# Obtain the index of the site code data
			site_code_index = header.index('Site Code')

		except:

			raise Exception('Unable to assign Site Code index')

		# Create list to hold unique locations
		unique_locations = []

		# Loop the rows of data
		for row in collection_reader:

			# Check if the row has a site code
			if not row[site_code_index]:

				# Check that the row isn't empty
				if len(set(row)) > 1:
					raise Exception('No Site Code found %s' % row)

				# Skip iteration due to empty line
				continue

			# Create a list of the current location
			current_location = [row[site_code_index], row[gps_index]]

			# Check if the current location isnt within the list of unique locations
			if current_location not in unique_locations:

				# Add the current location to the location list
				unique_locations.append(current_location)

		# Loop the unique locations
		for unique_location in unique_locations:

			# Yield the location data
			yield [header[site_code_index], header[gps_index]], unique_location

def addAppLocationsToDatabase (database, table, loc_file):

	# Update log
	logging.info('Uploading locations from app file (%s) to database' % app_file)

	# Loop the loc file by line
	for header, loc_data in readAppLocations(loc_file):

		# Insert the loc into the database
		insertValues(database, table, header, loc_data)

	# Update log
	logging.info('Upload successful')

def updateAppLocationsToDatabase (database, table, select_key, loc_file):

	# Update log
	logging.info('Uploading locations from app file (%s) to database' % app_file)

	# Loop the loc file by line
	for header, loc_data in readAppLocations(loc_file):

		# Check if the selection key isn't among the headers
		if select_key not in header:
			raise Exception('Selection key (%s) not found. Please check the input file' % select_key)

		# Create an empty set dict
		loc_set_dict = {}

		# Create an empty selection dict
		loc_select_dict = {}
		loc_select_dict['IN'] = {}

		# Loop the header ans sample data
		for header_column, loc_value in zip(header, loc_data):

			# Check if the current column is the selection key
			if header_column == select_key:

				# Populate the selection dict
				loc_select_dict['IN'][header_column] = [loc_value]

			else:

				# Populate the selection dict
				loc_set_dict[header_column] = loc_value

		# Update the values for the selected value
		updateValues(database, table, loc_select_dict, loc_set_dict)

	# Update log
	logging.info('Upload successful')

#convertLoc ('kocherDB.sqlite', 'Locations', 'Location', 'Venus', 'Site Code')