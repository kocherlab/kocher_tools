import os
import sys
import csv
import copy
from database import insertValues, updateValues

def readAppCollection (collection_filename, add_filename_col = True, filename_header = 'Collection File', remove_GPS = True):

	with open(collection_filename) as collection_file:

		# Read the data file
		collection_reader = csv.reader(collection_file)

		if sys.version_info[0] == 3:

			# Save the header
			header = next(collection_reader)

		else:
		
			# Save the header
			header = collection_reader.next()

		# Check if GPS data should not be stored
		if remove_GPS:

			try:

				# Obtain the index of the GPS data
				gps_index = header.index('GPS')

			except:

				raise Exception('Unable to assign GPS index')


			# Remove the GPS header entry
			del header[gps_index]

		# Check if a filename column should be added
		if add_filename_col:

			# Add the origin file column
			header.append(filename_header)

		# Loop the rows of data
		for row in collection_reader:

			# Check if the row has an ID
			if not row[0]:

				# Check that the row isn't empty
				if len(set(row)) > 1:
					raise Exception('No Unique ID found %s' % row)

				# Skip iteration due to empty line
				continue

			# Check if GPS data should not be stored
			if remove_GPS:

				# Remove the GPS header entry
				del row[gps_index]

			# Check if a filename column should be added
			if add_filename_col:

				# Add the collection filename (as basename) value
				row.append(os.path.basename(collection_filename))

			# Return the header and row
			yield header, row

def addAppCollectionToDatabase (database, table, app_file):

	# Loop the collection app file by line
	for header, sample_data in readAppCollection(app_file):

		# Insert the sample into the database
		insertValues(database, table, header, sample_data)

def updateAppCollectionToDatabase (database, table, select_key, app_file):

	# Loop the collection app file by line
	for header, sample_data in readAppCollection(app_file):

		# Check if the selection key isn't among the headers
		if select_key not in header:
			raise Exception('Selection key (%s) not found. Please check the input file' % select_key)

		# Create an empty set dict
		app_set_dict = {}

		# Create an empty selection dict
		app_select_dict = {} 
		app_select_dict['IN'] = {}

		# Loop the header ans sample data
		for header_column, sample_value in zip(header, sample_data):

			# Check if the current column is the selection key
			if header_column == select_key:

				# Populate the selection dict
				app_select_dict['IN'][header_column] = [sample_value]

			else:

				# Populate the selection dict
				app_set_dict[header_column] = sample_value

		# Update the values for the selected value
		updateValues(database, table, app_select_dict, app_set_dict)
