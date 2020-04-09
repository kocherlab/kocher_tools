import os
import sys
import csv
import copy
import logging

from kocher_tools.database import insertValues, updateValues, confirmValue

def readAppCollection (collection_filename, add_filename_col = True, filename_header = 'Collection File', remove_GPS = True):

	with open(collection_filename) as collection_file:

		# Read the data file
		collection_reader = csv.reader(collection_file, delimiter = '\t')

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

def addAppCollectionToDatabase (cursor, table, app_file):

	# Update log
	logging.info('Uploading collection from app file (%s) to database' % app_file)

	# Loop the collection app file by line
	for header, sample_data in readAppCollection(app_file):

		# Insert the sample into the database
		insertValues(cursor, table, header, sample_data)

	# Update log
	logging.info('Upload successful')

def updateAppCollectionToDatabase (cursor, table, select_key, app_file):

	# Update log
	logging.info('Uploading collection from app file (%s) to database' % app_file)

	# Loop the collection app file by line
	for header, sample_data in readAppCollection(app_file):

		# Check if the selection key isn't among the headers
		if select_key not in header:
			raise Exception('Selection key (%s) not found. Please check the input file' % select_key)

		# Create an empty string to store the select_value
		select_value = ''

		# Create an empty set dict
		app_set_dict = {}

		# Create an empty selection dict
		app_select_dict = {} 
		app_select_dict['IN'] = {}

		# Loop the header ans sample data
		for header_column, sample_value in zip(header, sample_data):

			# Check if the current column is the selection key
			if header_column == select_key:

				# Update the select value
				select_value = sample_value

				# Populate the selection dict
				app_select_dict['IN'][header_column] = [sample_value]

			else:

				# Populate the selection dict
				app_set_dict[header_column] = sample_value

		# Check that selected value is present
		if not confirmValue(cursor, table, select_key, select_value):

			# If not, log warning
			logging.warning('Entry (%s) not found, unable to update. Please check input' % select_value)

		# Update the values for the selected value
		updateValues(cursor, table, app_select_dict, app_set_dict)

	# Update log
	logging.info('Upload successful')