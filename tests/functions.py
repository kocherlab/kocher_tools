import os
import sys
import sqlite3
import unittest
import filecmp
import gzip
import shutil
import tempfile
import io
import string
import random

def checkTable (database, table):
	
	try:
		
		# Create the table check string
		sqlite_check_table = 'SELECT COUNT(*) FROM sqlite_master WHERE type=? AND name=?'

		# Connect to the sqlite database
		sqlite_connection = sqlite3.connect(database)

		# Create the cursor
		cursor = sqlite_connection.cursor()

		# Execute the create table command
		cursor.execute(sqlite_check_table, ['table', table])

		# Save the selected results
		selection_results = cursor.fetchall()

		# Commit any changes
		sqlite_connection.commit()

		# Close the connection
		cursor.close()

		# Check if there was an assignment error
		if not selection_results:
			raise Exception('Assignment error')

		# Check if the table was found
		if selection_results[0][0] == 1:

			# Return True
			return True

		# Check if the table was not found
		elif selection_results[0][0] == 0:

			# Return False
			return False

		# Check if there was an unknown error
		else:
			raise Exception('Unknown error')

	except sqlite3.Error as error:
		raise Exception(error)

def checkColumn (database, table, column):
	
	try:
		
		# Create the column check string
		sqlite_check_column = 'SELECT COUNT(*) FROM pragma_table_info(?) WHERE name=?'

		# Connect to the sqlite database
		sqlite_connection = sqlite3.connect(database)

		# Create the cursor
		cursor = sqlite_connection.cursor()

		# Execute the create table command
		cursor.execute(sqlite_check_column, [table, column])

		# Save the selected results
		selection_results = cursor.fetchall()

		# Commit any changes
		sqlite_connection.commit()

		# Close the connection
		cursor.close()

		# Check if there was an assignment error
		if not selection_results:
			raise Exception('Assignment error')

		# Check if the table was found
		if selection_results[0][0] == 1:

			# Return True
			return True

		# Check if the table was not found
		elif selection_results[0][0] == 0:

			# Return False
			return False

		# Check if there was an unknown error
		else:
			raise Exception('Unknown error')

	except sqlite3.Error as error:
		raise Exception(error)

def checkValue (database, table, column, value):
	
	try:

		# Connect to the sqlite database
		sqlite_connection = sqlite3.connect(database)

		# Create the cursor
		cursor = sqlite_connection.cursor()

		# Check if the value is None
		if value == None:
			
			# Create the value check string
			sqlite_check_value = 'SELECT COUNT(*) FROM %s WHERE %s is NULL' % (table, column)

			# Execute the create table command
			cursor.execute(sqlite_check_value)

		else:
		
			# Create the value check string
			sqlite_check_value = 'SELECT COUNT(*) FROM %s WHERE %s = ?' % (table, column)

			# Execute the create table command
			cursor.execute(sqlite_check_value, [value])

		# Save the selected results
		selection_results = cursor.fetchall()

		# Commit any changes
		sqlite_connection.commit()

		# Close the connection
		cursor.close()

		# Check if there was an assignment error
		if not selection_results:
			raise Exception('Assignment error')

		# Check if the table was found
		if selection_results[0][0] >= 1:

			# Return True
			return True

		# Check if the table was not found
		elif selection_results[0][0] == 0:

			# Return False
			return False

		# Check if there was an unknown error
		else:

			raise Exception('Unknown error')

	except sqlite3.Error as error:
		raise Exception(error)

def fileComp (test_output, expected_output):

	# Compare two files, return bool
	return filecmp.cmp(test_output, expected_output)

def gzFileComp (test_output, expected_output, tmp_dir):

	# Create the tmp paths
	tmp_test_path = os.path.join(tmp_dir, 'Test')
	tmp_expected_path = os.path.join(tmp_dir, 'Expected')

	# Create test tmp directories, if needed
	if not os.path.exists(tmp_test_path):
		os.makedirs(tmp_test_path)

	# Create test expected directories, if needed
	if not os.path.exists(tmp_expected_path):
		os.makedirs(tmp_expected_path)

	# Assign the tmp output files
	tmp_test_output = os.path.join(tmp_test_path, os.path.basename(test_output))
	tmp_expected_output = os.path.join(tmp_expected_path, os.path.basename(expected_output))

	# Open the gzip file
	with gzip.open(test_output, 'rb') as test_file:

		# Open the gunzip file
		with open(tmp_test_output, 'wb') as tmp_test_file:
			
			# Copy the file
			shutil.copyfileobj(test_file, tmp_test_file)

	# Open the gzip file
	with gzip.open(expected_output, 'rb') as expected_file:

		# Open the gunzip file
		with open(tmp_expected_output, 'wb') as tmp_expected_file:
			
			# Copy the file
			shutil.copyfileobj(expected_file, tmp_expected_file)

	# Check if the files have the same content
	file_compare_results = fileComp(tmp_test_output, tmp_expected_output)

	# Remove the tmp dirs
	shutil.rmtree(tmp_test_path)
	shutil.rmtree(tmp_expected_path)

	# Return the results
	return file_compare_results

def gzExpFileComp (test_output, expected_output, tmp_dir):

	# Create the tmp paths
	tmp_expected_path = os.path.join(tmp_dir, 'Expected')

	# Create test expected directories, if needed
	if not os.path.exists(tmp_expected_path):
		os.makedirs(tmp_expected_path)

	# Assign the tmp output files
	tmp_expected_output = os.path.join(tmp_expected_path, os.path.basename(expected_output))

	# Open the gzip file
	with gzip.open(expected_output, 'rb') as expected_file:

		# Open the gunzip file
		with open(tmp_expected_output, 'wb') as tmp_expected_file:
			
			# Copy the file
			shutil.copyfileobj(expected_file, tmp_expected_file)

	# Check if the files have the same content
	file_compare_results = fileComp(test_output, tmp_expected_output)

	# Remove the tmp dir
	shutil.rmtree(tmp_expected_path)

	# Return the results
	return file_compare_results

def randomGenerator (length = 10, char_set = string.ascii_uppercase + string.digits):

	# Return a random string of letters and numbers
	return ''.join(random.choice(char_set) for i in range(length))
