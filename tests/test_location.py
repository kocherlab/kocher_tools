import os
import sys
import sqlite3
import unittest
import filecmp
import shutil
import tempfile
import io
import string
import random

from kocher_tools.database import createTable
from kocher_tools.config_file import readConfig
from kocher_tools.location import addLocFileToDatabase, updateLocFileToDatabase, convertLoc, readAppLocations, addAppLocationsToDatabase, updateAppLocationsToDatabase
from tests.functions import checkValue

# Run tests for database input from location files
class test_location (unittest.TestCase):

	@classmethod
	def setUpClass (cls):

		# Create a temporary directory
		cls.test_dir = tempfile.mkdtemp()

		# Assign the script directory
		cls.script_dir = os.path.dirname(os.path.realpath(__file__))

		# Assign the expected output directory
		cls.expected_dir = 'test_files'

		# Assign the expected path
		cls.expected_path = os.path.join(cls.script_dir, cls.expected_dir)

		# Try creating the database
		try:

			# Assign the existing YAML filename
			config_filename = os.path.join(cls.expected_path, 'testDB_large.yml')

			# Assign the test YAML filename
			cls.config_filename = os.path.join(cls.test_dir, 'testDB_large.yml')

			# Copy the YAML file
			shutil.copy(config_filename, cls.config_filename)

			# Read in the YAML config file
			config_data = readConfig(cls.config_filename)

			# Assign the filename of the database
			cls.database_filename = config_data.sql_database

			# Connect to the sqlite database
			sqlite_connection = sqlite3.connect(cls.database_filename)

			# Setup SQLite to reture the rows as dict with columns
			sqlite_connection.row_factory = sqlite3.Row

			# Create the cursor
			cursor = sqlite_connection.cursor()

			# Loop the tables
			for table in config_data:

				# Create the table
				createTable(cursor, table.name, table.assignment_str)

			# Commit any changes
			sqlite_connection.commit()

			# Close the connection
			cursor.close()

		# Set the data to None if that fails
		except:

			# Read in the config file
			cls.database_filename = None

	@classmethod
	def tearDownClass (cls):

		# Remove the test directory after the tests
		shutil.rmtree(cls.test_dir)

	# Check addLocFileToDatabase 
	def test_01_addLocFileToDatabase (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the location filename
		loc_filename = os.path.join(self.expected_path, 'test_location_01_input.tsv')

		# Connect to the sqlite database
		sqlite_connection = sqlite3.connect(self.database_filename)

		# Create the cursor
		cursor = sqlite_connection.cursor()

		# Add the data to the database
		addLocFileToDatabase(cursor, 'Locations', loc_filename)

		# Commit any changes
		sqlite_connection.commit()

		# Close the connection
		cursor.close()

		# Check that the values were correctly inserted
		self.assertTrue(checkValue(self.database_filename, 'Locations', '"Site Code"', 'TOM'))
		self.assertTrue(checkValue(self.database_filename, 'Locations', 'Location', '"Outer Rim"'))
		self.assertTrue(checkValue(self.database_filename, 'Locations', 'GPS', '"40.34555268323508, -74.65406448370945"'))

	# Check updateLocFileToDatabase 
	def test_02_updateLocFileToDatabase (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the location filename
		loc_filename = os.path.join(self.expected_path, 'test_location_02_input.tsv')

		# Connect to the sqlite database
		sqlite_connection = sqlite3.connect(self.database_filename)

		# Create the cursor
		cursor = sqlite_connection.cursor()

		# Add the data to the database
		updateLocFileToDatabase(cursor, 'Locations', 'Site Code', loc_filename)

		# Commit any changes
		sqlite_connection.commit()

		# Close the connection
		cursor.close()

		# Check that the values were correctly inserted
		self.assertTrue(checkValue(self.database_filename, 'Locations', 'Location', '"Inner Rim"'))
		self.assertTrue(checkValue(self.database_filename, 'Locations', 'GPS', '"50.123, -74.50.123"'))

	# Check convertLoc
	def test_03_convertLoc (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the expected location tuple
		expected_loc = ('Location', '"Inner Rim"')

		# Connect to the sqlite database
		sqlite_connection = sqlite3.connect(self.database_filename)

		# Setup SQLite to reture the rows as dict with columns
		sqlite_connection.row_factory = sqlite3.Row

		# Create the cursor
		cursor = sqlite_connection.cursor()

		# Assign the test loc from the loc file
		test_loc = convertLoc(cursor, 'Locations', '"Site Code"', 'RIM', 'Location')

		# Commit any changes
		sqlite_connection.commit()

		# Close the connection
		cursor.close()

		# Check if we get the expected value from the test results
		self.assertEqual(test_loc, expected_loc)

	# Check readAppLocations
	def test_04_readAppLocations (self):

		# Assign the collection filename
		collection_filename = os.path.join(self.expected_path, 'test_location_03_input.tsv')

		# Assign the expected header names
		expected_header = ['Site Code', 'GPS']

		# Create a bool to indicate the header has been checked
		checked_header = False

		# Assign the expected rows
		expected_rows = 	[['RIM2', '40.345576285427555, -74.65398915528671']]
		expected_rows.append(['WIM2', '40.345576285427555, -74.65398915528671'])
		expected_rows.append(['VEN2', '40.34555268323508, -74.65406448370945'])
		expected_rows.append(['TOM2', '40.00, -74.00'])

		# Parse the common file
		for line_pos, (header, loc_data) in enumerate(readAppLocations(collection_filename)):

			# Check that the header file has not been checked
			if not checked_header:

				# Check that the header is what we expect
				self.assertEqual(header, expected_header)

				# Update the bool
				checked_header = True

			# Check that the current row is what we expect
			self.assertEqual(loc_data, expected_rows[line_pos])

	# Check addAppLocationsToDatabase
	def test_05_addAppLocationsToDatabase (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the collection filename
		collection_filename = os.path.join(self.expected_path, 'test_location_03_input.tsv')

		# Connect to the sqlite database
		sqlite_connection = sqlite3.connect(self.database_filename)

		# Create the cursor
		cursor = sqlite_connection.cursor()

		# Add the locations to the database
		addAppLocationsToDatabase(cursor, 'Locations', collection_filename)

		# Commit any changes
		sqlite_connection.commit()

		# Close the connection
		cursor.close()

		# Check that the values were correctly inserted
		self.assertTrue(checkValue(self.database_filename, 'Locations', '"Site Code"', 'RIM2'))
		self.assertTrue(checkValue(self.database_filename, 'Locations', 'GPS', '"40.00, -74.00"'))

	# Check updateAppLocationsToDatabase
	def test_06_updateAppLocationsToDatabase (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the collection filename
		collection_filename = os.path.join(self.expected_path, 'test_location_04_input.tsv')

		# Connect to the sqlite database
		sqlite_connection = sqlite3.connect(self.database_filename)

		# Create the cursor
		cursor = sqlite_connection.cursor()

		# Add the locations to the database
		updateAppLocationsToDatabase(cursor, 'Locations', 'Site Code',collection_filename)

		# Commit any changes
		sqlite_connection.commit()

		# Close the connection
		cursor.close()

		# Check that the values were correctly inserted
		self.assertTrue(checkValue(self.database_filename, 'Locations', 'GPS', '"50, -75"'))

if __name__ == "__main__":
	unittest.main(verbosity = 2)