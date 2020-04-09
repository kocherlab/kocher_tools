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
from kocher_tools.storage import convertPlateWell, addStorageFileToDatabase, updateStorageFileToDatabase
from tests.functions import checkValue

# Run tests for database input from storage files
class test_storage (unittest.TestCase):

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

	# Check addStorageFileToDatabase
	def test_01_addStorageFileToDatabase (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the storage filename
		storage_filename = os.path.join(self.expected_path, 'test_storage_01_input.tsv')

		# Connect to the sqlite database
		sqlite_connection = sqlite3.connect(self.database_filename)

		# Setup SQLite to reture the rows as dict with columns
		sqlite_connection.row_factory = sqlite3.Row

		# Create the cursor
		cursor = sqlite_connection.cursor()

		# Upload the Storage data
		addStorageFileToDatabase(cursor, 'Storage', storage_filename)

		# Commit any changes
		sqlite_connection.commit()

		# Close the connection
		cursor.close()

		# Check that the values were correctly inserted
		self.assertTrue(checkValue(self.database_filename, 'Storage', '"Unique ID"', '"DBtest-0001"'))
		self.assertTrue(checkValue(self.database_filename, 'Storage', '"Storage ID"', '"DBtest-A2"'))
		self.assertTrue(checkValue(self.database_filename, 'Storage', 'Plate', 'DBtest'))
		self.assertTrue(checkValue(self.database_filename, 'Storage', 'Well', 'B1'))

	# Check updateStorageFileToDatabase
	def test_02_updateStorageFileToDatabase (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the storage filename
		storage_filename = os.path.join(self.expected_path, 'test_storage_02_input.tsv')

		# Connect to the sqlite database
		sqlite_connection = sqlite3.connect(self.database_filename)

		# Setup SQLite to reture the rows as dict with columns
		sqlite_connection.row_factory = sqlite3.Row

		# Create the cursor
		cursor = sqlite_connection.cursor()

		# Upload the Storage data
		updateStorageFileToDatabase(cursor, 'Storage', 'Storage ID', storage_filename)

		# Commit any changes
		sqlite_connection.commit()

		# Close the connection
		cursor.close()

		# Check that the values were correctly inserted
		self.assertTrue(checkValue(self.database_filename, 'Storage', 'Plate', 'DBtest2'))
		self.assertTrue(checkValue(self.database_filename, 'Storage', 'Well', 'A4'))

	# Check convertPlateWell
	def test_03_convertPlateWell (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign test data
		test_plate_1, test_well_1 = ['DBtest', 'A1']
		test_plate_2, test_well_2 = ['DBtest2', 'A4']

		# Connect to the sqlite database
		sqlite_connection = sqlite3.connect(self.database_filename)

		# Setup SQLite to reture the rows as dict with columns
		sqlite_connection.row_factory = sqlite3.Row

		# Create the cursor
		cursor = sqlite_connection.cursor()

		# Get the IDs for the tests
		test_ID_1 = convertPlateWell(cursor, 'Storage', 'Storage ID', test_plate_1, test_well_1)
		test_ID_2 = convertPlateWell(cursor, 'Storage', 'Storage ID', test_plate_2, test_well_2)

		# Commit any changes
		sqlite_connection.commit()

		# Close the connection
		cursor.close()

		# Make sure we were able to get the correct IDs
		self.assertEqual(test_ID_1, '"DBtest-A1"')
		self.assertEqual(test_ID_2, '"DBtest-A4"')

if __name__ == "__main__":
	unittest.main(verbosity = 2)