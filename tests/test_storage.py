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
from testing_functions import checkValue

# Run tests for database input from storage files
class tests_07_storage (unittest.TestCase):

	@classmethod
	def setUpClass(cls):

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

			# Read in the YAML config file
			config_data = readConfig(os.path.join(cls.expected_path, 'testDB_large.yml'))

			# Assign the filename of the database
			cls.database_filename = os.path.join(cls.test_dir, config_data.sql_database)

			# Loop the tables
			for table in config_data:

				# Create the table
				createTable(cls.database_filename, table.name, table.assignment_str)

		# Set the data to None if that fails
		except:

			# Read in the config file
			cls.database_filename = None

	@classmethod
	def tearDownClass(cls):

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

		# Upload the Storage data
		addStorageFileToDatabase(self.database_filename, 'Storage', storage_filename)

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

		# Upload the Storage data
		updateStorageFileToDatabase(self.database_filename, 'Storage', 'Storage ID', storage_filename)

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

		# Get the IDs for the tests
		test_ID_1 = convertPlateWell(self.database_filename, 'Storage', 'Storage ID', test_plate_1, test_well_1)
		test_ID_2 = convertPlateWell(self.database_filename, 'Storage', 'Storage ID', test_plate_2, test_well_2)

		# Make sure we were able to get the correct IDs
		self.assertEqual(test_ID_1, '"DBtest-A1"')
		self.assertEqual(test_ID_2, '"DBtest-A4"')

if __name__ == "__main__":
	unittest.main(verbosity = 2)