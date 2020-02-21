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
from kocher_tools.collection import readAppCollection, addAppCollectionToDatabase, updateAppCollectionToDatabase
from tests.functions import checkValue

# Run tests for database input from collection files
class test_collection (unittest.TestCase):

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

			# Loop the tables
			for table in config_data:

				# Create the table
				createTable(cls.database_filename, table.name, table.assignment_str)

		# Set the data to None if that fails
		except:

			# Read in the config file
			cls.database_filename = None

	@classmethod
	def tearDownClass (cls):

		# Remove the test directory after the tests
		shutil.rmtree(cls.test_dir)

	# Check readAppCollection
	def test_01_readAppCollection (self):

		# Assign the collection filename
		collection_filename = os.path.join(self.expected_path, 'test_collection_01_input.csv')

		# Assign the expected header names
		expected_header = ['Unique ID', 'Site Code', 'Date Collected', 'Time Entered', 'Sex', 
						   'Life Stage', 'Has Pollen?', 'Sample Preservation Method', 'Head Preserved',
						   'Head Preservation Method', 'From Nest?', 'Nest Code', 'Notes', 'Collection File']

		# Create a bool to indicate the header has been checked
		checked_header = False

		# Assign the expected rows
		expected_rows = 	[['DBtest-0001', 'RIM', '11/6/2019', '1:48:14 PM', 'Female', 'Adult', 
		                	  'No', 'EtoH', 'Yes', 'PFA', 'No', 'N/A', 'These are notes', 
		                	  'test_collection_01_input.csv']]
		expected_rows.append(['DBtest-0002', 'WIM', '11/6/2019', '2:17:47 PM', 'Male', 'Adult', 
							  'No', 'EtoH', 'No', 'N/A', 'No', 'N/A', 'Also notes ', 
							  'test_collection_01_input.csv'])
		expected_rows.append(['DBtest-0003', 'VEN', '11/6/2019', '2:18:47 PM', 'Unknown', 'Big larva', 
							  'No', 'RNAlater', 'No', 'N/A', 'Yes', 'N-1', 'From nest on Mars', 
							  'test_collection_01_input.csv'])
		expected_rows.append(['DBtest-0004', 'TOM', '11/6/2019', '2:19:57 PM', 'Multiple', 'Small larva', 
							  'No', 'RNAlater', 'No', 'N/A', 'Yes', 'Nest 21', 'All the larvae from this nest', 
							  'test_collection_01_input.csv'])

		# Parse the common file
		for line_pos, (header, row) in enumerate(readAppCollection(collection_filename)):
			
			# Check that the header file has not been checked
			if not checked_header:

				# Check that the header is what we expect
				self.assertEqual(header, expected_header)

				# Update the bool
				checked_header = True

			# Check that the current row is what we expect
			self.assertEqual(row, expected_rows[line_pos])
			
	# Check addAppCollectionToDatabase
	def test_02_addAppCollectionToDatabase (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the collection filename
		collection_filename = os.path.join(self.expected_path, 'test_collection_01_input.csv')

		# Add the data to the database
		addAppCollectionToDatabase(self.database_filename, 'Collection', collection_filename)

		# Check that the values were correctly inserted 
		self.assertTrue(checkValue(self.database_filename, 'Collection', '"Unique ID"', '"DBtest-0001"'))
		self.assertTrue(checkValue(self.database_filename, 'Collection', '"Site Code"', 'WIM'))
		self.assertTrue(checkValue(self.database_filename, 'Collection', '"Nest Code"', '"N-1"'))
		self.assertTrue(checkValue(self.database_filename, 'Collection', 'Sex', 'Multiple'))

	# Check updateAppCollectionToDatabase
	def test_03_updateAppCollectionToDatabase (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the collection filename
		collection_filename = os.path.join(self.expected_path, 'test_collection_02_input.csv')

		# Add the data to the database
		updateAppCollectionToDatabase(self.database_filename, 'Collection', "Unique ID", collection_filename)

		# Check that the values were correctly inserted
		self.assertTrue(checkValue(self.database_filename, 'Collection', '"Has Pollen?"', 'Yes'))
		self.assertTrue(checkValue(self.database_filename, 'Collection', '"Site Code"', 'BAS'))
		self.assertTrue(checkValue(self.database_filename, 'Collection', '"Nest Code"', '"F-21"'))
		self.assertTrue(checkValue(self.database_filename, 'Collection', 'Sex', 'N/A'))

if __name__ == "__main__":
	unittest.main(verbosity = 2)