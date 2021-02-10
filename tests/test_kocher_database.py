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

from kocher_tools.config_file import ConfigDB
from kocher_tools.database import *
from kocher_tools.kocher_database import *
from tests.functions import checkValue, updateConfigFilename
#from functions import checkValue

# Run tests for database input from collection files
class test_kocher_database (unittest.TestCase):

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

			'''
			Create a copy of the yml file, then update the sqlite
			filename to the test dir.
			'''
			config_filename = os.path.join(cls.expected_path, 'testDB_large.yml')
			cls.config_filename = os.path.join(cls.test_dir, 'testDB_large.yml')
			shutil.copy(config_filename, cls.config_filename)
			updateConfigFilename(cls.config_filename, os.path.join(cls.test_dir, 'testDB_large.sqlite'))

			# Create the tables
			db_config_data = ConfigDB.readConfig(cls.config_filename)
			sql_engine = createEngineFromConfig(db_config_data)
			createAllFromConfig(db_config_data, sql_engine)
			cls.database_filename = db_config_data.filename			

		# Set the data to None if that fails
		except:

			# Read in the config file
			cls.database_filename = None

	@classmethod
	def tearDownClass (cls):

		# Remove the test directory after the tests
		shutil.rmtree(cls.test_dir)

	# Check insertCollectionFileUsingConfig
	def test_01_insertCollectionFileUsingConfig (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the collection filename and insert
		collection_filename = os.path.join(self.expected_path, 'test_collection_01_input.tsv')
		insertCollectionFileUsingConfig(self.config_filename, 'collection', collection_filename)

		# Check that the values were correctly inserted 
		self.assertTrue(checkValue(self.database_filename, 'collection', 'unique_id', 'DBtest-0001'))
		self.assertTrue(checkValue(self.database_filename, 'collection', 'site_code', 'WIM'))
		self.assertTrue(checkValue(self.database_filename, 'collection', 'nest_code', 'N-1'))
		self.assertTrue(checkValue(self.database_filename, 'collection', 'sex', 'Multiple'))

	# Check addStorageFileToDatabase
	def test_02_insertStorageFileUsingConfig (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the storage filename and insert
		storage_filename = os.path.join(self.expected_path, 'test_storage_01_input.tsv')
		insertStorageFileUsingConfig(self.config_filename, storage_filename)

		# Check that the values were correctly inserted
		self.assertTrue(checkValue(self.database_filename, 'storage', 'unique_id', 'DBtest-0001'))
		self.assertTrue(checkValue(self.database_filename, 'storage', 'sample_id', 'DBtest-A2'))
		self.assertTrue(checkValue(self.database_filename, 'storage', 'plate', 'DBtest'))
		self.assertTrue(checkValue(self.database_filename, 'storage', 'well', 'B1'))
		self.assertTrue(checkValue(self.database_filename, 'plates', 'plate', 'DBtest'))
		self.assertTrue(checkValue(self.database_filename, 'plates', 'box', 'DBBox'))
		self.assertTrue(checkValue(self.database_filename, 'boxes', 'box', 'DBBox'))

	# Check insertBarcodeFilesUsingConfig
	def test_03_insertBarcodeFilesUsingConfig (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the barcode files and insert
		blast_filename = os.path.join(self.expected_path, 'test_barcode_01_input.out')
		fasta_filename = os.path.join(self.expected_path, 'test_barcode_01_input.fasta')
		json_filename = os.path.join(self.expected_path, 'test_barcode_01_input.json')
		insertBarcodeFilesUsingConfig(self.config_filename, 'sequencing', [blast_filename, fasta_filename, json_filename])

		# Check that the values were correctly inserted
		self.assertTrue(checkValue(self.database_filename, 'sequencing', 'sample_id', 'DBtest-A1'))
		self.assertTrue(checkValue(self.database_filename, 'sequencing', 'sequence_id', 'DBtest-A2_1'))
		self.assertTrue(checkValue(self.database_filename, 'sequencing', 'species', 'Lasioglossum oenotherae'))
		self.assertTrue(checkValue(self.database_filename, 'sequencing', 'sequence_status', 'Ambiguous Hits'))
