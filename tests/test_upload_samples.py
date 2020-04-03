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
import multiprocessing
import pkg_resources
import subprocess

from unittest.mock import patch

from kocher_tools.upload_samples import *
from kocher_tools.database import createTable
from kocher_tools.config_file import readConfig
from tests.functions import checkValue

# Run tests for upload_samples.py
class test_upload_samples (unittest.TestCase):

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

			# Assign the path of the existing backup dir
			existing_backup_dir = os.path.join(cls.expected_path, 'TestBackups')

			# Assign the path of the test backup dir
			cls.test_backup_dir = os.path.join(cls.test_dir, 'TestBackups')

			# Copy the backup dir
			shutil.copytree(existing_backup_dir, cls.test_backup_dir)

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

			# Set the fienames of the database and config file to None
			cls.database_filename = None
			cls.config_filename = None

	@classmethod
	def tearDownClass (cls):

		# Remove the test directory after the tests
		shutil.rmtree(cls.test_dir)

	# Check upload_samples main function
	def test_01_main (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None or self.config_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		
		# Collection App Tests

		# Assign the collection filename
		collection_filename = os.path.join(self.expected_path, 'test_collection_01_input.csv')

		# Assign the upload_samples args
		upload_args = [sys.argv[0], '--sqlite-db', self.database_filename, '--yaml', self.config_filename, '--app-file', collection_filename]

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', upload_args):

			# Call the function
			main()

			# Check that the values were correctly inserted 
			self.assertTrue(checkValue(self.database_filename, 'Collection', '"Unique ID"', '"DBtest-0001"'))
			self.assertTrue(checkValue(self.database_filename, 'Collection', '"Site Code"', 'WIM'))
			self.assertTrue(checkValue(self.database_filename, 'Collection', '"Nest Code"', '"N-1"'))
			self.assertTrue(checkValue(self.database_filename, 'Collection', 'Sex', 'Multiple'))

			# Check that a backup was created
			self.assertEqual(len(os.listdir(self.test_backup_dir)), 2)

		# Assign the collection filename
		collection_filename = os.path.join(self.expected_path, 'test_collection_02_input.csv')

		# Assign the upload_samples args
		upload_args = [sys.argv[0], '--sqlite-db', self.database_filename, '--yaml', self.config_filename, '--app-file', collection_filename, '--update-with-file']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', upload_args):

			# Call the function
			main()

			# Check that the values were correctly inserted
			self.assertTrue(checkValue(self.database_filename, 'Collection', '"Has Pollen?"', 'Yes'))
			self.assertTrue(checkValue(self.database_filename, 'Collection', '"Site Code"', 'BAS'))
			self.assertTrue(checkValue(self.database_filename, 'Collection', '"Nest Code"', '"F-21"'))
			self.assertTrue(checkValue(self.database_filename, 'Collection', 'Sex', 'N/A'))

			# Check that a backup wasn't created
			self.assertEqual(len(os.listdir(self.test_backup_dir)), 2)

		# Storage Tests

		# Assign the storage filename
		storage_filename = os.path.join(self.expected_path, 'test_storage_01_input.tsv')

		# Assign the upload_samples args
		upload_args = [sys.argv[0], '--sqlite-db', self.database_filename, '--yaml', self.config_filename, '--storage-file', storage_filename]

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', upload_args):

			# Call the function
			main()

			# Check that the values were correctly inserted
			self.assertTrue(checkValue(self.database_filename, 'Storage', '"Unique ID"', '"DBtest-0001"'))
			self.assertTrue(checkValue(self.database_filename, 'Storage', '"Storage ID"', '"DBtest-A2"'))
			self.assertTrue(checkValue(self.database_filename, 'Storage', 'Plate', 'DBtest'))
			self.assertTrue(checkValue(self.database_filename, 'Storage', 'Well', 'B1'))

		# Assign the storage filename
		storage_filename = os.path.join(self.expected_path, 'test_storage_02_input.tsv')

		# Assign the upload_samples args
		upload_args = [sys.argv[0], '--sqlite-db', self.database_filename, '--yaml', self.config_filename, '--storage-file', storage_filename, '--update-with-file']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', upload_args):

			# Call the function
			main()

			# Check that the values were correctly inserted
			self.assertTrue(checkValue(self.database_filename, 'Storage', 'Plate', 'DBtest2'))
			self.assertTrue(checkValue(self.database_filename, 'Storage', 'Well', 'A4'))


		# Location Tests

		# Assign the location filename
		loc_filename = os.path.join(self.expected_path, 'test_location_01_input.tsv')

		# Assign the upload_samples args
		upload_args = [sys.argv[0], '--sqlite-db', self.database_filename, '--yaml', self.config_filename, '--location-file', loc_filename]

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', upload_args):

			# Call the function
			main()

			# Check that the values were correctly inserted
			self.assertTrue(checkValue(self.database_filename, 'Locations', '"Site Code"', 'TOM'))
			self.assertTrue(checkValue(self.database_filename, 'Locations', 'Location', '"Outer Rim"'))
			self.assertTrue(checkValue(self.database_filename, 'Locations', 'GPS', '"40.34555268323508, -74.65406448370945"'))

		# Assign the location filename
		loc_filename = os.path.join(self.expected_path, 'test_location_02_input.tsv')

		# Assign the upload_samples args
		upload_args = [sys.argv[0], '--sqlite-db', self.database_filename, '--yaml', self.config_filename, '--location-file', loc_filename, '--update-with-file']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', upload_args):

			# Call the function
			main()

			# Check that the values were correctly inserted
			self.assertTrue(checkValue(self.database_filename, 'Locations', 'Location', '"Inner Rim"'))
			self.assertTrue(checkValue(self.database_filename, 'Locations', 'GPS', '"50.123, -74.50.123"'))

		# Assign the collection filename
		collection_filename = os.path.join(self.expected_path, 'test_location_03_input.csv')

		# Assign the upload_samples args
		upload_args = [sys.argv[0], '--sqlite-db', self.database_filename, '--yaml', self.config_filename, '--app-file', collection_filename, '--app-upload-method', 'Location']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', upload_args):

			# Call the function
			main()

			# Check that the values were correctly inserted 
			self.assertTrue(checkValue(self.database_filename, 'Locations', '"Site Code"', 'RIM2'))
			self.assertTrue(checkValue(self.database_filename, 'Locations', 'GPS', '"40.00, -74.00"'))

		# Assign the collection filename
		collection_filename = os.path.join(self.expected_path, 'test_location_04_input.csv')

		# Assign the upload_samples args
		upload_args = [sys.argv[0], '--sqlite-db', self.database_filename, '--yaml', self.config_filename, '--app-file', collection_filename, '--app-upload-method', 'Location', '--update-with-file']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', upload_args):

			# Call the function
			main()

			# Check that the values were correctly inserted
			self.assertTrue(checkValue(self.database_filename, 'Locations', 'GPS', '"50, -75"'))


		# Barcode Tests

		# Assign the blast filename
		blast_filename = os.path.join(self.expected_path, 'test_barcode_01_input.out')
		fasta_filename = os.path.join(self.expected_path, 'test_barcode_01_input.fasta')
		json_filename = os.path.join(self.expected_path, 'test_barcode_01_input.json')

		# Assign the upload_samples args
		upload_args = [sys.argv[0], '--sqlite-db', self.database_filename, '--yaml', self.config_filename, '--barcode-all-files', blast_filename, fasta_filename, json_filename]

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', upload_args):

			# Call the function
			main()

			# Check that the values were correctly inserted
			self.assertTrue(checkValue(self.database_filename, 'Sequencing', '"Storage ID"', '"DBtest-A1"'))
			self.assertTrue(checkValue(self.database_filename, 'Sequencing', '"Sequence ID"', '"DBtest-A2_1"'))
			self.assertTrue(checkValue(self.database_filename, 'Sequencing', 'Species', '"Lasioglossum oenotherae"'))
			self.assertTrue(checkValue(self.database_filename, 'Sequencing', 'Status', '"Ambiguous Hits"'))

		# Assign the blast filename
		blast_filename = os.path.join(self.expected_path, 'test_barcode_02_input.out')
		fasta_filename = os.path.join(self.expected_path, 'test_barcode_02_input.fasta')
		json_filename = os.path.join(self.expected_path, 'test_barcode_02_input.json')

		# Assign the upload_samples args
		upload_args = [sys.argv[0], '--sqlite-db', self.database_filename, '--yaml', self.config_filename, '--barcode-all-files', blast_filename, fasta_filename, json_filename, '--update-with-file']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', upload_args):

			# Call the function
			main()

			# Check that the values were correctly inserted
			self.assertTrue(checkValue(self.database_filename, 'Sequencing', '"Ambiguous Hits"', None))
			self.assertTrue(checkValue(self.database_filename, 'Sequencing', '"Sequence ID"', '"DBtest-A2_1"'))
			self.assertTrue(checkValue(self.database_filename, 'Sequencing', 'Species', '"Ceratina strenua"'))
			self.assertTrue(checkValue(self.database_filename, 'Sequencing', 'Status', '"No Hits"'))


		# Basic Tests

		# Assign the upload_samples args
		upload_args = [sys.argv[0], '--sqlite-db', self.database_filename, '--yaml', self.config_filename, '--include-ID', 'DBtest-0002', '--update', 'Species', 'Unknown Species']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', upload_args):

			# Call the function
			main()

			# Check that the values were correctly inserted
			self.assertTrue(checkValue(self.database_filename, 'Sequencing', 'Species', '"Unknown Species"'))

		# Assign the upload_samples args
		upload_args = [sys.argv[0], '--sqlite-db', self.database_filename, '--yaml', self.config_filename, '--exclude-ID', 'DBtest-0002', '--update', 'Species Guess', 'Apis mellifera']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', upload_args):

			# Call the function
			main()

			# Check that the values were correctly inserted
			self.assertTrue(checkValue(self.database_filename, 'Collection', '"Species Guess"', '"Apis mellifera"', expected_count = 3))

		# Assign the upload_samples args
		upload_args = [sys.argv[0], '--sqlite-db', self.database_filename, '--yaml', self.config_filename, '--include-species', 'Ceratina strenua', '--update', 'Collected By', 'AEW_02']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', upload_args):

			# Call the function
			main()

			# Check that the values were correctly inserted
			self.assertTrue(checkValue(self.database_filename, 'Collection', '"Collected By"', 'AEW_02'))

		# Assign the upload_samples args
		upload_args = [sys.argv[0], '--sqlite-db', self.database_filename, '--yaml', self.config_filename, '--exclude-species', 'Ceratina strenua', '--update', 'Collected By', 'AEW_03']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', upload_args):

			# Call the function
			main()

			# Check that the values were correctly inserted
			self.assertTrue(checkValue(self.database_filename, 'Collection', '"Collected By"', 'AEW_03'))


		# Assign the upload_samples args
		upload_args = [sys.argv[0], '--sqlite-db', self.database_filename, '--yaml', self.config_filename, '--include-genus', 'Ceratina', '--update', 'Collected By', 'AEW_04']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', upload_args):

			# Call the function
			main()

			# Check that the values were correctly inserted
			self.assertTrue(checkValue(self.database_filename, 'Collection', '"Collected By"', 'AEW_04'))

		# Assign the upload_samples args
		upload_args = [sys.argv[0], '--sqlite-db', self.database_filename, '--yaml', self.config_filename, '--exclude-genus', 'Ceratina', '--update', 'Collected By', 'AEW_05']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', upload_args):

			# Call the function
			main()

			# Check that the values were correctly inserted
			self.assertTrue(checkValue(self.database_filename, 'Collection', '"Collected By"', 'AEW_05'))


		# Assign the upload_samples args
		upload_args = [sys.argv[0], '--sqlite-db', self.database_filename, '--yaml', self.config_filename, '--include-nests', '--update', 'Collected By', 'AEW_06']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', upload_args):

			# Call the function
			main()

			# Check that the values were correctly inserted
			self.assertTrue(checkValue(self.database_filename, 'Collection', '"Collected By"', 'AEW_06', expected_count = 2))

		# Assign the upload_samples args
		upload_args = [sys.argv[0], '--sqlite-db', self.database_filename, '--yaml', self.config_filename, '--exclude-nests', '--update', 'Collected By', 'AEW_07']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', upload_args):

			# Call the function
			main()

			# Check that the values were correctly inserted
			self.assertTrue(checkValue(self.database_filename, 'Collection', '"Collected By"', 'AEW_07', expected_count = 2))


		# Assign the upload_samples args
		upload_args = [sys.argv[0], '--sqlite-db', self.database_filename, '--yaml', self.config_filename, '--include', 'Sex', 'Male', '--update', 'Collected By', 'AEW_08']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', upload_args):

			# Call the function
			main()

			# Check that the values were correctly inserted
			self.assertTrue(checkValue(self.database_filename, 'Collection', '"Collected By"', 'AEW_08'))

		# Assign the upload_samples args
		upload_args = [sys.argv[0], '--sqlite-db', self.database_filename, '--yaml', self.config_filename, '--exclude', 'Sex', 'Male', '--update', 'Collected By', 'AEW_09']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', upload_args):

			# Call the function
			main()

			# Check that the values were correctly inserted
			self.assertTrue(checkValue(self.database_filename, 'Collection', '"Collected By"', 'AEW_09', expected_count = 3))







		




			
if __name__ == "__main__":
	unittest.main(verbosity = 2)