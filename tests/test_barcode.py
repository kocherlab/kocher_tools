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

from Bio import SeqIO

from kocher_tools.database import createTable
from kocher_tools.config_file import readConfig
from kocher_tools.storage import convertPlateWell, addStorageFileToDatabase
from kocher_tools.barcode import assignStorageIDs, readSeqFiles, addSeqFilesToDatabase, updateSeqFilesToDatabase
from tests.functions import checkValue

# Run tests for database input from barcode files
class test_barcode (unittest.TestCase):

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

			# Read in the YAML config file
			config_data = readConfig(os.path.join(cls.expected_path, 'testDB_large.yml'))

			# Assign the filename of the database
			cls.database_filename = os.path.join(cls.test_dir, config_data.sql_database)

			# Loop the tables
			for table in config_data:

				# Create the table
				createTable(cls.database_filename, table.name, table.assignment_str)

			# Assign the storage filename
			storage_filename = os.path.join(cls.expected_path, 'test_storage_02_input.tsv')

			# Upload the Storage data
			addStorageFileToDatabase(cls.database_filename, 'Storage', storage_filename)

		# Set the data to None if that fails
		except:

			# Read in the config file
			cls.database_filename = None

	@classmethod
	def tearDownClass (cls):

		# Remove the test directory after the tests
		shutil.rmtree(cls.test_dir)

	# Check assignStorageIDs
	def test_01_assignStorageIDs (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the blast filename
		blast_filename = os.path.join(self.expected_path, 'test_barcode_01_input.out')

		# Assign the json filename
		json_filename = os.path.join(self.expected_path, 'test_barcode_01_input.json')

		# Save the expected dict
		expected_dict = {'DBtest-A1': '"DBtest-A1"', 'DBtest-A2': '"DBtest-A2"'}

		# Assign the storage IDs
		test_dict = assignStorageIDs(self.database_filename, 'Storage', 'Storage ID', blast_filename, json_filename)

		# Make sure we were able to get the correct IDs
		self.assertEqual(test_dict, expected_dict)

	# Check readSeqFiles
	def test_02_readSeqFiles (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the blast filename
		blast_filename = os.path.join(self.expected_path, 'test_barcode_01_input.out')

		# Assign the fasta filename
		fasta_filename = os.path.join(self.expected_path, 'test_barcode_01_input.fasta')

		# Assign the json filename
		json_filename = os.path.join(self.expected_path, 'test_barcode_01_input.json')

		# Assign the storage IDs
		assigned_IDs = assignStorageIDs(self.database_filename, 'Storage', 'Storage ID', blast_filename, json_filename)

		# Save a list of the expected headers
		expected_headers = [['Storage ID', 'Sequence ID', 'Species', 'Reads', 'BOLD Identifier', 'Percent Identity', 'Alignment Length', 'Sequence Length', 'Status'],
							['Storage ID', 'Sequence ID', 'Reads', 'Status', 'Ambiguous Hits', 'BOLD Bins']]

		# Save a list of the expected rows
		expected_rows = [['"DBtest-A1"', 'DBtest-A1', 'Lasioglossum oenotherae', '500', 'ASIO003-06', '100.000', '313', '313', 'Species Identified'],
						 ['"DBtest-A2"', 'DBtest-A2', '500', 'Ambiguous Hits', 'Ceratina mikmaqi, Ceratina strenua', 'BOLD:AAA2368']]

		# Index the sequence file
		sequence_index = SeqIO.index(fasta_filename, 'fasta')

		# Loop the header and row for the test files
		for test_pos, (header, row) in enumerate(readSeqFiles(blast_filename, sequence_index, json_filename, assigned_IDs, 'Storage ID')):

			# Loop the column and value pairs for the expected results
			for expected_column, expected_value in zip(expected_headers[test_pos], expected_rows[test_pos]):

				# Check if the current column is within the test results
				if expected_column in header:

					# Assign the index of the expected column
					expected_column_index = header.index(expected_column)

					# Check if we get the expected value from the test results
					self.assertEqual(row[expected_column_index], expected_value)

				# Check if the current column isn't within the test results
				else:

					# Mark a failure if the expected column isn't found
					self.assertTrue(False)

		sequence_index.close()	

	# Check addSeqFilesToDatabase
	def test_03_addSeqFilesToDatabase (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the blast filename
		blast_filename = os.path.join(self.expected_path, 'test_barcode_01_input.out')

		# Assign the fasta filename
		fasta_filename = os.path.join(self.expected_path, 'test_barcode_01_input.fasta')

		# Assign the json filename
		json_filename = os.path.join(self.expected_path, 'test_barcode_01_input.json')

		# Add the data to the database
		addSeqFilesToDatabase(self.database_filename, 'Sequencing', blast_filename, fasta_filename, json_filename, 'Storage', 'Storage ID')

		# Check that the values were correctly inserted
		self.assertTrue(checkValue(self.database_filename, 'Sequencing', '"Storage ID"', '"DBtest-A1"'))
		self.assertTrue(checkValue(self.database_filename, 'Sequencing', '"Sequence ID"', '"DBtest-A2"'))
		self.assertTrue(checkValue(self.database_filename, 'Sequencing', 'Species', '"Lasioglossum oenotherae"'))
		self.assertTrue(checkValue(self.database_filename, 'Sequencing', 'Status', '"Ambiguous Hits"'))

	# Check updateSeqFilesToDatabase
	def test_04_updateSeqFilesToDatabase (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the blast filename
		blast_filename = os.path.join(self.expected_path, 'test_barcode_02_input.out')

		# Assign the fasta filename
		fasta_filename = os.path.join(self.expected_path, 'test_barcode_02_input.fasta')

		# Assign the json filename
		json_filename = os.path.join(self.expected_path, 'test_barcode_02_input.json')

		# Update the data to the database
		updateSeqFilesToDatabase(self.database_filename, 'Sequencing', "Sequence ID", blast_filename, fasta_filename, json_filename, 'Storage', 'Storage ID')

		# Check that the values were correctly inserted
		self.assertTrue(checkValue(self.database_filename, 'Sequencing', '"Ambiguous Hits"', None))
		self.assertTrue(checkValue(self.database_filename, 'Sequencing', '"Sequence ID"', '"DBtest-A2"'))
		self.assertTrue(checkValue(self.database_filename, 'Sequencing', 'Species', '"Ceratina strenua"'))
		self.assertTrue(checkValue(self.database_filename, 'Sequencing', 'Status', '"No Hits"'))

if __name__ == "__main__":
	unittest.main(verbosity = 2)