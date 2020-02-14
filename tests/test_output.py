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

from kocher_tools.database import *
from kocher_tools.output import *
from tests.functions import fileComp, stdoutToStr

# Run tests for output.py
class test_output (unittest.TestCase):

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

		# Assign the filename of the database
		cls.database = os.path.join(cls.script_dir, cls.expected_dir, 'testDB.sqlite')

		# Retrieve values from the database
		cls.retrieved_entries = retrieveValues(cls.database, ['Table1', 'Table2'], {'IN':{'Table1."Unique ID"': ['Value1']}}, ['Table1."Unique ID"', 'Table2.Species'], join_table_columns = ['"Unique ID"'])

	@classmethod
	def tearDownClass (cls):

		# Remove the test directory after the tests
		shutil.rmtree(cls.test_dir)

	# Check entriesToFile
	def test_01_entriesToFile (self):

		# Create the filename for the comparison file
		retrieveValues_test_filename = os.path.join(self.test_dir, 'entriesToFile_comp.tsv')

		# Create the filename of the expected output file
		retrieveValues_expt_filename = os.path.join(self.expected_path,'test_output_1_expected_output.tsv')

		# Create the file
		entriesToFile(self.retrieved_entries, retrieveValues_test_filename, '\t')

		# Compare the files
		self.assertTrue(fileComp(retrieveValues_test_filename, retrieveValues_expt_filename))

	# Check entriesToScreen
	def test_02_entriesToScreen (self):

		# Run the command, with stdout converted to a string
		test_str = stdoutToStr(entriesToScreen, self.retrieved_entries, '\t')

		# Assign the expected string
		expected_str = 'Unique ID\tSpecies\nValue1\tValue2\n'

		self.assertEqual(test_str, expected_str)

if __name__ == "__main__":
	unittest.main(verbosity = 2)
