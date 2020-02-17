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

from kocher_tools.config_file import readConfig
from kocher_tools.assignment import assignTables, assignSelectionDict

# Run tests for assignment.py
class test_assignment (unittest.TestCase):

	@classmethod
	def setUpClass (cls):

		# Create a temporary directory
		cls.test_dir = tempfile.mkdtemp()

		# Assign the script directory
		cls.script_dir = os.path.dirname(os.path.realpath(__file__))

		# Assign the expected output directory
		cls.expected_dir = 'test_files'

		# Assign the filename of the database
		cls.config_file = os.path.join(cls.script_dir, cls.expected_dir, 'testDB.yml')

		# Try loading the config file
		try:

			# Read in the config file
			cls.config_data = readConfig(cls.config_file)

		# Set the data to None if that fails
		except:

			# Read in the config file
			cls.config_data = None

	@classmethod
	def tearDownClass (cls):

		# Remove the test directory after the tests
		shutil.rmtree(cls.test_dir)

	# Check assignTables 
	def test_01_assignTables (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires working config file to operate. Check config tests for errors')

		# Create test keyword dicts
		test_01 = {'include_ID': ['TestID']}
		test_02 = {'include_species': ['TestSpecies']}
		test_03 = {'include_ID': ['TestID'], 'exclude_species': ['TestSpecies']}
		test_04 = {'exclude_ID': ['TestID'], 'include_species': ['TestSpecies']}
		test_05 = {'include_nests': True}
		test_06 = {'include_species': 'TestSpecies', 'include_nests': True}

		# Create data of the expected results
		expected_01 = ['Table1']
		expected_02 = ['Table2']
		expected_03 = set(['Table1', 'Table2'])
		expected_04 = set(['Table1', 'Table2'])
		expected_05 = ['Table3']
		expected_06 = set(['Table2', 'Table3'])

		# Test which tables are returned
		self.assertEqual(assignTables(self.config_data, **test_01), expected_01)
		self.assertEqual(assignTables(self.config_data, **test_02), expected_02)
		self.assertEqual(set(assignTables(self.config_data, **test_03)), expected_03)
		self.assertEqual(set(assignTables(self.config_data, **test_04)), expected_04)
		self.assertEqual(assignTables(self.config_data, **test_05), expected_05)
		self.assertEqual(set(assignTables(self.config_data, **test_06)), expected_06)

	# Check assignSelectionDict
	def test_02_assignSelectionDict (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires working config file to operate. Check config tests for errors')

		# Create test keyword dicts
		test_01 = {'include_ID': ['TestID']}
		test_02 = {'include_species': ['TestSpecies']}
		test_03 = {'include_ID': ['TestID'], 'exclude_species': ['TestSpecies']}
		test_04 = {'exclude_ID': ['TestID'], 'include_species': ['TestSpecies']}
		test_05 = {'include_nests': True}
		test_06 = {'include_species': ['TestSpecies'], 'exclude_nests': True}

		# Create data of the expected results
		expected_01 = {'IN': {'Table1."Unique ID"': ['TestID']}}
		expected_02 = {'IN': {'Table2.Species': ['TestSpecies']}}
		expected_03 = {'IN': {'Table1."Unique ID"': ['TestID']}, 'NOT IN': {'Table2.Species': ['TestSpecies']}}
		expected_04 = {'NOT IN': {'Table1."Unique ID"': ['TestID']}, 'IN': {'Table2.Species': ['TestSpecies']}}
		expected_05 = {'IN': {'Table3."From Nest?"': ['Yes']}}
		expected_06 = {'IN': {'Table2.Species': ['TestSpecies']}, 'NOT IN': {'Table3."From Nest?"': ['Yes']}}

		# Test which tables are returned
		self.assertEqual(assignSelectionDict(self.config_data, **test_01), expected_01)
		self.assertEqual(assignSelectionDict(self.config_data, **test_02), expected_02)
		self.assertEqual(assignSelectionDict(self.config_data, **test_03), expected_03)
		self.assertEqual(assignSelectionDict(self.config_data, **test_04), expected_04)
		self.assertEqual(assignSelectionDict(self.config_data, **test_05), expected_05)
		self.assertEqual(assignSelectionDict(self.config_data, **test_06), expected_06)

if __name__ == "__main__":
	unittest.main(verbosity = 2)