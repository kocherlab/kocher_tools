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
from testing_functions import randomGenerator

# Run tests for config_file.py
class tests_03_config_file (unittest.TestCase):

	@classmethod
	def setUpClass(cls):

		# Create a temporary directory
		cls.test_dir = tempfile.mkdtemp()

		# Assign the script directory
		cls.script_dir = os.path.dirname(os.path.realpath(__file__))

		# Assign the expected output directory
		cls.expected_dir = 'test_files'

		# Assign the filename of the database
		cls.config_file = os.path.join(cls.script_dir, cls.expected_dir, 'testDB.yml')

		# Create empty variable to store the config data
		cls.config_data = None

	@classmethod
	def tearDownClass(cls):

		# Remove the test directory after the tests
		shutil.rmtree(cls.test_dir)

	# Check readConfig
	def test_01_readConfigs (self):

		# Read in the config file
		config_data = readConfig(self.config_file)

		# Check that the config data was assigned
		self.assertIsNotNone(config_data)

		# Update the config data for the other tests
		type(self).config_data = config_data

	# Check table iter function
	def test_02_tableIter (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Create list of the expected tables
		expected_tables = ['Table1', 'Table2', 'Table3']

		# Create list of the table in the config file
		test_tables = [str(table) for table in self.config_data]

		# Check that the table are the same
		self.assertEqual(expected_tables, test_tables)

	# Check table list function
	def test_03_tableList (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Create list of the expected tables
		expected_tables = ['Table1', 'Table2', 'Table3']

		# Check that the table are the same
		self.assertEqual(expected_tables, self.config_data.tables)

	# Check the find table function
	def test_04_tableFind (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Create list of the expected tables
		expected_tables = ['Table1', 'Table2', 'Table3']

		# Loop the expected tables
		for expected_table in expected_tables:

			self.assertTrue(expected_table in self.config_data)

		# Create list of the expected tables
		unexpected_tables = ['Table' + randomGenerator(), 'Table' + randomGenerator(), 'Table' + randomGenerator()]

		# Loop the unexpected tables
		for unexpected_table in unexpected_tables:

			self.assertFalse(unexpected_table in self.config_data)

	# Check the tableHasColumn function
	def test_05_tableHasColumn (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Create list of the expected columns
		expected_cols = ['"Unique ID"', '"Last Modified (Table1)"', '"Entry Created (Table1)"']
		expected_cols.extend(['"Unique ID"', 'Species', '"Last Modified (Table2)"', '"Entry Created (Table2)"'])
		expected_cols.extend(['Species', '"Last Modified (Table3)"', '"Entry Created (Table3)"'])

		# Loop the expected columns
		for expected_col in expected_cols:

			# Check if the column is within the config data
			self.assertTrue(self.config_data.hasColumn(expected_col))

		# Create list of the expected tables
		unexpected_cols = ['SubTest-' + randomGenerator(), 'SubTest-' + randomGenerator(), 'SubTest-' + randomGenerator()]

		# Loop the unexpected columns
		for unexpected_col in unexpected_cols:

			# Check if the column is within the config data
			self.assertFalse(self.config_data.hasColumn(unexpected_cols))

	# Check the returnColumnPath function
	def test_06_returnColumnPath (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Create list of the expected columns
		expected_cols = ['"Unique ID"', '"Last Modified (Table1)"', '"Entry Created (Table1)"']
		expected_cols.extend(['"Unique ID"', 'Species', '"Last Modified (Table2)"', '"Entry Created (Table2)"'])
		expected_cols.extend(['Species', '"Last Modified (Table3)"', '"Entry Created (Table3)"'])

		# Loop the expected columns
		for expected_col in expected_cols:

			self.assertIsNotNone(self.config_data.returnColumnPath(expected_col))

		# Check that the function fails with an unexpected column
		self.assertRaises(Exception, self.config_data.returnColumnPath, 'SubTest_' + randomGenerator())

	# Check the returnJoinLists function
	def test_07_returnJoinLists (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Assign the expected tables and columns
		expected_tables_to_join = ['Table1', 'Table2']
		expected_join_by_columns = ['"Unique ID"']

		# Assigned the test tables and columns for a simple join
		test_tables_to_join, test_join_by_columns = self.config_data.returnJoinLists(['Table1','Table2'])

		# Check that the table and columns are the same
		self.assertEqual(expected_tables_to_join, test_tables_to_join)
		self.assertEqual(expected_join_by_columns, test_join_by_columns)

		# Assign the expected tables and columns
		expected_tables_to_join = ['Table1', {'Table2': ['Table2', 'Table3']}]
		expected_join_by_columns = [{'"Unique ID"': ['Species']}]

		# Assigned the test tables and columns for a simple join
		test_tables_to_join, test_join_by_columns = self.config_data.returnJoinLists(['Table1','Table3'])

		# Check that the table and columns are the same
		self.assertEqual(expected_tables_to_join, test_tables_to_join)
		self.assertEqual(expected_join_by_columns, test_join_by_columns)

		# Check that the function fails with an unexpected table
		self.assertRaises(Exception, self.config_data.returnJoinLists, ['Table' + randomGenerator(),'Table' + randomGenerator()])

	# Check the returnColumnPathDict function
	def test_08_returnColumnPathDict (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Assign the expected path dict
		expected_path_dict = {'Table1."Unique ID"': ['Value1']}

		# Save the test path dict
		test_path_dict = self.config_data.returnColumnPathDict({'"Unique ID"':['Value1']})

		# Check that the path dicts are the same
		self.assertEqual(expected_path_dict, test_path_dict)

		# Check that the function fails with an unexpected column
		self.assertRaises(Exception, self.config_data.returnColumnPathDict, {'SubTest-' + randomGenerator():['Value1']})

	# Check the returnColumnDic function
	def test_09_returnColumnDict (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Assign the expected path dict
		expected_dict = {'"Unique ID"': ['Value1']}

		# Save the test path dict
		test_dict = self.config_data.returnColumnDict({'"Unique ID"':['Value1']}, 'Table1')

		# Check that the path dicts are the same
		self.assertEqual(expected_dict, test_dict)

		# Check that the function fails with an unexpected column
		self.assertRaises(Exception, self.config_data.returnColumnDict, {'SubTest-' + randomGenerator():['Value1']})

	# Check the returnTables function
	def test_10_returnTables (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Assign the expected tables
		expected_tables = set(['Table1', 'Table2'])

		# Assign the test tables
		test_tables = set(self.config_data.returnTables(['"Unique ID"', 'Species']))

		# Check that the tables are the same
		self.assertEqual(expected_tables, test_tables)

		# Check that the function fails with an unexpected column
		self.assertRaises(Exception, self.config_data.returnTables, ['SubTest-' + randomGenerator()])

	# Check the column iter function
	def test_11_columnIter (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Create list of the expected columns from each table
		expected_cols = [['"Unique ID"', '"Last Modified (Table1)"', '"Entry Created (Table1)"']]
		expected_cols.append(['"Unique ID"', 'Species', '"Last Modified (Table2)"', '"Entry Created (Table2)"'])
		expected_cols.append(['Species', '"From Nest?"', '"Last Modified (Table3)"', '"Entry Created (Table3)"'])

		# Iterate the tables in the config file
		for table_pos, table in enumerate(self.config_data):

			# Assign the test columns
			test_cols = [str(col) for col in table]

			# Check that the table are the same
			self.assertEqual(expected_cols[table_pos], test_cols)

	# Check the column iter function
	def test_12_columnList (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Create list of the expected columns from each table
		expected_cols = [['"Unique ID"', '"Last Modified (Table1)"', '"Entry Created (Table1)"']]
		expected_cols.append(['"Unique ID"', 'Species', '"Last Modified (Table2)"', '"Entry Created (Table2)"'])
		expected_cols.append(['Species', '"From Nest?"', '"Last Modified (Table3)"', '"Entry Created (Table3)"'])

		# Iterate the tables in the config file
		for table_pos, table in enumerate(self.config_data):

			# Check that the table are the same
			self.assertEqual(expected_cols[table_pos], table.columns)

		# Create list of the expected columns from each table
		expected_cols = [['Unique ID', 'Last Modified (Table1)', 'Entry Created (Table1)']]
		expected_cols.append(['Unique ID', 'Species', 'Last Modified (Table2)', 'Entry Created (Table2)'])
		expected_cols.append(['Species', 'From Nest?', 'Last Modified (Table3)', 'Entry Created (Table3)'])

		# Iterate the tables in the config file
		for table_pos, table in enumerate(self.config_data):

			# Check that the table are the same
			self.assertEqual(expected_cols[table_pos], table.unquoted_columns)

	# Check the find column function
	def test_13_columnFind (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Create list of the expected columns from each table
		expected_cols =     [['"Unique ID"', '"Last Modified (Table1)"', '"Entry Created (Table1)"']]
		expected_cols.append(['"Unique ID"', 'Species', '"Last Modified (Table2)"', '"Entry Created (Table2)"'])
		expected_cols.append(['Species', '"From Nest?"', '"Last Modified (Table3)"', '"Entry Created (Table3)"'])

		# Iterate the tables in the config file
		for table_pos, table in enumerate(self.config_data):

			# Loop the expected tables
			for expected_col in expected_cols[table_pos]:

				# Check that the current column is within the table
				self.assertTrue(expected_col in table)

			# Create list of the expected tables
			unexpected_cols = ['SubTest-' + randomGenerator()]

			# Loop the unexpected tables
			for unexpected_col in unexpected_cols:

				# Check that the current column isnt within the table
				self.assertFalse(unexpected_col in table)

	# Check the tableAssignmentStr function
	def test_14_tableAssignmentStr (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Create list of the expected strings for each table
		expected_strs =     ['"Unique ID" text, "Last Modified (Table1)" text, "Entry Created (Table1)" text']
		expected_strs.append('"Unique ID" text, Species text, "Last Modified (Table2)" text, "Entry Created (Table2)" text')
		expected_strs.append('Species text, "From Nest?" text, "Last Modified (Table3)" text, "Entry Created (Table3)" text')

		# Iterate the tables in the config file
		for table_pos, table in enumerate(self.config_data):

			# Check the strings are the same
			self.assertEqual(expected_strs[table_pos], table.assignment_str)

	# Check the retieveColumnPaths function
	def test_15_retieveColumnPaths (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Assign the expected paths
		expected_paths =     [['Table1."Unique ID"', 'Table1."Last Modified (Table1)"', 'Table1."Entry Created (Table1)"']]
		expected_paths.append(['Table2."Unique ID"', 'Table2.Species', 'Table2."Last Modified (Table2)"', 'Table2."Entry Created (Table2)"'])
		expected_paths.append(['Table3.Species', 'Table3."From Nest?"', 'Table3."Last Modified (Table3)"', 'Table3."Entry Created (Table3)"'])

		# Iterate the tables in the config file
		for table_pos, table in enumerate(self.config_data):

			# Assign the test paths
			test_paths = table.retieveColumnPaths()

			# Check the strings are the same
			self.assertEqual(expected_paths[table_pos], test_paths)

		# Assign the expected paths
		expected_paths = [['Table1.*'], ['Table2.*'], ['Table3.*']]

		# Iterate the tables in the config file
		for table_pos, table in enumerate(self.config_data):

			# Assign the test paths
			test_paths = table.retieveColumnPaths(keep_db_specific_columns = True)

			# Check the strings are the same
			self.assertEqual(expected_paths[table_pos], test_paths)

	# Return the columnAssignmentStr function
	def test_16_columnAssignmentStr (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Assign the expected paths
		expected_strs =     [['"Unique ID" text', '"Last Modified (Table1)" text', '"Entry Created (Table1)" text']]
		expected_strs.append(['"Unique ID" text', 'Species text', '"Last Modified (Table2)" text', '"Entry Created (Table2)" text'])
		expected_strs.append(['Species text', '"From Nest?" text', '"Last Modified (Table3)" text', '"Entry Created (Table3)" text'])

		# Loop the tables
		for table_pos, table in enumerate(self.config_data):

			# Loop the column
			for column_pos, column in enumerate(table):

				# Check the strings are the same
				self.assertEqual(expected_strs[table_pos][column_pos], column.assignment_str)

if __name__ == "__main__":
	unittest.main(verbosity = 2)
