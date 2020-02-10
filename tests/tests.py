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

from kocher_tools.database import *
from kocher_tools.output import *
from kocher_tools.config_file import readConfig
from kocher_tools.assignment import assignTables, assignSelectionDict
from kocher_tools.common import readCommonFile
from kocher_tools.collection import readAppCollection, addAppCollectionToDatabase, updateAppCollectionToDatabase
from kocher_tools.storage import convertPlateWell, addStorageFileToDatabase, updateStorageFileToDatabase
from kocher_tools.barcode import assignStorageIDs, readSeqFiles, addSeqFilesToDatabase, updateSeqFilesToDatabase
from kocher_tools.location import addLocFileToDatabase, updateLocFileToDatabase, convertLoc, readAppLocations, addAppLocationsToDatabase, updateAppLocationsToDatabase
from kocher_tools.misc import confirmExecutable

def checkTable (database, table):
	
	try:
		
		# Create the table check string
		sqlite_check_table = 'SELECT COUNT(*) FROM sqlite_master WHERE type=? AND name=?'

		# Connect to the sqlite database
		sqlite_connection = sqlite3.connect(database)

		# Create the cursor
		cursor = sqlite_connection.cursor()

		# Execute the create table command
		cursor.execute(sqlite_check_table, ['table', table])

		# Save the selected results
		selection_results = cursor.fetchall()

		# Commit any changes
		sqlite_connection.commit()

		# Close the connection
		cursor.close()

		# Check if there was an assignment error
		if not selection_results:
			raise Exception('Assignment error')

		# Check if the table was found
		if selection_results[0][0] == 1:

			# Return True
			return True

		# Check if the table was not found
		elif selection_results[0][0] == 0:

			# Return False
			return False

		# Check if there was an unknown error
		else:
			raise Exception('Unknown error')

	except sqlite3.Error as error:
		raise Exception(error)

def checkColumn (database, table, column):
	
	try:
		
		# Create the column check string
		sqlite_check_column = 'SELECT COUNT(*) FROM pragma_table_info(?) WHERE name=?'

		# Connect to the sqlite database
		sqlite_connection = sqlite3.connect(database)

		# Create the cursor
		cursor = sqlite_connection.cursor()

		# Execute the create table command
		cursor.execute(sqlite_check_column, [table, column])

		# Save the selected results
		selection_results = cursor.fetchall()

		# Commit any changes
		sqlite_connection.commit()

		# Close the connection
		cursor.close()

		# Check if there was an assignment error
		if not selection_results:
			raise Exception('Assignment error')

		# Check if the table was found
		if selection_results[0][0] == 1:

			# Return True
			return True

		# Check if the table was not found
		elif selection_results[0][0] == 0:

			# Return False
			return False

		# Check if there was an unknown error
		else:
			raise Exception('Unknown error')

	except sqlite3.Error as error:
		raise Exception(error)

def checkValue (database, table, column, value):
	
	try:

		# Connect to the sqlite database
		sqlite_connection = sqlite3.connect(database)

		# Create the cursor
		cursor = sqlite_connection.cursor()

		# Check if the value is None
		if value == None:
			
			# Create the value check string
			sqlite_check_value = 'SELECT COUNT(*) FROM %s WHERE %s is NULL' % (table, column)

			# Execute the create table command
			cursor.execute(sqlite_check_value)

		else:
		
			# Create the value check string
			sqlite_check_value = 'SELECT COUNT(*) FROM %s WHERE %s = ?' % (table, column)

			# Execute the create table command
			cursor.execute(sqlite_check_value, [value])

		# Save the selected results
		selection_results = cursor.fetchall()

		# Commit any changes
		sqlite_connection.commit()

		# Close the connection
		cursor.close()

		# Check if there was an assignment error
		if not selection_results:
			raise Exception('Assignment error')

		# Check if the table was found
		if selection_results[0][0] >= 1:

			# Return True
			return True

		# Check if the table was not found
		elif selection_results[0][0] == 0:

			# Return False
			return False

		# Check if there was an unknown error
		else:

			raise Exception('Unknown error')

	except sqlite3.Error as error:
		raise Exception(error)

def fileComp (test_output, expected_output):

	# Compare two files, return bool
	return filecmp.cmp(test_output, expected_output)

def randomGenerator (length = 10, char_set = string.ascii_uppercase + string.digits):

	# Return a random string of letters and numbers
	return ''.join(random.choice(char_set) for i in range(length))

# Run tests for database.py
class testset_01_database (unittest.TestCase):

	@classmethod
	def setUpClass(self):

		# Create a temporary directory
		self.test_dir = tempfile.mkdtemp()

		# Assign the expected output directory
		self.expected_dir = 'test_files'

		# Assign the filename of the database
		self.database = os.path.join(self.test_dir, 'testDB.sqlite')

	@classmethod
	def tearDownClass(self):

		# Remove the test directory after the tests
		shutil.rmtree(self.test_dir)

	# Check valueMarksStr
	def test_01_database_valueMarksStr (self):

		# Run Tests
		self.assertEqual(valueMarksStr(['Test-1']), '?')
		self.assertEqual(valueMarksStr(['Test-1', '"Test 2"']), '?, ?')
		self.assertEqual(valueMarksStr(['Test-1', '"Test 2"', 'Test3']), '?, ?, ?')

	# Check quoteStr
	def test_02_database_quoteStr (self):
		
		# Run Tests
		self.assertEqual(quoteStr('Test-1'), '"Test-1"')
		self.assertEqual(quoteStr('"Test 2"'), '"Test 2"')
		self.assertEqual(quoteStr('Test3'), 'Test3')

	# Check quoteField
	def test_03_database_quoteField (self):
		
		# Run Tests
		self.assertEqual(quoteField('Test-1'), '"Test-1"')
		self.assertEqual(quoteField('"Test 2"'), '"Test 2"')
		self.assertEqual(quoteField('Test3'), 'Test3')
		self.assertEqual(quoteField('Test-1.Unique ID'),'"Test-1"."Unique ID"')
		self.assertEqual(quoteField('Test-1.Species'), '"Test-1".Species')
		self.assertEqual(quoteField('Test-1.SubTest3'), '"Test-1".SubTest3')
		self.assertEqual(quoteField('Test. 4', split_by_dot = False), '"Test. 4"')

	# Check quoteFields
	def test_04_database_quoteFields (self):
		
		# Run Tests
		self.assertEqual(quoteFields(['Test-1', "Test 2"]), ['"Test-1"', '"Test 2"'])
		self.assertEqual(quoteFields(['Test-1', "Test 2", 'Test3', 'Test. 4'], split_by_dot = False), ['"Test-1"', '"Test 2"', 'Test3', '"Test. 4"'])

	# Check returnSetExpression
	def test_05_database_returnSetExpression (self):
		
		# Run Tests
		self.assertEqual(returnSetExpression({'Test-1': 1, '"Test 2"': 2, 'Test3': 3}), '"Test-1" = ?, "Test 2" = ?, Test3 = ?')

	# Check returnSelectionDict
	def test_06_database_returnSelectionDict (self):
		
		# Run Tests
		self.assertEqual(returnSelectionDict({'IN': {'Test-1':['Unique ID']}}), '"Test-1" IN (?)')
		self.assertEqual(returnSelectionDict({'NOT IN': {'Test-1':['Unique ID', 'Species']}}), '"Test-1" NOT IN (?, ?)')
		self.assertEqual(returnSelectionDict({'IN': {'Test-1':['Unique ID'], '"Test 2"':['Species']}}), '"Test-1" IN (?) AND "Test 2" IN (?)')
		self.assertEqual(returnSelectionDict({'LIKE': {'Test-1':['Unique ID']}}), '"Test-1" LIKE ?')
		self.assertEqual(returnSelectionDict({'NOT LIKE': {'Test-1':['Unique ID', 'Species']}}), '("Test-1" NOT LIKE ? OR "Test-1" NOT LIKE ?)')

	# Check returnSelectionValues
	def test_07_database_returnSelectionValues (self):
		
		# Run Tests
		self.assertEqual(returnSelectionValues({'IN': {'Test-1':['Unique ID']}}), ['"Unique ID"'])
		self.assertEqual(returnSelectionValues({'NOT IN': {'Test-1':['Unique ID', 'Species']}}), ['"Unique ID"', 'Species'])

	# Check innerJoinTables
	def test_08_database_innerJoinTables (self):
		
		# Run Tests
		self.assertEqual(innerJoinTables(['Table1', 'Table2'], ['Unique ID']), 'Table1 INNER JOIN Table2 ON Table1."Unique ID" = Table2."Unique ID"')
		self.assertEqual(innerJoinTables(['Table1', {'Table2':['Table2', 'Table3']}], [{'Unique ID':['Species']}]), 'Table1 INNER JOIN (Table2 INNER JOIN Table3 ON Table2.Species = Table3.Species) Table2 ON Table1."Unique ID" = Table2."Unique ID"')

	# Check createTable
	def test_09_database_createTable (self):

		# Create the database
		createTable(self.database, 'Table1', '"Unique ID" text, "Last Modified (Table1)" text, "Entry Created (Table1)" text')
		createTable(self.database, 'Table2', '"Unique ID" text, Species text, "Last Modified (Table2)" text, "Entry Created (Table2)" text')
		createTable(self.database, 'Table3', 'Species text, "Last Modified (Table3)" text, "Entry Created (Table3)" text')

		# Check the tables exist
		self.assertTrue(checkTable(self.database, 'Table1'))
		self.assertTrue(checkTable(self.database, 'Table2'))
		self.assertTrue(checkTable(self.database, 'Table3'))

		# Check the columns exist
		self.assertTrue(checkColumn(self.database, 'Table1', "Unique ID"))
		self.assertTrue(checkColumn(self.database, 'Table1', "Last Modified (Table1)"))
		self.assertTrue(checkColumn(self.database, 'Table1', "Entry Created (Table1)"))
		self.assertTrue(checkColumn(self.database, 'Table2', "Unique ID"))
		self.assertTrue(checkColumn(self.database, 'Table2', 'Species'))
		self.assertTrue(checkColumn(self.database, 'Table2', "Last Modified (Table2)"))
		self.assertTrue(checkColumn(self.database, 'Table2', "Entry Created (Table2)"))	  
		self.assertTrue(checkColumn(self.database, 'Table3', 'Species'))
		self.assertTrue(checkColumn(self.database, 'Table3', "Last Modified (Table3)"))
		self.assertTrue(checkColumn(self.database, 'Table3', "Entry Created (Table3)"))

	# Check insertValues
	def test_10_database_insertValues (self):

		# Insert values into the database
		insertValues(self.database, 'Table1', ["Unique ID"], ['Value1'])
		insertValues(self.database, 'Table2', ["Unique ID", 'Species'], ['Value1', 'Value2'])
		insertValues(self.database, 'Table3', ['Species'], ['Value2'])

		# Check that the values were correctly inserted 
		self.assertTrue(checkValue(self.database, 'Table1', '"Unique ID"', 'Value1'))
		self.assertTrue(checkValue(self.database, 'Table2', '"Unique ID"', 'Value1'))
		self.assertTrue(checkValue(self.database, 'Table2', 'Species', 'Value2'))
		self.assertTrue(checkValue(self.database, 'Table3', 'Species', 'Value2'))

	# Check updateValues
	def test_11_database_updateValues (self):

		# Update values in the database withing having to join tables
		updateValues (self.database, 'Table1', {'IN': {'"Unique ID"':['Value1']}}, {'"Unique ID"': 'Updated1'})
		updateValues (self.database, 'Table2', {'IN': {'"Unique ID"':['Value1']}}, {'"Unique ID"': 'Updated1'})

		# Check that the values were correctly inserted 
		self.assertTrue(checkValue(self.database, 'Table1', '"Unique ID"', 'Updated1'))
		self.assertTrue(checkValue(self.database, 'Table2', '"Unique ID"', 'Updated1'))

		# Update value in the database with joined tables
		updateValues (self.database, 'Table1', {'IN':{'Table1."Unique ID"': ['Updated1'], 'Table2.Species': ['Value2']}}, {'"Unique ID"': 'Updated2'}, update_table_column = "Unique ID", tables_to_join = ['Table1', 'Table2'], join_table_columns = ['"Unique ID"'])
		updateValues (self.database, 'Table2', {'IN': {'"Unique ID"':['Updated1']}}, {'"Unique ID"': 'Updated2'})

		# Check that the value were correctly inserted 
		self.assertTrue(checkValue(self.database, 'Table1', '"Unique ID"', 'Updated2'))
		self.assertTrue(checkValue(self.database, 'Table2', '"Unique ID"', 'Updated2'))

	# Check retrieveValues
	def test_12_database_retrieveValues (self):

		# Retrieve values from the database
		retrieved_entries = retrieveValues(self.database, ['Table1', 'Table2'], {}, ['Table1."Unique ID"', 'Table2.Species'], join_table_columns = ['"Unique ID"'])

		# Create the filename for the comparison file
		retrieveValues_test_filename = os.path.join(self.test_dir, 'test_12_comp.tsv')

		# Create the filename of the expected output file
		retrieveValues_expt_filename = os.path.join(self.expected_dir,'test_12_expected_output.tsv')

		# Create the file
		entriesToFile(retrieved_entries, retrieveValues_test_filename, '\t')

		# Compare the files
		self.assertTrue(fileComp(retrieveValues_test_filename, retrieveValues_expt_filename))

		# Create a StringIO object to store the stdout 
		test_str_stdout = io.StringIO()

		# Redirect stdout to StringIO objec
		sys.stdout = test_str_stdout

		# Call the stdout producing function
		entriesToScreen(retrieved_entries, '\t')

		# Reset the stdout
		sys.stdout = sys.__stdout__

		# Assign the test string
		test_str = test_str_stdout.getvalue()

		# Assign the expected string
		expected_str = 'Unique ID\tSpecies\nUpdated2\tValue2\n'

		self.assertEqual(test_str, expected_str)

# Run tests for config_file.py
class testset_02_config (unittest.TestCase):

	@classmethod
	def setUpClass(cls):

		# Create a temporary directory
		cls.test_dir = tempfile.mkdtemp()

		# Assign the expected output directory
		cls.expected_dir = 'test_files'

		# Assign the filename of the database
		cls.config_file = os.path.join(cls.expected_dir, 'testDB.yml')

		# Create empty variable to store the config data
		cls.config_data = None

	@classmethod
	def tearDownClass(cls):

		# Remove the test directory after the tests
		shutil.rmtree(cls.test_dir)

	# Check readConfig
	def test_13_config_readConfigs (self):

		# Read in the config file
		config_data = readConfig(self.config_file)

		# Check that the config data was assigned
		self.assertIsNotNone(config_data)

		# Update the config data for the other tests
		type(self).config_data = config_data

	# Check table iter function
	def test_14_config_tableIter (self):

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
	def test_15_config_tableList (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Create list of the expected tables
		expected_tables = ['Table1', 'Table2', 'Table3']

		# Check that the table are the same
		self.assertEqual(expected_tables, self.config_data.tables)

	# Check the find table function
	def test_16_config_tableFind (self):

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
	def test_17_config_tableHasColumn (self):

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
	def test_18_config_returnColumnPath (self):

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
	def test_19_config_returnJoinLists (self):

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
	def test_20_config_returnColumnPathDict (self):

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
	def test_21_config_returnColumnDict (self):

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
	def test_22_config_returnTables (self):

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
	def test_23_config_columnIter (self):

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
	def test_24_config_columnList (self):

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
	def test_25_config_columnFind (self):

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
	def test_26_config_tableAssignmentStr (self):

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
	def test_27_config_retieveColumnPaths (self):

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
	def test_28_config_columnAssignmentStr (self):

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
			
# Run tests for assignment.py
class testset_03_assignment (unittest.TestCase):

	@classmethod
	def setUpClass(cls):

		# Create a temporary directory
		cls.test_dir = tempfile.mkdtemp()

		# Assign the expected output directory
		cls.expected_dir = 'test_files'

		# Assign the filename of the database
		cls.config_file = os.path.join(cls.expected_dir, 'testDB.yml')

		# Try loading the config file
		try:

			# Read in the config file
			cls.config_data = readConfig(cls.config_file)

		# Set the data to None if that fails
		except:

			# Read in the config file
			cls.config_data = None


	@classmethod
	def tearDownClass(cls):

		# Remove the test directory after the tests
		shutil.rmtree(cls.test_dir)

	# Check assignTables 
	def test_29_assignTables  (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires working config file to operate. Check config tests for errors')

		# Create test keyword dicts
		test_01 = {'include_ID': 'TestID'}
		test_02 = {'include_species': 'TestSpecies'}
		test_03 = {'include_ID': 'TestID', 'exclude_species': 'TestSpecies'}
		test_04 = {'exclude_ID': 'TestID', 'include_species': 'TestSpecies'}
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
	def test_30_assignSelectionDict (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires working config file to operate. Check config tests for errors')

		# Create test keyword dicts
		test_01 = {'include_ID': 'TestID'}
		test_02 = {'include_species': 'TestSpecies'}
		test_03 = {'include_ID': 'TestID', 'exclude_species': 'TestSpecies'}
		test_04 = {'exclude_ID': 'TestID', 'include_species': 'TestSpecies'}
		test_05 = {'include_nests': True}
		test_06 = {'include_species': 'TestSpecies', 'exclude_nests': True}

		# Create data of the expected results
		expected_01 = {'IN': {'Table1."Unique ID"': 'TestID'}}
		expected_02 = {'IN': {'Table2.Species': 'TestSpecies'}}
		expected_03 = {'IN': {'Table1."Unique ID"': 'TestID'}, 'NOT IN': {'Table2.Species': 'TestSpecies'}}
		expected_04 = {'NOT IN': {'Table1."Unique ID"': 'TestID'}, 'IN': {'Table2.Species': 'TestSpecies'}}
		expected_05 = {'IN': {'Table3."From Nest?"': 'Yes'}}
		expected_06 = {'IN': {'Table2.Species': 'TestSpecies'}, 'NOT IN': {'Table3."From Nest?"': 'Yes'}}

		# Test which tables are returned
		self.assertEqual(assignSelectionDict(self.config_data, **test_01), expected_01)
		self.assertEqual(assignSelectionDict(self.config_data, **test_02), expected_02)
		self.assertEqual(assignSelectionDict(self.config_data, **test_03), expected_03)
		self.assertEqual(assignSelectionDict(self.config_data, **test_04), expected_04)
		self.assertEqual(assignSelectionDict(self.config_data, **test_05), expected_05)
		self.assertEqual(assignSelectionDict(self.config_data, **test_06), expected_06)

# Run tests for database input from various files
class testset_04_db_input (unittest.TestCase):

	@classmethod
	def setUpClass(cls):

		# Create a temporary directory
		cls.test_dir = tempfile.mkdtemp()

		# Assign the expected output directory
		cls.expected_dir = 'test_files'

		# Try creating the database
		try:

			# Read in the YAML config file
			config_data = readConfig(os.path.join(cls.expected_dir, 'testDB_large.yml'))

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

	# Check readCommonFile from common.py 
	def test_31_readCommonFile (self):

		# Assign the common filename
		common_filename = os.path.join(self.expected_dir, 'test_31_42_input.tsv')

		# Assign the expected header names
		expected_header = ['Site Code', 'Location', 'GPS']

		# Create a bool to indicate the header has been checked
		checked_header = False

		# Assign the expected rows
		expected_rows = 	[['RIM', 'Outer Rim', '40.345576285427555, -74.65398915528671']]
		expected_rows.append(['WIM', 'Wimbledon', '40.345576285427555, -74.65398915528671'])
		expected_rows.append(['VEN', 'Venus', '40.34555268323508, -74.65406448370945'])
		expected_rows.append(['TOM', 'Tomorrowland', '40.345575692332645, -74.65398227932042'])

		# Parse the common file
		for line_pos, (header, row) in enumerate(readCommonFile(common_filename)):

			# Check that the header file has not been checked
			if not checked_header:

				# Check that the header is what we expect
				self.assertEqual(header, expected_header)

				# Update the bool
				checked_header = True

			# Check that the current row is what we expect
			self.assertEqual(row, expected_rows[line_pos])

	# Check readAppCollection from collection.py 
	def test_32_readAppCollection (self):

		# Assign the collection filename
		collection_filename = os.path.join(self.expected_dir, 'test_32-33_input.csv')

		# Assign the expected header names
		expected_header = ['Unique ID', 'Site Code', 'Date Collected', 'Time Entered', 'Sex', 
						   'Life Stage', 'Has Pollen?', 'Sample Preservation Method', 'Head Preserved',
						   'Head Preservation Method', 'From Nest?', 'Nest Code', 'Notes', 'Collection File']


		# Create a bool to indicate the header has been checked
		checked_header = False

		# Assign the expected rows
		expected_rows = 	[['DBtest-0001', 'RIM', '11/6/2019', '1:48:14 PM', 'Female', 'Adult', 
		                	  'No', 'EtoH', 'Yes', 'PFA', 'No', 'N/A', 'These are notes', 
		                	  'test_32-33_input.csv']]
		expected_rows.append(['DBtest-0002', 'WIM', '11/6/2019', '2:17:47 PM', 'Male', 'Adult', 
							  'No', 'EtoH', 'No', 'N/A', 'No', 'N/A', 'Also notes ', 
							  'test_32-33_input.csv'])
		expected_rows.append(['DBtest-0003', 'VEN', '11/6/2019', '2:18:47 PM', 'Unknown', 'Big larva', 
							  'No', 'RNAlater', 'No', 'N/A', 'Yes', 'N-1', 'From nest on Mars', 
							  'test_32-33_input.csv'])
		expected_rows.append(['DBtest-0004', 'TOM', '11/6/2019', '2:19:57 PM', 'Multiple', 'Small larva', 
							  'No', 'RNAlater', 'No', 'N/A', 'Yes', 'Nest 21', 'All the larvae from this nest', 
							  'test_32-33_input.csv'])

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
			
	# Check addAppCollectionToDatabase from collection.py 
	def test_33_addAppCollectionToDatabase (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the collection filename
		collection_filename = os.path.join(self.expected_dir, 'test_32-33_input.csv')

		# Add the data to the database
		addAppCollectionToDatabase(self.database_filename, 'Collection', collection_filename)

		# Check that the values were correctly inserted 
		self.assertTrue(checkValue(self.database_filename, 'Collection', '"Unique ID"', '"DBtest-0001"'))
		self.assertTrue(checkValue(self.database_filename, 'Collection', '"Site Code"', 'WIM'))
		self.assertTrue(checkValue(self.database_filename, 'Collection', '"Nest Code"', '"N-1"'))
		self.assertTrue(checkValue(self.database_filename, 'Collection', 'Sex', 'Multiple'))

	# Check updateAppCollectionToDatabase from collection.py 
	def test_34_updateAppCollectionToDatabase (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the collection filename
		collection_filename = os.path.join(self.expected_dir, 'test_34_input.csv')

		# Add the data to the database
		updateAppCollectionToDatabase(self.database_filename, 'Collection', "Unique ID", collection_filename)

		# Check that the values were correctly inserted
		self.assertTrue(checkValue(self.database_filename, 'Collection', '"Has Pollen?"', 'Yes'))
		self.assertTrue(checkValue(self.database_filename, 'Collection', '"Site Code"', 'BAS'))
		self.assertTrue(checkValue(self.database_filename, 'Collection', '"Nest Code"', '"F-21"'))
		self.assertTrue(checkValue(self.database_filename, 'Collection', 'Sex', 'N/A'))

	# Check addStorageFileToDatabase from storage.py 
	def test_35_addStorageFileToDatabase (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the storage filename
		storage_filename = os.path.join(self.expected_dir, 'test_35_input.tsv')

		# Upload the Storage data
		addStorageFileToDatabase(self.database_filename, 'Storage', storage_filename)

		# Check that the values were correctly inserted
		self.assertTrue(checkValue(self.database_filename, 'Storage', '"Unique ID"', '"DBtest-0001"'))
		self.assertTrue(checkValue(self.database_filename, 'Storage', '"Storage ID"', '"DBtest-A2"'))
		self.assertTrue(checkValue(self.database_filename, 'Storage', 'Plate', 'DBtest'))
		self.assertTrue(checkValue(self.database_filename, 'Storage', 'Well', 'B1'))

	# Check updateStorageFileToDatabase from storage.py 
	def test_36_updateStorageFileToDatabase (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the storage filename
		storage_filename = os.path.join(self.expected_dir, 'test_36_input.tsv')

		# Upload the Storage data
		updateStorageFileToDatabase(self.database_filename, 'Storage', 'Storage ID', storage_filename)

		# Check that the values were correctly inserted
		self.assertTrue(checkValue(self.database_filename, 'Storage', 'Plate', 'DBtest2'))
		self.assertTrue(checkValue(self.database_filename, 'Storage', 'Well', 'A4'))

	# Check convertPlateWell from storage.py 
	def test_37_convertPlateWell (self):

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

	# Check assignStorageIDs from barcode.py 
	def test_38_assignStorageIDs (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the blast filename
		blast_filename = os.path.join(self.expected_dir, 'test_38-40_input.out')

		# Assign the json filename
		json_filename = os.path.join(self.expected_dir, 'test_38-40_input.json')

		# Save the expected dict
		expected_dict = {'DBtest-A1': '"DBtest-A1"', 'DBtest-A2': '"DBtest-A2"'}

		# Assign the storage IDs
		test_dict = assignStorageIDs(self.database_filename, 'Storage', 'Storage ID', blast_filename, json_filename)

		# Make sure we were able to get the correct IDs
		self.assertEqual(test_dict, expected_dict)

	# Check readSeqFiles from barcode.py 
	def test_39_readSeqFiles (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the blast filename
		blast_filename = os.path.join(self.expected_dir, 'test_38-40_input.out')

		# Assign the fasta filename
		fasta_filename = os.path.join(self.expected_dir, 'test_38-40_input.fasta')

		# Assign the json filename
		json_filename = os.path.join(self.expected_dir, 'test_38-40_input.json')

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

	# Check addSeqFilesToDatabase from barcode.py 
	def test_40_addSeqFilesToDatabase (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the blast filename
		blast_filename = os.path.join(self.expected_dir, 'test_38-40_input.out')

		# Assign the fasta filename
		fasta_filename = os.path.join(self.expected_dir, 'test_38-40_input.fasta')

		# Assign the json filename
		json_filename = os.path.join(self.expected_dir, 'test_38-40_input.json')

		# Add the data to the database
		addSeqFilesToDatabase(self.database_filename, 'Sequencing', blast_filename, fasta_filename, json_filename, 'Storage', 'Storage ID')

		# Check that the values were correctly inserted
		self.assertTrue(checkValue(self.database_filename, 'Sequencing', '"Storage ID"', '"DBtest-A1"'))
		self.assertTrue(checkValue(self.database_filename, 'Sequencing', '"Sequence ID"', '"DBtest-A2"'))
		self.assertTrue(checkValue(self.database_filename, 'Sequencing', 'Species', '"Lasioglossum oenotherae"'))
		self.assertTrue(checkValue(self.database_filename, 'Sequencing', 'Status', '"Ambiguous Hits"'))

	# Check updateSeqFilesToDatabase from barcode.py 
	def test_41_updateSeqFilesToDatabase (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the blast filename
		blast_filename = os.path.join(self.expected_dir, 'test_41_input.out')

		# Assign the fasta filename
		fasta_filename = os.path.join(self.expected_dir, 'test_41_input.fasta')

		# Assign the json filename
		json_filename = os.path.join(self.expected_dir, 'test_41_input.json')

		# Update the data to the database
		updateSeqFilesToDatabase(self.database_filename, 'Sequencing', "Sequence ID", blast_filename, fasta_filename, json_filename, 'Storage', 'Storage ID')

		# Check that the values were correctly inserted
		self.assertTrue(checkValue(self.database_filename, 'Sequencing', '"Ambiguous Hits"', None))
		self.assertTrue(checkValue(self.database_filename, 'Sequencing', '"Sequence ID"', '"DBtest-A2"'))
		self.assertTrue(checkValue(self.database_filename, 'Sequencing', 'Species', '"Ceratina strenua"'))
		self.assertTrue(checkValue(self.database_filename, 'Sequencing', 'Status', '"No Hits"'))

	# Check addLocFileToDatabase from location.py 
	def test_42_addLocFileToDatabase (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the location filename
		loc_filename = os.path.join(self.expected_dir, 'test_31_42_input.tsv')

		# Add the data to the database
		addLocFileToDatabase (self.database_filename, 'Locations', loc_filename)

		# Check that the values were correctly inserted
		self.assertTrue(checkValue(self.database_filename, 'Locations', '"Site Code"', 'TOM'))
		self.assertTrue(checkValue(self.database_filename, 'Locations', 'Location', '"Outer Rim"'))
		self.assertTrue(checkValue(self.database_filename, 'Locations', 'GPS', '"40.34555268323508, -74.65406448370945"'))

	# Check updateLocFileToDatabase from location.py 
	def test_43_updateLocFileToDatabase (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the location filename
		loc_filename = os.path.join(self.expected_dir, 'test_43_input.tsv')

		# Add the data to the database
		updateLocFileToDatabase (self.database_filename, 'Locations', 'Site Code', loc_filename)

		# Check that the values were correctly inserted
		self.assertTrue(checkValue(self.database_filename, 'Locations', 'Location', '"Inner Rim"'))
		self.assertTrue(checkValue(self.database_filename, 'Locations', 'GPS', '"50.123, -74.50.123"'))

	# Check convertLoc from location.py 
	def test_44_convertLoc (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the expected location tuple
		expected_loc = ('Location', '"Inner Rim"')

		# Assign the test loc from the loc file
		test_loc = convertLoc(self.database_filename, 'Locations', '"Site Code"', 'RIM', 'Location')

		# Check if we get the expected value from the test results
		self.assertEqual(test_loc, expected_loc)

	# Check readAppLocations from location.py 
	def test_45_readAppLocations (self):

		# Assign the collection filename
		collection_filename = os.path.join(self.expected_dir, 'test_45-46_input.csv')

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

	# Check addAppLocationsToDatabase from location.py 
	def test_46_addAppLocationsToDatabase (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the collection filename
		collection_filename = os.path.join(self.expected_dir, 'test_45-46_input.csv')

		# Add the locations to the database
		addAppLocationsToDatabase(self.database_filename, 'Locations', collection_filename)

		# Check that the values were correctly inserted
		self.assertTrue(checkValue(self.database_filename, 'Locations', '"Site Code"', 'RIM2'))
		self.assertTrue(checkValue(self.database_filename, 'Locations', 'GPS', '"40.00, -74.00"'))

	# Check updateAppLocationsToDatabase from location.py 
	def test_47_updateAppLocationsToDatabase (self):

		# Check if the config data wasn't assigned
		if self.database_filename == None:

			# Skip the test if so
			self.skipTest('Requires database to operate. Check database tests for errors')

		# Assign the collection filename
		collection_filename = os.path.join(self.expected_dir, 'test_47_input.csv')

		# Add the locations to the database
		updateAppLocationsToDatabase(self.database_filename, 'Locations', 'Site Code',collection_filename)

		# Check that the values were correctly inserted
		self.assertTrue(checkValue(self.database_filename, 'Locations', 'GPS', '"50, -75"'))

# Run tests for miscellaneous functions
class testset_05_misc (unittest.TestCase):

	@classmethod
	def setUpClass(cls):

		# Create a temporary directory
		cls.test_dir = tempfile.mkdtemp()

		# Assign the expected output directory
		cls.expected_dir = 'test_files'

	@classmethod
	def tearDownClass(cls):

		# Remove the test directory after the tests
		shutil.rmtree(cls.test_dir)

	# Check confirmExecutable from misc.py 
	def test_48_confirmExecutable  (self):

		# Check that the package functions are executable
		self.assertIsNotNone(confirmExecutable('retrieve_samples.py'))
		self.assertIsNotNone(confirmExecutable('upload_samples.py'))
		self.assertIsNotNone(confirmExecutable('barcode_pipeline.py'))
		self.assertIsNotNone(confirmExecutable('barcode_filter.py'))

		# Create list of executables
		executable_list = ['vsearch', 'fastq-multx', 'blastn']

		# Loop the executables
		for executable_str in executable_list:

			# Check that the non-standard executables were installed
			self.assertIsNotNone(confirmExecutable(executable_str), '%s not found. Please install' % executable_str)


		# Check that the function fails with a fake executable
		self.assertIsNone(confirmExecutable('retrieve_samples.py' + randomGenerator()))
		




if __name__ == "__main__":
	unittest.main(verbosity = 2)
