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
from kocher_tools.config_file import readConfig

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
		
		# Create the value check string
		sqlite_check_value = 'SELECT COUNT(*) FROM %s WHERE %s = ?' % (table, column)

		# Connect to the sqlite database
		sqlite_connection = sqlite3.connect(database)

		# Create the cursor
		cursor = sqlite_connection.cursor()

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

def fileComp (test_output, expected_output):

	# Compare two files, return bool
	return filecmp.cmp(test_output, expected_output)

def randomGenerator (length = 10, char_set = string.ascii_uppercase + string.digits):

	# Return a random string of letters and numbers
	return ''.join(random.choice(char_set) for i in range(length))

# Run tests for the functions within the vcftools module
class databaseTests (unittest.TestCase):

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
		self.assertEqual(quoteField('Test-1.SubTest-1'),'"Test-1"."SubTest-1"')
		self.assertEqual(quoteField('Test-1."SubTest 2"'), '"Test-1"."SubTest 2"')
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
		self.assertEqual(returnSelectionDict({'IN': {'Test-1':['SubTest-1']}}), '"Test-1" IN (?)')
		self.assertEqual(returnSelectionDict({'NOT IN': {'Test-1':['SubTest-1', '"SubTest 2"']}}), '"Test-1" NOT IN (?, ?)')
		self.assertEqual(returnSelectionDict({'IN': {'Test-1':['SubTest-1'], '"Test 2"':['"SubTest 2"']}}), '"Test-1" IN (?) AND "Test 2" IN (?)')
		self.assertEqual(returnSelectionDict({'LIKE': {'Test-1':['SubTest-1']}}), '"Test-1" LIKE ?')
		self.assertEqual(returnSelectionDict({'NOT LIKE': {'Test-1':['SubTest-1', '"SubTest 2"']}}), '("Test-1" NOT LIKE ? OR "Test-1" NOT LIKE ?)')

	# Check returnSelectionValues
	def test_07_database_returnSelectionValues (self):
		
		# Run Tests
		self.assertEqual(returnSelectionValues({'IN': {'Test-1':['SubTest-1']}}), ['"SubTest-1"'])
		self.assertEqual(returnSelectionValues({'NOT IN': {'Test-1':['SubTest-1', '"SubTest 2"']}}), ['"SubTest-1"', '"SubTest 2"'])

	# Check innerJoinTables
	def test_08_database_innerJoinTables (self):
		
		# Run Tests
		self.assertEqual(innerJoinTables(['Table1', 'Table2'], ['SubTest-1']), 'Table1 INNER JOIN Table2 ON Table1."SubTest-1" = Table2."SubTest-1"')
		self.assertEqual(innerJoinTables(['Table1', {'Table2':['Table2', 'Table3']}], [{'SubTest-1':["SubTest 2"]}]), 'Table1 INNER JOIN (Table2 INNER JOIN Table3 ON Table2."SubTest 2" = Table3."SubTest 2") Table2 ON Table1."SubTest-1" = Table2."SubTest-1"')

	# Check createTable
	def test_09_database_createTable (self):

		# Create the database
		createTable(self.database, 'Table1', '"SubTest-1" text, "Last Modified (Table1)" text, "Entry Created (Table1)" text')
		createTable(self.database, 'Table2', '"SubTest-1" text, "SubTest 2" text, "Last Modified (Table2)" text, "Entry Created (Table2)" text')
		createTable(self.database, 'Table3', '"SubTest 2" text, "Last Modified (Table3)" text, "Entry Created (Table3)" text')

		# Check the tables exist
		self.assertTrue(checkTable(self.database, 'Table1'))
		self.assertTrue(checkTable(self.database, 'Table2'))
		self.assertTrue(checkTable(self.database, 'Table3'))

		# Check the columns exist
		self.assertTrue(checkColumn(self.database, 'Table1', "SubTest-1"))
		self.assertTrue(checkColumn(self.database, 'Table1', "Last Modified (Table1)"))
		self.assertTrue(checkColumn(self.database, 'Table1', "Entry Created (Table1)"))
		self.assertTrue(checkColumn(self.database, 'Table2', "SubTest-1"))
		self.assertTrue(checkColumn(self.database, 'Table2', "SubTest 2"))
		self.assertTrue(checkColumn(self.database, 'Table2', "Last Modified (Table2)"))
		self.assertTrue(checkColumn(self.database, 'Table2', "Entry Created (Table2)"))	  
		self.assertTrue(checkColumn(self.database, 'Table3', "SubTest 2"))
		self.assertTrue(checkColumn(self.database, 'Table3', "Last Modified (Table3)"))
		self.assertTrue(checkColumn(self.database, 'Table3', "Entry Created (Table3)"))

	# Check insertValues
	def test_10_database_insertValues (self):

		# Insert values into the database
		insertValues(self.database, 'Table1', ["SubTest-1"], ['Value1'])
		insertValues(self.database, 'Table2', ["SubTest-1", "SubTest 2"], ['Value1', 'Value2'])
		insertValues(self.database, 'Table3', ["SubTest 2"], ['Value2'])

		# Check that the values were correctly inserted 
		self.assertTrue(checkValue(self.database, 'Table1', '"SubTest-1"', 'Value1'))
		self.assertTrue(checkValue(self.database, 'Table2', '"SubTest-1"', 'Value1'))
		self.assertTrue(checkValue(self.database, 'Table2', '"SubTest 2"', 'Value2'))
		self.assertTrue(checkValue(self.database, 'Table3', '"SubTest 2"', 'Value2'))

	# Check updateValues
	def test_11_database_updateValues (self):

		# Update values in the database withing having to join tables
		updateValues (self.database, 'Table1', {'IN': {'"SubTest-1"':['Value1']}}, {'"SubTest-1"': 'Updated1'})
		updateValues (self.database, 'Table2', {'IN': {'"SubTest-1"':['Value1']}}, {'"SubTest-1"': 'Updated1'})

		# Check that the values were correctly inserted 
		self.assertTrue(checkValue(self.database, 'Table1', '"SubTest-1"', 'Updated1'))
		self.assertTrue(checkValue(self.database, 'Table2', '"SubTest-1"', 'Updated1'))

		# Update value in the database with joined tables
		updateValues (self.database, 'Table1', {'IN':{'Table1."SubTest-1"': ['Updated1'], 'Table2."SubTest 2"': ['Value2']}}, {'"SubTest-1"': 'Updated2'}, update_table_column = "SubTest-1", tables_to_join = ['Table1', 'Table2'], join_table_columns = ['"SubTest-1"'])
		updateValues (self.database, 'Table2', {'IN': {'"SubTest-1"':['Updated1']}}, {'"SubTest-1"': 'Updated2'})

		# Check that the value were correctly inserted 
		self.assertTrue(checkValue(self.database, 'Table1', '"SubTest-1"', 'Updated2'))
		self.assertTrue(checkValue(self.database, 'Table2', '"SubTest-1"', 'Updated2'))

	# Check retrieveValues
	def test_12_database_retrieveValues (self):

		# Retrieve values from the database
		retrieved_entries = retrieveValues(self.database, ['Table1', 'Table2'], {}, ['Table1."SubTest-1"', 'Table2."SubTest 2"'], join_table_columns = ['"SubTest-1"'])

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
		expected_str = 'SubTest-1\tSubTest 2\nUpdated2\tValue2\n'

		self.assertEqual(test_str, expected_str)

# Run tests for the functions within the vcftools module
class configTests (unittest.TestCase):

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
	def test_01_config_readConfigs (self):

		# Read in the config file
		config_data = readConfig(self.config_file)

		# Check that the config data was assigned
		self.assertIsNotNone(config_data)

		# Update the config data for the other tests
		type(self).config_data = config_data

	def test_02_config_testTableIter (self):

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

	def test_03_config_testTableList (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Create list of the expected tables
		expected_tables = ['Table1', 'Table2', 'Table3']

		# Check that the table are the same
		self.assertEqual(expected_tables, self.config_data.tables)

	def test_04_config_testTableFind (self):

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

	def test_05_config_testTableHasColumn (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Create list of the expected columns
		expected_cols = ['"SubTest-1"', '"Last Modified (Table1)"', '"Entry Created (Table1)"']
		expected_cols.extend(['"SubTest-1"', '"SubTest 2"', '"Last Modified (Table2)"', '"Entry Created (Table2)"'])
		expected_cols.extend(['"SubTest 2"', '"Last Modified (Table3)"', '"Entry Created (Table3)"'])

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

	def test_06_config_testReturnColumnPath (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Create list of the expected columns
		expected_cols = ['"SubTest-1"', '"Last Modified (Table1)"', '"Entry Created (Table1)"']
		expected_cols.extend(['"SubTest-1"', '"SubTest 2"', '"Last Modified (Table2)"', '"Entry Created (Table2)"'])
		expected_cols.extend(['"SubTest 2"', '"Last Modified (Table3)"', '"Entry Created (Table3)"'])

		# Loop the expected columns
		for expected_col in expected_cols:

			self.assertIsNotNone(self.config_data.returnColumnPath(expected_col))

		# Check that the function fails with an unexpected column
		self.assertRaises(Exception, self.config_data.returnColumnPath, 'SubTest_' + randomGenerator())

	def test_07_config_testReturnJoinLists (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Assign the expected tables and columns
		expected_tables_to_join = ['Table1', 'Table2']
		expected_join_by_columns = ['"SubTest-1"']

		# Assigned the test tables and columns for a simple join
		test_tables_to_join, test_join_by_columns = self.config_data.returnJoinLists(['Table1','Table2'])

		# Check that the table and columns are the same
		self.assertEqual(expected_tables_to_join, test_tables_to_join)
		self.assertEqual(expected_join_by_columns, test_join_by_columns)

		# Assign the expected tables and columns
		expected_tables_to_join = ['Table1', {'Table2': ['Table2', 'Table3']}]
		expected_join_by_columns = [{'"SubTest-1"': ['"SubTest 2"']}]

		# Assigned the test tables and columns for a simple join
		test_tables_to_join, test_join_by_columns = self.config_data.returnJoinLists(['Table1','Table3'])

		# Check that the table and columns are the same
		self.assertEqual(expected_tables_to_join, test_tables_to_join)
		self.assertEqual(expected_join_by_columns, test_join_by_columns)

		# Check that the function fails with an unexpected table
		self.assertRaises(Exception, self.config_data.returnJoinLists, ['Table' + randomGenerator(),'Table' + randomGenerator()])

	def test_08_config_testReturnColumnPathDict (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Assign the expected path dict
		expected_path_dict = {'Table1."SubTest-1"': ['Value1']}

		# Save the test path dict
		test_path_dict = self.config_data.returnColumnPathDict({'"SubTest-1"':['Value1']})

		# Check that the path dicts are the same
		self.assertEqual(expected_path_dict, test_path_dict)

		# Check that the function fails with an unexpected column
		self.assertRaises(Exception, self.config_data.returnColumnPathDict, {'SubTest-' + randomGenerator():['Value1']})

	def test_09_config_testReturnColumnDict (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Assign the expected path dict
		expected_dict = {'"SubTest-1"': ['Value1']}

		# Save the test path dict
		test_dict = self.config_data.returnColumnDict({'"SubTest-1"':['Value1']}, 'Table1')

		# Check that the path dicts are the same
		self.assertEqual(expected_dict, test_dict)

		# Check that the function fails with an unexpected column
		self.assertRaises(Exception, self.config_data.returnColumnDict, {'SubTest-' + randomGenerator():['Value1']})

	def test_10_config_testReturnTables (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Assign the expected tables
		expected_tables = set(['Table1', 'Table2'])

		# Assign the test tables
		test_tables = set(self.config_data.returnTables(['"SubTest-1"', "SubTest 2"]))

		# Check that the tables are the same
		self.assertEqual(expected_tables, test_tables)

		# Check that the function fails with an unexpected column
		self.assertRaises(Exception, self.config_data.returnTables, ['SubTest-' + randomGenerator()])

	def test_11_config_testColumnIter (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Create list of the expected columns from each table
		expected_cols = [['"SubTest-1"', '"Last Modified (Table1)"', '"Entry Created (Table1)"']]
		expected_cols.append(['"SubTest-1"', '"SubTest 2"', '"Last Modified (Table2)"', '"Entry Created (Table2)"'])
		expected_cols.append(['"SubTest 2"', '"Last Modified (Table3)"', '"Entry Created (Table3)"'])

		# Iterate the tables in the config file
		for table_pos, table in enumerate(self.config_data):

			# Assign the test columns
			test_cols = [str(col) for col in table]

			# Check that the table are the same
			self.assertEqual(expected_cols[table_pos], test_cols)

	def test_12_config_testColumnList (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Create list of the expected columns from each table
		expected_cols = [['"SubTest-1"', '"Last Modified (Table1)"', '"Entry Created (Table1)"']]
		expected_cols.append(['"SubTest-1"', '"SubTest 2"', '"Last Modified (Table2)"', '"Entry Created (Table2)"'])
		expected_cols.append(['"SubTest 2"', '"Last Modified (Table3)"', '"Entry Created (Table3)"'])

		# Iterate the tables in the config file
		for table_pos, table in enumerate(self.config_data):

			# Check that the table are the same
			self.assertEqual(expected_cols[table_pos], table.columns)

		# Create list of the expected columns from each table
		expected_cols = [['SubTest-1', 'Last Modified (Table1)', 'Entry Created (Table1)']]
		expected_cols.append(['SubTest-1', 'SubTest 2', 'Last Modified (Table2)', 'Entry Created (Table2)'])
		expected_cols.append(['SubTest 2', 'Last Modified (Table3)', 'Entry Created (Table3)'])

		# Iterate the tables in the config file
		for table_pos, table in enumerate(self.config_data):

			# Check that the table are the same
			self.assertEqual(expected_cols[table_pos], table.unquoted_columns)

	def test_13_config_testColumnFind (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Create list of the expected columns from each table
		expected_cols =     [['"SubTest-1"', '"Last Modified (Table1)"', '"Entry Created (Table1)"']]
		expected_cols.append(['"SubTest-1"', '"SubTest 2"', '"Last Modified (Table2)"', '"Entry Created (Table2)"'])
		expected_cols.append(['"SubTest 2"', '"Last Modified (Table3)"', '"Entry Created (Table3)"'])

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

	def test_14_config_testTableAssignmentStr (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Create list of the expected strings for each table
		expected_strs =     ['"SubTest-1" text, "Last Modified (Table1)" text, "Entry Created (Table1)" text']
		expected_strs.append('"SubTest-1" text, "SubTest 2" text, "Last Modified (Table2)" text, "Entry Created (Table2)" text')
		expected_strs.append('"SubTest 2" text, "Last Modified (Table3)" text, "Entry Created (Table3)" text')

		# Iterate the tables in the config file
		for table_pos, table in enumerate(self.config_data):

			# Check the strings are the same
			self.assertEqual(expected_strs[table_pos], table.assignment_str)

	def test_15_config_testRetieveColumnPaths (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Assign the expected paths
		expected_paths =     [['Table1."SubTest-1"', 'Table1."Last Modified (Table1)"', 'Table1."Entry Created (Table1)"']]
		expected_paths.append(['Table2."SubTest-1"', 'Table2."SubTest 2"', 'Table2."Last Modified (Table2)"', 'Table2."Entry Created (Table2)"'])
		expected_paths.append(['Table3."SubTest 2"', 'Table3."Last Modified (Table3)"', 'Table3."Entry Created (Table3)"'])

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

	def test_16_config_testColumnAssignmentStr (self):

		# Check if the config data wasn't assigned
		if self.config_data == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Assign the expected paths
		expected_strs =     [['"SubTest-1" text', '"Last Modified (Table1)" text', '"Entry Created (Table1)" text']]
		expected_strs.append(['"SubTest-1" text', '"SubTest 2" text', '"Last Modified (Table2)" text', '"Entry Created (Table2)" text'])
		expected_strs.append(['"SubTest 2" text', '"Last Modified (Table3)" text', '"Entry Created (Table3)" text'])

		# Loop the tables
		for table_pos, table in enumerate(self.config_data):

			# Loop the column
			for column_pos, column in enumerate(table):

				# Check the strings are the same
				self.assertEqual(expected_strs[table_pos][column_pos], column)
			
















		

if __name__ == "__main__":
	unittest.main(verbosity = 2)
