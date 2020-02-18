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
import datetime
import pytz

from kocher_tools.backup import *
from tests.functions import checkValue

# Run tests for backup.py
class test_backup (unittest.TestCase):

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
		cls.database = os.path.join(cls.expected_path, 'testDB.sqlite')

	@classmethod
	def tearDownClass (cls):

		# Remove the test directory after the tests
		shutil.rmtree(cls.test_dir)

	# Check returnDate
	def test_01_returnDate (self):

		# Assign the date object using the command
		test_one_date_object = returnDate()

		# Set the timezone
		specified_timezone = pytz.timezone('US/Eastern')

		# Assign the current date
		expected_date_object = datetime.datetime.now(specified_timezone)

		# Set the date format for the logging system
		date_format = '%Y-%m-%d'

		# Convert the dates into a strings
		test_one_date_str = test_one_date_object.strftime(date_format)
		expected_date_str = expected_date_object.strftime(date_format)

		# Confirm the dates are the same
		self.assertEqual(test_one_date_str, expected_date_str)

		# Assign the date object using the command
		test_two_date_object = returnDate()

		# Confirm the dates are the same
		self.assertEqual(test_one_date_object, test_two_date_object)

	# Check returnDateStr
	def test_02_returnDateStr (self):

		# Assign the date string using the command
		test_date_str = returnDateStr()

		# Set the timezone
		specified_timezone = pytz.timezone('US/Eastern')

		# Assign the current date
		expected_date_object = datetime.datetime.now(specified_timezone)

		# Set the date format for the logging system
		date_format = '%Y-%m-%d'

		# Convert the date into a string
		expected_date_str = expected_date_object.strftime(date_format)

		# Confirm the dates are the same
		self.assertEqual(test_date_str, expected_date_str)

	# Check strToDate
	def test_03_strToDate (self):

		# Set the timezone
		specified_timezone = pytz.timezone('US/Eastern')

		# Assign the current date
		date_object = datetime.datetime.now(specified_timezone)

		# Set the date format for the logging system
		date_format = '%Y-%m-%d'

		# Convert the date into a string
		date_str = date_object.strftime(date_format)

		# Assign the expected date object
		expected_date_object = datetime.datetime.strptime(date_str, '%Y-%m-%d')

		# Assign the date object using the command
		test_date_object = strToDate(date_str)

		# Confirm the dates are the same
		self.assertEqual(test_date_object, expected_date_object)

	# Check backupNeeded
	def test_04_backupNeeded (self):

		# Assign the out dir of the backup
		backup_dir = os.path.join(self.expected_path, 'TestBackups')

		# Run the command
		backup_needed_result_one = backupNeeded(self.database, backup_dir, 10)
		backup_needed_result_two = backupNeeded(self.database, backup_dir, 10**10)

		# Check that the commands returned the expected bool
		self.assertTrue(backup_needed_result_one)
		self.assertFalse(backup_needed_result_two)

	# Check createBackup
	def test_05_createBackup (self):

		# Assign the out dir of the backup
		test_backup_dir = os.path.join(self.test_dir, 'TestBackups')

		# Create the test directory
		if not os.path.exists(test_backup_dir):
			os.makedirs(test_backup_dir)

		# Run the command
		createBackup(self.database, test_backup_dir)

		# Assign the contents of the test dir
		test_backup_files = os.listdir(test_backup_dir)

		# Check that the file was created
		self.assertTrue(len(test_backup_files) == 1)

		# List the files within the directory
		for test_backup_file in test_backup_files:

			# Assign the filepath of the test file
			test_backup_filepath = os.path.join(test_backup_dir, test_backup_file)

			# Check that the expected values are found
			self.assertTrue(checkValue(test_backup_filepath, 'Table1', '"Unique ID"', 'Value1'))
			self.assertTrue(checkValue(test_backup_filepath, 'Table2', '"Unique ID"', 'Value1'))

	# Check removeOldBackup
	def test_06_removeOldBackup (self):

		# Assign the out dir of the backup
		test_backup_dir = os.path.join(self.test_dir, 'TestBackups')

		# Create the test directory
		if not os.path.exists(test_backup_dir):
			os.makedirs(test_backup_dir)

		# Assign the contents of the test dir
		expected_backup_files = os.listdir(test_backup_dir)

		# Create filename for empty files to be deleted
		old_backup_filenames = ['testDB.sqlite.AGH5C1.2018-01-01.backup',
								'testDB.sqlite.NH147F.2019-01-01.backup']

		# Loop filenames
		for old_backup_filename in old_backup_filenames:

			# Assign the filepath for the file
			test_backup_filepath = os.path.join(test_backup_dir, old_backup_filename)

			# Create the file
			old_backup_file = open(test_backup_filepath, 'w')
			old_backup_file.close()

		# Assign the contents of the test dir
		test_backup_files = os.listdir(test_backup_dir)

		# Check that the file was created
		self.assertTrue(len(test_backup_files) == 3)

		# Run the command
		removeOldBackup(self.database, test_backup_dir, 1)

		# Assign the contents of the test dir
		test_backup_files = os.listdir(test_backup_dir)

		# Check that the file was created
		self.assertTrue(len(test_backup_files) == 1)
		self.assertEqual(test_backup_files, expected_backup_files)

if __name__ == "__main__":
	unittest.main(verbosity = 2)
