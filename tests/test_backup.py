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
from kocher_tools.config_file import readConfig
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

		# Create empty variable to store the backups
		cls.backups = None

	@classmethod
	def tearDownClass (cls):

		# Remove the test directory after the tests
		shutil.rmtree(cls.test_dir)

	# Check Backups
	def test_01_Backups (self):

		try:

			# Assign the path of the existing files/dir to copy
			existing_config_file = os.path.join(self.expected_path, 'testDB.yml')
			existing_backup_dir = os.path.join(self.expected_path, 'TestBackups')

			# Assign the path of the test files/dir to copy
			test_config_file = os.path.join(self.test_dir, 'testDB.yml')
			test_backup_dir = os.path.join(self.test_dir, 'TestBackups')

			# Copy the test files/dir to copy
			shutil.copy(existing_config_file, test_config_file)
			shutil.copytree(existing_backup_dir, test_backup_dir)

			# Read in the config file
			config_data = readConfig(test_config_file)

			# Load the current backups
			type(self).backups = Backups(out_dir = config_data.backup_out_dir, limit = config_data.backup_limit, update_freq = config_data.backup_update_freq)

		except:

			raise Exception('Unable to assign Backups')

	# Check Backup
	def test_02_Backup (self):

		# Check if the config data wasn't assigned
		if self.backups == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Confirm a single backup was found in the backups
		self.assertEqual(len(self.backups), 1)

		# Assign the test backup
		test_backup = self.backups[0]

		# Check the string function returns the path
		self.assertEqual(test_backup.file_path, str(test_backup))

		# Assign the expected backup info
		expected_file = test_backup_dir = os.path.join(self.test_dir, 'TestBackups', 'testDB.sqlite.QR56G6.2020-01-01.backup')
		expected_date_object = datetime.datetime.strptime('2020-01-01', '%Y-%m-%d')

		# Add timezone information to the expected date object
		expected_date_object = expected_date_object.replace(tzinfo = pytz.timezone('US/Eastern'))
 		
 		# Confirm the contents are as expected
		self.assertEqual(test_backup.file_path, expected_file)
		self.assertEqual(test_backup.backup_date, expected_date_object)

		# Assign a new backup for testing
		test_01_file = test_backup_dir = os.path.join(self.test_dir, 'TestBackups', 'testDB.sqlite.QR56G6.2000-01-01.backup')
		test_01_date_object = datetime.datetime.strptime('2000-01-01', '%Y-%m-%d')

		# Add timezone information to the expected date object
		test_01_date_object = test_01_date_object.replace(tzinfo = pytz.timezone('US/Eastern'))

		# Copy a file for the test
		shutil.copy(expected_file, test_01_file)

		# Save the file as a backup
		test_01 = Backup(test_01_file, test_01_date_object)

		# Confirm the first test is older than the current backup
		self.assertTrue(test_01 < test_backup)

		# Remove the test file
		os.remove(test_01_file)

		# Assign a new backup for testing
		test_02_file = test_backup_dir = os.path.join(self.test_dir, 'TestBackups', 'testDB.sqlite.QR56G6.2030-01-01.backup')
		test_01_date_object = datetime.datetime.strptime('2030-01-01', '%Y-%m-%d')

		# Add timezone information to the expected date object
		test_02_date_object = test_01_date_object.replace(tzinfo = pytz.timezone('US/Eastern'))

		# Copy a file for the test
		shutil.copy(expected_file, test_02_file)

		# Save the file as a backup
		test_02 = Backup(test_02_file, test_02_date_object)

		# Confirm the first test is older than the current backup
		self.assertTrue(test_02 > test_backup)

		# Remove the test file
		os.remove(test_02_file)

	# Check backupNeeded
	def test_03_backupNeeded (self):

		# Confirm that the backup needed function returns True when past the undate frequency
		self.assertTrue(self.backups.backupNeeded())

		# Save the orignal update frequency
		update_freq = self.backups.update_freq

		# Update the update frequency
		type(self).backups.update_freq = 10**10

		# Confirm that the backup needed function returns True when past the undate frequency
		self.assertFalse(self.backups.backupNeeded())

		# Revert the update frequency
		type(self).backups.update_freq = update_freq

	# Check newBackup
	def test_04_newBackup (self):

		# Create a new backup
		type(self).backups.newBackup(self.database)

		# Confirm two backups were found in the backups
		self.assertEqual(len(self.backups), 2)

	# Check deleteBackup
	def test_05_deleteBackup (self):

		# Confirm two backups were found in the backups
		self.assertEqual(len(self.backups), 2)

		# Delete the most recent backup
		type(self).backups.deleteBackup(self.backups[-1])

		# Confirm a single backup was found in the backups
		self.assertEqual(len(self.backups), 1)

	# Check deleteBackup
	def test_06_updateBackups (self):

		# Create a new backup
		type(self).backups.newBackup(self.database)
		type(self).backups.newBackup(self.database)
		type(self).backups.newBackup(self.database)
		type(self).backups.newBackup(self.database)

		# Confirm five backups were found in the backups
		self.assertEqual(len(self.backups), 5)

		# Save the orignal limit
		limit = self.backups.limit

		# Update the limit
		type(self).backups.limit = 4

		self.backups.updateBackups()

		# Confirm four backups were found in the backups
		self.assertEqual(len(self.backups), 4)

if __name__ == "__main__":
	unittest.main(verbosity = 2)
