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

from kocher_tools.retrieve_samples import *
from tests.functions import fileComp, stdoutToStr

# Run tests for retrieve_samples.py
class test_retrieve_samples (unittest.TestCase):

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

	@classmethod
	def tearDownClass (cls):

		# Remove the test directory after the tests
		shutil.rmtree(cls.test_dir)

	# Check barcode_pipeline main function
	def test_01_main (self):

		# Assign the filename of the database and config file
		test_database = os.path.join(self.expected_path, 'testDB.sqlite')
		test_config_file = os.path.join(self.expected_path, 'testDB.yml')

		# Assign the retrieve_samples args
		retrieve_args = [sys.argv[0], '--sqlite-db', test_database, '--yaml', test_config_file, '--column', 'Unique ID', 'Species', '--stdout']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', retrieve_args):

			# Run the command, with stdout converted to a string
			test_str = stdoutToStr(main)

			# Assign the expected string
			expected_str = 'Unique ID\tSpecies\nValue1\tValue2\nValue3\tValue4\n'

			# Confirm the string is as expected
			self.assertEqual(test_str, expected_str)

		# Assign the retrieve_samples args
		retrieve_args = [sys.argv[0], '--sqlite-db', test_database, '--yaml', test_config_file, '--column', 'Species', '--include-ID', 'Value1', '--stdout']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', retrieve_args):

			# Run the command, with stdout converted to a string
			test_str = stdoutToStr(main)

			# Assign the expected string
			expected_str = 'Species\nValue2\n'

			# Confirm the string is as expected
			self.assertEqual(test_str, expected_str)

		# Assign the retrieve_samples args
		retrieve_args = [sys.argv[0], '--sqlite-db', test_database, '--yaml', test_config_file, '--column', 'Species', '--exclude-ID', 'Value1', '--stdout']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', retrieve_args):

			# Run the command, with stdout converted to a string
			test_str = stdoutToStr(main)

			# Assign the expected string
			expected_str = 'Species\nValue4\n'

			# Confirm the string is as expected
			self.assertEqual(test_str, expected_str)

		# Assign the retrieve_samples args
		retrieve_args = [sys.argv[0], '--sqlite-db', test_database, '--yaml', test_config_file, '--column', 'Unique ID', 'Species', '--exclude-ID', 'Value1', '--include-ID', 'Value3', '--stdout']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', retrieve_args):

			# Run the command, with stdout converted to a string
			test_str = stdoutToStr(main)

			# Assign the expected string
			expected_str = 'Unique ID\tSpecies\nValue3\tValue4\n'

			# Confirm the string is as expected
			self.assertEqual(test_str, expected_str)

		# Assign the filename of the database and config file
		test_large_database = os.path.join(self.expected_path, 'testDB_large.sqlite')
		test_large_config_file = os.path.join(self.expected_path, 'testDB_large.yml')

		# Assign the retrieve_samples args
		retrieve_args = [sys.argv[0], '--sqlite-db', test_large_database, '--yaml', test_large_config_file, '--column', 'Unique ID', '--include-species', 'Genus1 Species1', '--stdout']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', retrieve_args):

			# Run the command, with stdout converted to a string
			test_str = stdoutToStr(main)

			# Assign the expected string
			expected_str = 'Unique ID\nDBtest-0001\n'

			# Confirm the string is as expected
			self.assertEqual(test_str, expected_str)

		# Assign the retrieve_samples args
		retrieve_args = [sys.argv[0], '--sqlite-db', test_large_database, '--yaml', test_large_config_file, '--column', 'Unique ID', '--exclude-species', 'Genus1 Species1', '--stdout']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', retrieve_args):

			# Run the command, with stdout converted to a string
			test_str = stdoutToStr(main)

			# Assign the expected string
			expected_str = 'Unique ID\nDBtest-0002\nDBtest-0003\nDBtest-0004\n'

			# Confirm the string is as expected
			self.assertEqual(test_str, expected_str)

		# Assign the retrieve_samples args
		retrieve_args = [sys.argv[0], '--sqlite-db', test_large_database, '--yaml', test_large_config_file, '--column', 'Unique ID', '--include-genus', 'Genus1', '--stdout']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', retrieve_args):

			# Run the command, with stdout converted to a string
			test_str = stdoutToStr(main)

			# Assign the expected string
			expected_str = 'Unique ID\nDBtest-0001\nDBtest-0002\n'

			# Confirm the string is as expected
			self.assertEqual(test_str, expected_str)

		# Assign the retrieve_samples args
		retrieve_args = [sys.argv[0], '--sqlite-db', test_large_database, '--yaml', test_large_config_file, '--column', 'Unique ID', '--exclude-genus', 'Genus2', '--stdout']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', retrieve_args):

			# Run the command, with stdout converted to a string
			test_str = stdoutToStr(main)

			# Assign the expected string
			expected_str = 'Unique ID\nDBtest-0001\nDBtest-0002\n'

			# Confirm the string is as expected
			self.assertEqual(test_str, expected_str)

		# Assign the retrieve_samples args
		retrieve_args = [sys.argv[0], '--sqlite-db', test_large_database, '--yaml', test_large_config_file, '--column', 'Unique ID', '--include-nests', '--stdout']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', retrieve_args):

			# Run the command, with stdout converted to a string
			test_str = stdoutToStr(main)

			# Assign the expected string
			expected_str = 'Unique ID\nDBtest-0003\nDBtest-0004\n'

			# Confirm the string is as expected
			self.assertEqual(test_str, expected_str)

		# Assign the retrieve_samples args
		retrieve_args = [sys.argv[0], '--sqlite-db', test_large_database, '--yaml', test_large_config_file, '--column', 'Unique ID', '--exclude-nests', '--stdout']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', retrieve_args):

			# Run the command, with stdout converted to a string
			test_str = stdoutToStr(main)

			# Assign the expected string
			expected_str = 'Unique ID\nDBtest-0001\nDBtest-0002\n'

			# Confirm the string is as expected
			self.assertEqual(test_str, expected_str)

		# Assign the retrieve_samples args
		retrieve_args = [sys.argv[0], '--sqlite-db', test_large_database, '--yaml', test_large_config_file, '--column', 'Unique ID', '--include', 'Sex', 'Male', '--stdout']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', retrieve_args):

			# Run the command, with stdout converted to a string
			test_str = stdoutToStr(main)

			# Assign the expected string
			expected_str = 'Unique ID\nDBtest-0002\n'

			# Confirm the string is as expected
			self.assertEqual(test_str, expected_str)

		# Create the test output filename
		test_retrieved_file = os.path.join(self.test_dir, 'test_retrieve_samples.out')

		# Assign the expected output filename
		expected_retrieved_file = os.path.join(self.expected_path, 'test_retrieve_samples_retrieved_1.out')

		# Assign the retrieve_samples args
		retrieve_args = [sys.argv[0], '--sqlite-db', test_large_database, '--yaml', test_large_config_file, '--column', 'Unique ID', '--exclude', 'Sex', 'Male', '--out', test_retrieved_file]

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', retrieve_args):

			# Run the command
			main()

			# Confirm the file contents are as expected
			self.assertTrue(fileComp(test_retrieved_file, expected_retrieved_file))
			
if __name__ == "__main__":
	unittest.main(verbosity = 2)