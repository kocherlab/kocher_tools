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
from tests.functions import strFileComp, fileComp, stdoutToStr

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

			self.assertEqual(test_str, expected_str)

		# Assign the retrieve_samples args
		retrieve_args = [sys.argv[0], '--sqlite-db', test_database, '--yaml', test_config_file, '--column', 'Species', '--include-ID', 'Value1', '--stdout']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', retrieve_args):

			# Run the command, with stdout converted to a string
			test_str = stdoutToStr(main)

			# Assign the expected string
			expected_str = 'Species\nValue2\n'

			self.assertEqual(test_str, expected_str)

if __name__ == "__main__":
	unittest.main(verbosity = 2)