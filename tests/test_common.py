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

from kocher_tools.common import readCommonFile

# Run tests for database input from common files
class tests_05_common (unittest.TestCase):

	@classmethod
	def setUpClass(cls):

		# Assign the script directory
		cls.script_dir = os.path.dirname(os.path.realpath(__file__))

		# Assign the expected output directory
		cls.expected_dir = 'test_files'

		# Assign the expected path
		cls.expected_path = os.path.join(cls.script_dir, cls.expected_dir)

	# Check readCommonFile from common.py 
	def test_01_readCommonFile (self):

		# Assign the common filename
		common_filename = os.path.join(self.expected_path, 'test_location_01_input.tsv')

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

if __name__ == "__main__":
	unittest.main(verbosity = 2)