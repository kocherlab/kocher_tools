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
import logging

from unittest.mock import patch

from kocher_tools.barcode_filter import *
from tests.functions import strFileComp, fileComp

# Run tests for barcode_filter.py
class test_barcode_filter (unittest.TestCase):

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

	# Check barcode_filter yieldBestHits function
	def test_01_yieldBestHits (self):

		# Assign the test blast file
		test_blast_file = os.path.join(self.expected_path, 'test_blast_BLAST_header.out')

		# Assign the expected best hits file
		expected_best_hits = os.path.join(self.expected_path, 'test_barcode_filter_1.csv')

		# Open the BLAST input file
		with open(test_blast_file) as blast_file:

			# Read the blast using DictReader
			blast_reader = csv.DictReader(blast_file, delimiter = '\t')

			# Create a string to store the test data
			test_str = ''

			# Loop each query with its best hits
			for current_query, best_blast_hits in yieldBestHits(blast_reader):

				# Confirm the function only returns a single hit
				self.assertEqual(len(best_blast_hits), 1)

				# Update the test string
				test_str += '%s,%s\n' % (current_query, best_blast_hits[0]['Subject ID'])

			# Confirm the test has the correct contents
			self.assertTrue(strFileComp(test_str, expected_best_hits))

		# Assign the test blast file
		test_blast_file = os.path.join(self.expected_path, 'test_barcode_BLAST_unsorted.out')
		
		# Assign the expected best hits file
		expected_best_hits = os.path.join(self.expected_path, 'test_barcode_filter_2.csv')

		# Open the BLAST input file
		with open(test_blast_file) as blast_file:

			# Read the blast using DictReader
			blast_reader = csv.DictReader(blast_file, delimiter = '\t')

			# Create a string to store the test data
			test_str = ''

			# Loop each query with its best hits
			for current_query, best_blast_hits in yieldBestHits(blast_reader):

				# Check if the query should have 2 blast hits
				if current_query in ['SD_04-A1_1;size=6', 'SD_04-A2_1;size=5']:

					# Confirm the function only returns a single hit
					self.assertEqual(len(best_blast_hits), 2)

					# Update the test string
					test_str += '%s,%s\n' % (current_query, best_blast_hits[0]['Subject ID'])
					test_str += '%s,%s\n' % (current_query, best_blast_hits[1]['Subject ID'])

				# Check if the query should have 1 blast hits
				else:

					# Confirm the function only returns a single hit
					self.assertEqual(len(best_blast_hits), 1)

					# Update the test string
					test_str += '%s,%s\n' % (current_query, best_blast_hits[0]['Subject ID'])

			# Confirm the test has the correct contents
			self.assertTrue(strFileComp(test_str, expected_best_hits))

	# Check barcode_filter sortBestHits function
	def test_02_sortBestHits (self):

			# Assign the test best hits list
			test_best_hits = [{'Query ID': 'SD_07-H12_1;size=19', 'Query Length': '313', 'Subject ID': 'SortSeq-2', 
							   'Subject Length': '313', 'Percent Identity': '95.000', 'Alignment Length': '313', 
							   'Mismatches': '0', 'Gaps': '0', 'Query Alignment Start': '1', 'Query Alignment End': '313', 
							   'Subject Alignment Start': '1', 'Subject Alignment End': '313', 'E-Value': '7.78e-168', 
							   'Bitscore': '579'},
							  {'Query ID': 'SD_07-H12_1;size=19', 'Query Length': '313', 'Subject ID': 'SortSeq-1', 
							   'Subject Length': '313', 'Percent Identity': '100.000', 'Alignment Length': '313', 
							   'Mismatches': '0', 'Gaps': '0', 'Query Alignment Start': '1', 'Query Alignment End': '313', 
							   'Subject Alignment Start': '1', 'Subject Alignment End': '313', 'E-Value': '7.78e-168', 
							   'Bitscore': '579'}]

			# Assign the expected sorted hits list
			expected_sorted_hits = [{'Query ID': 'SD_07-H12_1;size=19', 'Query Length': '313', 'Subject ID': 'SortSeq-1', 
									'Subject Length': '313', 'Percent Identity': '100.000', 'Alignment Length': '313', 
									'Mismatches': '0', 'Gaps': '0', 'Query Alignment Start': '1', 'Query Alignment End': '313', 
									'Subject Alignment Start': '1', 'Subject Alignment End': '313', 'E-Value': '7.78e-168', 
									'Bitscore': '579'}]

			# Sort the hits by percent identity
			test_sorted_hits = sortBestHits(test_best_hits, 'percent_ident')
			
			# Confirm the function return the sorted hits correctly
			self.assertEqual(test_sorted_hits, expected_sorted_hits)

	# Check barcode_filter main function
	def test_03_main (self):

		# Assign the test blast file
		test_blast_file = os.path.join(self.expected_path, 'test_barcode_BLAST_unsorted.out')

		# Assign the expected filtered hits file
		expected_filtered_file = os.path.join(self.expected_path, 'test_barcode_BLAST_filtered.out')

		# Assign the test filtered hits file
		test_filtered_file = os.path.join(self.test_dir, 'test_BLAST_filtered.out')

		# Assign the barcode_filter args
		barcode_args = [sys.argv[0], '--blast-file', test_blast_file, '--overwrite', '--abundance-cutoff', '0', 
					   '--out-blast', test_filtered_file]

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', barcode_args):

			# Run the command
			main()

		# Confirm the test file has the correct contents
		self.assertTrue(fileComp(test_filtered_file, expected_filtered_file))

		# Assign the expected filtered hits files
		expected_filtered_passed_file = os.path.join(self.expected_path, 'test_barcode_BLAST_filtered_passed.out')
		expected_filtered_failed_file = os.path.join(self.expected_path, 'test_barcode_BLAST_filtered_failures.json')

		# Assign the test filtered hits files
		test_filtered_passed_file = os.path.join(self.test_dir, 'test_BLAST_filtered_passed.out')
		test_filtered_failed_file = os.path.join(self.test_dir, 'test_BLAST_filtered_failures.json')

		# Assign the barcode_filter args
		barcode_args = [sys.argv[0],'--blast-file', test_blast_file, '--overwrite', '--abundance-cutoff', '4', 
					   '--out-blast', test_filtered_passed_file, '--out-failed', test_filtered_failed_file]

		# Disable logs for the next test, as warning messages are expected to be logged
		logging.disable(logging.CRITICAL)

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', barcode_args):

			# Run the command
			main()

		# Confirm the test files have the correct contents
		self.assertTrue(fileComp(test_filtered_passed_file, expected_filtered_passed_file))
		self.assertTrue(fileComp(test_filtered_failed_file, expected_filtered_failed_file))

if __name__ == "__main__":
	unittest.main(verbosity = 2)