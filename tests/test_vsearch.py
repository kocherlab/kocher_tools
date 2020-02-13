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

from kocher_tools.vsearch import *
from tests.functions import gzExpFileComp, randomGenerator

# Run tests for vsearch.py
class test_vsearch (unittest.TestCase):

	@classmethod
	def setUpClass(cls):

		# Create a temporary directory
		cls.test_dir = tempfile.mkdtemp()

		# Assign the script directory
		cls.script_dir = os.path.dirname(os.path.realpath(__file__))

		# Assign the expected output directory
		cls.expected_dir = 'test_files'

		# Assign the expected path
		cls.expected_path = os.path.join(cls.script_dir, cls.expected_dir)

	@classmethod
	def tearDownClass(cls):

		# Remove the test directory after the tests
		shutil.rmtree(cls.test_dir)

	# Check vsearch callVsearch function
	def test_01_callVsearch (self):

		# Assign the test input
		test_clustered_file = os.path.join(self.expected_path, 'test_vsearch_clustered_to_sort.fasta.gz')

		# Assign the test output
		test_sorted_file = os.path.join(self.test_dir, 'test_sorted_clustered.fasta')

		# Assign the expected output
		expected_sorted_file = os.path.join(self.expected_path, 'test_vsearch_sorted_clustered.fasta.gz')

		# Create the argument list for sorting
		sort_args = ['--sortbysize', test_clustered_file, '--output', test_sorted_file, '--relabel', 'SD_04-A2_', '--sizeout']

		# Call VSEARCH
		callVsearch(sort_args)

		# Check the file was created
		self.assertTrue(os.path.isfile(test_sorted_file))

		# Check the file has the correct contents
		self.assertTrue(gzExpFileComp(test_sorted_file, expected_sorted_file, self.test_dir))

	# Check vsearch checkVsearchForErrors function
	def test_02_checkVsearchForErrors (self):

		# Assign the test input
		test_clustered_file = os.path.join(self.expected_path, 'test_vsearch_clustered_to_sort.fasta.gz' + randomGenerator())

		# Assign the test output
		test_sorted_file = ''

		# Create the argument list for sorting
		sort_args = ['--sortbysize', test_clustered_file, '--output', test_sorted_file, '--relabel', 'SD_04-A2_', '--sizeout']

		# Check that the VSEARCH fails
		self.assertRaises(Exception, callVsearch, sort_args)

	# Check vsearch mergePairs function
	def test_03_mergePairs (self):

		# Assign the test input
		test_input_R1 = os.path.join(self.expected_path, 'test_vsearch_demultiplexed_R1.fastq.gz')
		test_input_R2 = os.path.join(self.expected_path, 'test_vsearch_demultiplexed_R2.fastq.gz')

		# Assign the test output
		test_merged_file = os.path.join(self.test_dir, 'test_merged.fastq')
		test_unmerged_R1_file = os.path.join(self.test_dir, 'test_notmerged_R1.fastq')
		test_unmerged_R2_file = os.path.join(self.test_dir, 'test_notmerged_R2.fastq')

		# Assign the expected output
		expected_merged_file = os.path.join(self.expected_path, 'test_vsearch_merged.fastq.gz')
		expected_unmerged_R1_file = os.path.join(self.expected_path, 'test_vsearch_notmerged_R1.fastq.gz')
		expected_unmerged_R2_file = os.path.join(self.expected_path, 'test_vsearch_notmerged_R2.fastq.gz')

		# Call the function
		mergePairs('A1', test_input_R1, test_input_R2, test_merged_file, test_unmerged_R1_file, test_unmerged_R2_file)

		# Check the files were created
		self.assertTrue(os.path.isfile(test_merged_file))
		self.assertTrue(os.path.isfile(test_unmerged_R1_file))
		self.assertTrue(os.path.isfile(test_unmerged_R2_file))

		# Check the files have the correct contents
		self.assertTrue(gzExpFileComp(test_merged_file, expected_merged_file, self.test_dir))
		self.assertTrue(gzExpFileComp(test_unmerged_R1_file, expected_unmerged_R1_file, self.test_dir))
		self.assertTrue(gzExpFileComp(test_unmerged_R2_file, expected_unmerged_R2_file, self.test_dir))

	# Check vsearch truncateFastq function
	def test_04_truncateFastq (self):

		# Assign the test input
		test_merged_file = os.path.join(self.expected_path, 'test_vsearch_merged.fastq.gz')

		# Assign the test output
		test_stripped_file = os.path.join(self.test_dir, 'test_stripped.fastq')

		# Assign the expected output
		expected_stripped_file = os.path.join(self.expected_path, 'test_vsearch_stripped.fastq.gz')

		# Call the function
		truncateFastq(test_merged_file, test_stripped_file)

		# Check the file was created
		self.assertTrue(os.path.isfile(test_stripped_file))

		# Check the file has the correct contents
		self.assertTrue(gzExpFileComp(test_stripped_file, expected_stripped_file, self.test_dir))

	# Check vsearch filterFastq function
	def test_05_filterFastq (self):

		# Assign the test input
		test_stripped_file = os.path.join(self.expected_path, 'test_vsearch_stripped.fastq.gz')

		# Assign the test output
		test_filtered_file = os.path.join(self.test_dir, 'test_filtered.fasta')

		# Assign the expected output
		expected_filtered_file = os.path.join(self.expected_path, 'test_vsearch_filtered.fasta.gz')

		# Call the function
		filterFastq(test_stripped_file, test_filtered_file)

		# Check the file was created
		self.assertTrue(os.path.isfile(test_filtered_file))

		# Check the file has the correct contents
		self.assertTrue(gzExpFileComp(test_filtered_file, expected_filtered_file, self.test_dir))

	# Check vsearch dereplicateFasta function
	def test_06_dereplicateFasta (self):

		# Assign the test input
		test_filtered_file = os.path.join(self.expected_path, 'test_vsearch_filtered.fasta.gz')

		# Assign the test output
		test_dereplicated_file = os.path.join(self.test_dir, 'test_dereplicated.fasta')

		# Assign the expected output
		expected_dereplicated_file = os.path.join(self.expected_path, 'test_vsearch_dereplicated.fasta.gz')

		# Call the function
		dereplicateFasta('SD_04', 'A1', test_filtered_file, test_dereplicated_file)

		# Check the file was created
		self.assertTrue(os.path.isfile(test_dereplicated_file))

		# Check the file has the correct contents
		self.assertTrue(gzExpFileComp(test_dereplicated_file, expected_dereplicated_file, self.test_dir))

	# Check vsearch clusterFasta function
	def test_07_clusterFasta (self):

		# Assign the test input
		test_dereplicated_file = os.path.join(self.expected_path, 'test_vsearch_dereplicated.fasta.gz')

		# Assign the test output
		test_clustered_file = os.path.join(self.test_dir, 'test_clustered.fasta')

		# Assign the expected output
		expected_clustered_file = os.path.join(self.expected_path, 'test_vsearch_clustered.fasta.gz')

		# Call the function
		clusterFasta('SD_04', 'A1', test_dereplicated_file, test_clustered_file)

		# Check the file was created
		self.assertTrue(os.path.isfile(test_clustered_file))

		# Check the file has the correct contents
		self.assertTrue(gzExpFileComp(test_clustered_file, expected_clustered_file, self.test_dir))

	# Check vsearch sortFasta function
	def test_08_sortFasta (self):

		# Assign the test input
		test_clustered_file = os.path.join(self.expected_path, 'test_vsearch_clustered_to_sort.fasta.gz')

		# Assign the test output
		test_sorted_file = os.path.join(self.test_dir, 'test_sorted_clustered.fasta')

		# Assign the expected output
		expected_sorted_file = os.path.join(self.expected_path, 'test_vsearch_sorted_clustered.fasta.gz')

		# Call the function
		sortFasta('SD_04', 'A2', test_clustered_file, test_sorted_file)

		# Check the file was created
		self.assertTrue(os.path.isfile(test_sorted_file))

		# Check the file has the correct contents
		self.assertTrue(gzExpFileComp(test_sorted_file, expected_sorted_file, self.test_dir))

if __name__ == "__main__":
	unittest.main(verbosity = 2)