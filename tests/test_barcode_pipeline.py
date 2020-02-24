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
import tarfile

from unittest.mock import patch

from kocher_tools.barcode_pipeline import *
from tests.functions import strFileComp, fileComp

# Run tests for barcode_pipeline.py
class test_barcode_pipeline (unittest.TestCase):

	@classmethod
	def setUpClass (cls):

		# Create a temporary directory
		cls.test_dir = tempfile.mkdtemp()

		# Assign the test path of the pipeline
		cls.test_pipeline_path = os.path.join(cls.test_dir, 'Pipeline_Output')

		# Assign the script directory
		cls.script_dir = os.path.dirname(os.path.realpath(__file__))

		# Assign the expected output directory
		cls.expected_dir = 'test_files'

		# Assign the expected path
		cls.expected_path = os.path.join(cls.script_dir, cls.expected_dir)

		# Assign the database file
		cls.blast_database = os.path.join(cls.expected_path, 'TestDB', 'TestDB.fasta')

		# Try create the TestPipeline directory
		try:

			# Assign the pipeline tar file path
			pipeline_tar_filename = os.path.join(cls.expected_path, 'TestPipeline.tar.gz')

			# Open the pipeline tar
			pipeline_tar = tarfile.open(pipeline_tar_filename, "r:gz")

			# Extract the tar into the test directory
			pipeline_tar.extractall(path = cls.test_dir)

			# Close the pipeline tar
			pipeline_tar.close()

			# Assign the expected path of the pipeline
			cls.expected_pipeline_path = os.path.join(cls.test_dir, 'TestPipeline')

			# Create an empty variable to store the multiplex job 
			cls.demultiplex_job = None

		# Set the directory to None if that fails
		except:

			raise Exception('Unable to generate the TestPipeline directory')

	@classmethod
	def tearDownClass (cls):

		# Remove the test directory after the tests
		shutil.rmtree(cls.test_dir)

	# Check barcode_pipeline main function
	def test_01_main (self):

		# Assign the paths of the test files
		test_i5_map = os.path.join(self.expected_path, 'test_pipeline_i5_map.txt')

		# Assign the paths of the test files
		test_i5_read_file = os.path.join(self.expected_path, 'test_pipeline_read_3.fastq.gz')
		test_i7_read_file = os.path.join(self.expected_path, 'test_pipeline_read_2.fastq.gz')
		test_R1_read_file = os.path.join(self.expected_path, 'test_pipeline_read_1.fastq.gz')
		test_R2_read_file = os.path.join(self.expected_path, 'test_pipeline_read_4.fastq.gz')

		# Assign the barcode_pipeline args
		barcode_args = [sys.argv[0], '--i5-map', test_i5_map, '--i5-read-file', test_i5_read_file,
					   '--i7-read-file', test_i7_read_file, '--R1-read-file', test_R1_read_file,
					   '--R2-read-file', test_R2_read_file, '--out-dir', self.test_pipeline_path,
					   '--blast-database', self.blast_database, '--overwrite']

		# Use mock to replace sys.argv for the test
		with patch('sys.argv', barcode_args):

			# Run the command
			main()

		# Assign the test files
		test_compiled_filepath = os.path.join(self.test_pipeline_path, 'Common.fasta')
		test_blast_filepath = os.path.join(self.test_pipeline_path, 'BLAST.out')

		# Assign the test files
		expected_compiled_filepath = os.path.join(self.expected_pipeline_path, 'Common.fasta')
		expected_blast_filepath = os.path.join(self.expected_pipeline_path, 'BLAST.out')

		# Confirm the test files have the correct contents
		self.assertTrue(fileComp(test_compiled_filepath, expected_compiled_filepath))
		self.assertTrue(fileComp(test_blast_filepath, expected_blast_filepath))

if __name__ == "__main__":
	unittest.main(verbosity = 2)