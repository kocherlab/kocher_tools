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
import tarfile

from kocher_tools.fastq_multx import *
from tests.functions import fileComp, gzFileComp, randomGenerator

# Run tests for fastq_multx.py
class test_fastq_multx (unittest.TestCase):

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

	# Check fastq_multx callFastqMultx function
	def test_01_callFastqMultx (self):

		# Assign the test input
		reformatted_i5_map_file = os.path.join(self.expected_path, 'test_fastq_multx_map.txt')
		test_i5_read_file = os.path.join(self.expected_path, 'test_pipeline_read_3.fastq.gz')
		test_i7_read_file = os.path.join(self.expected_path, 'test_pipeline_read_2.fastq.gz')
		test_R1_read_file = os.path.join(self.expected_path, 'test_pipeline_read_1.fastq.gz')
		test_R2_read_file = os.path.join(self.expected_path, 'test_pipeline_read_4.fastq.gz')

		# Create the basic input arg list
		multiplex_args = ['-B', reformatted_i5_map_file, test_i5_read_file, test_i7_read_file, test_R1_read_file, test_R2_read_file]

		# Assign the tmp output dir
		test_dir = os.path.join(self.test_dir, 'Pipeline_Test')

		# Create test tmp directory
		if not os.path.exists(test_dir):
			os.makedirs(test_dir)

		# Assign the expected output
		expected_i7 = os.path.join(test_dir, '%_i7.fastq.gz')
		expected_R1 = os.path.join(test_dir, '%_R1.fastq.gz')
		expected_R2 = os.path.join(test_dir, '%_R2.fastq.gz')

		# Add the output args
		multiplex_args.extend(['-o', 'n/a', '-o', expected_i7, '-o', expected_R1, '-o', expected_R2])

		# Call BLAST
		callFastqMultx(multiplex_args)

		# Assign the expected plates and locus
		expected_plates = ['SD_04', 'SD_07']
		expected_locus = 'Lep'

		# Loop the plate
		for expected_plate in expected_plates:

			# Assign the test files
			test_plate_i7_file = os.path.join(test_dir, '%s_%s_i7.fastq.gz' % (expected_plate, expected_locus))
			test_plate_R1_file = os.path.join(test_dir, '%s_%s_R1.fastq.gz' % (expected_plate, expected_locus))
			test_plate_R2_file = os.path.join(test_dir, '%s_%s_R2.fastq.gz' % (expected_plate, expected_locus))

			# Confirm the files exists
			self.assertTrue(os.path.isfile(test_plate_i7_file))
			self.assertTrue(os.path.isfile(test_plate_R1_file))
			self.assertTrue(os.path.isfile(test_plate_R2_file))

			# Assign the expected results path
			expected_results_path = os.path.join(self.expected_pipeline_path, expected_plate, expected_locus)

			# Assign the expected files
			expected_plate_i7_file = os.path.join(expected_results_path, '%s_%s_i7.fastq.gz' % (expected_plate, expected_locus))
			expected_plate_R1_file = os.path.join(expected_results_path, '%s_%s_R1.fastq.gz' % (expected_plate, expected_locus))
			expected_plate_R2_file = os.path.join(expected_results_path, '%s_%s_R2.fastq.gz' % (expected_plate, expected_locus))

			# Confirm the files contents were created as expected
			self.assertTrue(gzFileComp(test_plate_i7_file, expected_plate_i7_file, self.test_dir))
			self.assertTrue(gzFileComp(test_plate_R1_file, expected_plate_R1_file, self.test_dir))
			self.assertTrue(gzFileComp(test_plate_R2_file, expected_plate_R2_file, self.test_dir))

	# Check fastq_multx checkFastqMultxForErrors function
	def test_02_checkFastqMultxForErrors (self):

		# Assign the test input
		reformatted_i5_map_file = os.path.join(self.expected_path, 'test_fastq_multx_map.txt' + randomGenerator())
		test_i5_read_file = os.path.join(self.expected_path, 'test_pipeline_read_3.fastq.gz')
		test_i7_read_file = os.path.join(self.expected_path, 'test_pipeline_read_2.fastq.gz')
		test_R1_read_file = os.path.join(self.expected_path, 'test_pipeline_read_1.fastq.gz')
		test_R2_read_file = os.path.join(self.expected_path, 'test_pipeline_read_4.fastq.gz')

		# Create the basic input arg list
		multiplex_args = ['-B', reformatted_i5_map_file, test_i5_read_file, test_i7_read_file, test_R1_read_file, test_R2_read_file]

		# Check that the fastq_multx fails
		self.assertRaises(Exception, callFastqMultx, multiplex_args)

	# Check fastq_multx assignOutput function
	def test_03_assignOutput (self):

		# Assign the expected output
		expected_i5 = os.path.join(self.expected_path, '%_i5.fastq.gz')
		expected_i7 = os.path.join(self.expected_path, '%_i7.fastq.gz')
		expected_R1 = os.path.join(self.expected_path, '%_R1.fastq.gz')
		expected_R2 = os.path.join(self.expected_path, '%_R2.fastq.gz')

		# Assign the i5 test lists using the command
		test_i5_1 = assignOutput(self.expected_path, True, 'i5', True)
		test_i5_2 = assignOutput(self.expected_path, False, 'i5', True)
		test_i7_1 = assignOutput(self.expected_path, True, 'i7', True)
		test_i7_2 = assignOutput(self.expected_path, False, 'i7', True)

		# Check the test lists are as expected
		self.assertEqual(test_i5_1, ['-o', 'n/a', '-o', expected_i7, '-o', expected_R1, '-o', expected_R2])
		self.assertEqual(test_i5_2, ['-o', expected_i5, '-o', expected_i7, '-o', expected_R1, '-o', expected_R2])
		self.assertEqual(test_i7_1, ['-o', 'n/a', '-o', expected_R1, '-o', expected_R2])
		self.assertEqual(test_i7_2, ['-o', expected_i7, '-o', expected_R1, '-o', expected_R2])

	# Check fastq_multx i5ReformatMap function
	def test_04_i5ReformatMap (self):

		# Assign the test input
		test_i5_map_file = os.path.join(self.expected_path, 'test_pipeline_i5_map.txt')

		# Assign the test output
		test_reformatted_file = os.path.join(self.test_dir, 'fastq_multx_map.txt')

		# Assign the expected output
		expected_reformatted_file = os.path.join(self.expected_path, 'test_fastq_multx_map.txt')

		# Run the command
		i5ReformatMap(test_i5_map_file, test_reformatted_file)

		# Check the file was created
		self.assertTrue(os.path.isfile(test_reformatted_file))

		# Check the file has the correct contents
		self.assertTrue(fileComp(test_reformatted_file, expected_reformatted_file))
		
	# Check fastq_multx i5BarcodeJob function
	def test_05_i5BarcodeJob(self):

		# Assign the test input
		test_i5_map_file = os.path.join(self.expected_path, 'test_pipeline_i5_map.txt')
		test_i5_read_file = os.path.join(self.expected_path, 'test_pipeline_read_3.fastq.gz')
		test_i7_read_file = os.path.join(self.expected_path, 'test_pipeline_read_2.fastq.gz')
		test_R1_read_file = os.path.join(self.expected_path, 'test_pipeline_read_1.fastq.gz')
		test_R2_read_file = os.path.join(self.expected_path, 'test_pipeline_read_4.fastq.gz')

		# Assign the tmp output dir
		test_dir = os.path.join(self.test_dir, 'Pipeline_Test')

		# Create test tmp directory
		if not os.path.exists(test_dir):
			os.makedirs(test_dir)

		# Run the command
		i5BarcodeJob(test_i5_map_file, test_i5_read_file, test_i7_read_file, test_R1_read_file, test_R2_read_file, test_dir, True)

		# Assign the expected plates and locus
		expected_plates = ['SD_04', 'SD_07']
		expected_locus = 'Lep'

		# Loop the plate
		for expected_plate in expected_plates:

			# Assign the test files
			test_plate_i7_file = os.path.join(test_dir, '%s_%s_i7.fastq.gz' % (expected_plate, expected_locus))
			test_plate_R1_file = os.path.join(test_dir, '%s_%s_R1.fastq.gz' % (expected_plate, expected_locus))
			test_plate_R2_file = os.path.join(test_dir, '%s_%s_R2.fastq.gz' % (expected_plate, expected_locus))

			# Confirm the files exists
			self.assertTrue(os.path.isfile(test_plate_i7_file))
			self.assertTrue(os.path.isfile(test_plate_R1_file))
			self.assertTrue(os.path.isfile(test_plate_R2_file))

			# Assign the expected results path
			expected_results_path = os.path.join(self.expected_pipeline_path, expected_plate, expected_locus)

			# Assign the expected files
			expected_plate_i7_file = os.path.join(expected_results_path, '%s_%s_i7.fastq.gz' % (expected_plate, expected_locus))
			expected_plate_R1_file = os.path.join(expected_results_path, '%s_%s_R1.fastq.gz' % (expected_plate, expected_locus))
			expected_plate_R2_file = os.path.join(expected_results_path, '%s_%s_R2.fastq.gz' % (expected_plate, expected_locus))

			# Confirm the files contents were created as expected
			self.assertTrue(gzFileComp(test_plate_i7_file, expected_plate_i7_file, self.test_dir))
			self.assertTrue(gzFileComp(test_plate_R1_file, expected_plate_R1_file, self.test_dir))
			self.assertTrue(gzFileComp(test_plate_R2_file, expected_plate_R2_file, self.test_dir))

	# Check fastq_multx i7BarcodeJob function
	def test_06_i7BarcodeJob (self):

		# Assign the i7 map
		test_i7_map = pkg_resources.resource_filename('kocher_tools', 'data/i7_map.txt')

		# Assign the tmp output dir
		test_dir = os.path.join(self.test_dir, 'Pipeline_Test')

		# Assign the test files
		test_plate_i7_file = os.path.join(test_dir, 'SD_04_Lep_i7.fastq.gz')
		test_plate_R1_file = os.path.join(test_dir, 'SD_04_Lep_R1.fastq.gz')
		test_plate_R2_file = os.path.join(test_dir, 'SD_04_Lep_R2.fastq.gz')

		# Run the command
		i7BarcodeJob (test_i7_map, test_plate_i7_file, test_plate_R1_file, test_plate_R2_file, test_dir, True)

		# Assign the expected results path
		expected_results_path = os.path.join(self.expected_pipeline_path, 'SD_04', 'Lep', 'Demultiplexed')

		# Loop the expected results path
		for expected_file_str in os.listdir(expected_results_path):

			# Assign the path to the expected file
			test_file = os.path.join(test_dir, expected_file_str)

			# Confirm the file exists
			self.assertTrue(os.path.isfile(test_file))

			# Assign the path to the expected file
			expected_file = os.path.join(expected_results_path, expected_file_str)

			# Confirm the file contents were created as expected
			self.assertTrue(gzFileComp(test_file, expected_file, self.test_dir))


if __name__ == "__main__":
	unittest.main(verbosity = 2)