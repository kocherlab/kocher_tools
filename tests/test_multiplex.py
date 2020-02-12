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

from kocher_tools.multiplex import Multiplex

# Run tests for assignment.py
class test_assignment (unittest.TestCase):

	@classmethod
	def setUpClass(cls):

		# Create a temporary directory
		cls.test_dir = tempfile.mkdtemp()

		# Assign the script directory
		cls.script_dir = os.path.dirname(os.path.realpath(__file__))

		# Assign the expected output directory
		cls.expected_dir = 'test_files'

		# Assign the filename of the read file
		cls.i5_read_file = os.path.join(cls.script_dir, cls.expected_dir, 'test_pipeline_read_3.fastq.gz')
		cls.i7_read_file = os.path.join(cls.script_dir, cls.expected_dir, 'test_pipeline_read_2.fastq.gz')
		cls.R1_read_file = os.path.join(cls.script_dir, cls.expected_dir, 'test_pipeline_read_1.fastq.gz')
		cls.R2_read_file = os.path.join(cls.script_dir, cls.expected_dir, 'test_pipeline_read_4.fastq.gz')

		# Assign the map files
		cls.i5_map = os.path.join(cls.script_dir, cls.expected_dir, 'test_pipeline_i5_map.txt')
		cls.i7_map = pkg_resources.resource_filename('kocher_tools', 'data/i7_map.txt')

		# Assign the database file
		cls.blast_database = os.path.join(cls.script_dir, cls.expected_dir, 'TestDB', 'TestDB.fasta')

		# Assign the output path
		cls.out_dir = os.path.join(cls.test_dir, 'Pipeline_Output')

		# Create an empty variable to store the multiplex job 
		cls.demultiplex_job = None

	@classmethod
	def tearDownClass(cls):

		# Remove the test directory after the tests
		shutil.rmtree(cls.test_dir)

	# Check that a Multiplex object may be created
	def test_01_createMultiplex (self):

		# Create the multiplex job
		demultiplex_job = Multiplex()

		# Update the config data for the other tests
		type(self).demultiplex_job = demultiplex_job

	# Check Multiplex assignOutputPath function
	def test_02_assignOutputPath (self):

		# Check if that Multiplex variable was assigned correctly
		if self.demultiplex_job == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Assign the output path for all multiplex files
		type(self).demultiplex_job.assignOutputPath(self.out_dir)

		# Check that output path was correctly assigned
		self.assertEqual(self.demultiplex_job.out_path, self.out_dir)
		self.assertTrue(os.path.exists(self.demultiplex_job.out_path))

	# Check Multiplex assignFiles function
	def test_03_assignFiles (self):

		# Check if that Multiplex variable was assigned correctly
		if self.demultiplex_job == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

			# Assign the read file for the multiplex job
			type(self).demultiplex_job.assignFiles(self.i5_read_file, self.i7_read_file, self.R1_read_file, self.R2_read_file)

			# Check that files were correctly assigned
			self.assertEqual(self.demultiplex_job.i5_file, self.i5_read_file)
			self.assertEqual(self.demultiplex_job.i7_file, self.i7_read_file)
			self.assertEqual(self.demultiplex_job.R1_file, self.R1_read_file)
			self.assertEqual(self.demultiplex_job.R2_file, self.R2_read_file)

	# Check Multiplex assignPlates function
	def test_03_assignPlates (self):

		# Check if that Multiplex variable was assigned correctly
		if self.demultiplex_job == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Assign the plate using the i5 map
		type(self).demultiplex_job.assignPlates(self.i5_map)

		# Create a list of the expected plates
		expected_plates = ['SD_04', 'SD_07']

		# Save the expected locus
		expected_locus = 'Lep'

		# Loop the expected plates
		for expected_plate in expected_plates:

			# Confirm the plate was created
			self.assertTrue(expected_plate in self.demultiplex_job)

			# Assign the test plate
			test_plate = self.demultiplex_job[expected_plate]

			# Assign the expected file paths
			expected_plate_i5_file = ''
			expected_plate_i7_file = os.path.join(self.out_dir, '%s_%s_i7.fastq.gz' % (expected_plate, expected_locus))
			expected_plate_R1_file = os.path.join(self.out_dir, '%s_%s_R1.fastq.gz' % (expected_plate, expected_locus))
			expected_plate_R2_file = os.path.join(self.out_dir, '%s_%s_R2.fastq.gz' % (expected_plate, expected_locus))

			# Check if empty files are to be created
			if test_plate.discard_empty_output == False:

				# Assign the expected file path
				expected_plate_i5_file = os.path.join(self.out_dir, '%s_%s_i5.fastq.gz' % (expected_plate, expected_locus))

			# Confirm the variables and files were correctly assigned
			self.assertEqual(test_plate.locus, expected_locus)
			self.assertEqual(test_plate.out_path, self.out_dir)
			self.assertEqual(test_plate.plate_i5_file, expected_plate_i5_file)
			self.assertEqual(test_plate.plate_i7_file, expected_plate_i7_file)
			self.assertEqual(test_plate.plate_R1_file, expected_plate_R1_file)
			self.assertEqual(test_plate.plate_R2_file, expected_plate_R2_file)

if __name__ == "__main__":
	unittest.main(verbosity = 2)