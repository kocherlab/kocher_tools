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
import tarfile

from kocher_tools.multiplex import Multiplex
from tests.functions import fileComp, gzFileComp

# Run tests for multiplex.py
class test_multiplex (unittest.TestCase):

	@classmethod
	def setUpClass (cls):

		# Create a temporary directory
		#cls.test_dir = tempfile.mkdtemp()
		cls.test_dir = 'test'

		# Assign the test path of the pipeline
		cls.test_pipeline_path = os.path.join(cls.test_dir, 'Pipeline_Output')

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
			#pipeline_tar = tarfile.open(pipeline_tar_filename, "r:gz")

			# Extract the tar into the test directory
			#pipeline_tar.extractall(path = cls.test_dir)

			# Close the pipeline tar
			#pipeline_tar.close()

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
		#shutil.rmtree(cls.test_dir)
		pass

	'''
	# Check that a Multiplex object may be created
	def test_01_createMultiplex (self):

		# Create the multiplex job
		demultiplex_job = Multiplex()

		# Update the config data for the other tests
		type(self).demultiplex_job = demultiplex_job
	'''

	# Check Multiplex assignFiles function
	def test_01_fromFiles (self):

		# Assign the paths of the test files
		test_i5_read_file = os.path.join(self.expected_path, 'test_pipeline_read_3.fastq.gz')
		test_i7_read_file = os.path.join(self.expected_path, 'test_pipeline_read_2.fastq.gz')
		test_R1_read_file = os.path.join(self.expected_path, 'test_pipeline_read_1.fastq.gz')
		test_R2_read_file = os.path.join(self.expected_path, 'test_pipeline_read_4.fastq.gz')

		type(self).demultiplex_job = Multiplex.fromFiles(i5_read_file = test_i5_read_file, 
														 i7_read_file = test_i7_read_file, 
														 r1_file = test_R1_read_file, 
														 r2_file = test_R2_read_file)

		# Check that files were correctly assigned
		self.assertEqual(self.demultiplex_job.i5_file, test_i5_read_file)
		self.assertEqual(self.demultiplex_job.i7_file, test_i7_read_file)
		self.assertEqual(self.demultiplex_job.R1_file, test_R1_read_file)
		self.assertEqual(self.demultiplex_job.R2_file, test_R2_read_file)

	# Check Multiplex assignOutputPath function
	def test_02_assignOutputPath (self):

		# Check if that Multiplex variable was assigned correctly
		if self.demultiplex_job == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Assign the output path for all multiplex files
		type(self).demultiplex_job.assignOutputPath(self.test_pipeline_path)

		# Check that output path was correctly assigned
		self.assertEqual(self.demultiplex_job.out_path, self.test_pipeline_path)
		self.assertTrue(os.path.exists(self.demultiplex_job.out_path))

	
	# Check Multiplex assignPlates function
	def test_03_assignPlates (self):

		# Check if that Multiplex variable was assigned correctly
		if self.demultiplex_job == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Assign the paths of the test files
		test_i5_map = os.path.join(self.expected_path, 'test_pipeline_i5_map.txt')

		# Assign the output dir
		test_out_dir = self.demultiplex_job.out_path

		# Assign the expected plates and locus
		expected_plates = ['SD_04', 'SD_07']
		expected_locus = 'Lep'

		# Assign the plate using the i5 map
		type(self).demultiplex_job.assignPlates(test_i5_map)

		# Loop the expected plates
		for expected_plate in expected_plates:

			# Confirm the plate was assigned
			self.assertTrue(expected_plate in self.demultiplex_job)

		# Loop the test plates
		for test_plate in type(self).demultiplex_job:

			# Assign the test plate name and out dir
			test_plate.name = test_plate.name

			# Confirm the locus was correctly assigned
			self.assertEqual(test_plate.locus, expected_locus)

			# Assign the test plate name
			test_plate.locus = test_plate.locus

			# Assign the expected file paths
			expected_plate_i7_filepath = os.path.join(test_out_dir, '%s_%s_i7.fastq.gz' % (test_plate.name, test_plate.locus))
			expected_plate_R1_filepath = os.path.join(test_out_dir, '%s_%s_R1.fastq.gz' % (test_plate.name, test_plate.locus))
			expected_plate_R2_filepath = os.path.join(test_out_dir, '%s_%s_R2.fastq.gz' % (test_plate.name, test_plate.locus))

			# Confirm the files were correctly assigned		
			self.assertEqual(test_plate.plate_i7_file, expected_plate_i7_filepath)
			self.assertEqual(test_plate.plate_R1_file, expected_plate_R1_filepath)
			self.assertEqual(test_plate.plate_R2_file, expected_plate_R2_filepath)

			# Check if empty files are to be created
			if test_plate.discard_empty_output == False:

				# Assign the expected file path
				expected_plate_i5_filepath = os.path.join(test_out_dir, '%s_%s_i5.fastq.gz' % (test_plate.name, test_plate.locus))

				# Confirm the file was correctly assigned
				self.assertEqual(test_plate.plate_i5_file, expected_plate_i5_filepath)

	# Check Multiplex deMultiplex function
	def test_04_deMultiplex (self):

		# Check if that Multiplex variable was assigned correctly
		if self.demultiplex_job == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Assign the paths of the test files
		test_i5_map = os.path.join(self.expected_path, 'test_pipeline_i5_map.txt')

		# Assign the output dir
		test_out_dir = self.demultiplex_job.out_path

		# deMultiplex using the i5 map
		type(self).demultiplex_job.deMultiplex(test_i5_map)

		# Loop the test plates
		for test_plate in type(self).demultiplex_job:

			# Assign the test files
			test_plate_i7_file = os.path.join(test_out_dir, '%s_%s_i7.fastq.gz' % (test_plate.name, test_plate.locus))
			test_plate_R1_file = os.path.join(test_out_dir, '%s_%s_R1.fastq.gz' % (test_plate.name, test_plate.locus))
			test_plate_R2_file = os.path.join(test_out_dir, '%s_%s_R2.fastq.gz' % (test_plate.name, test_plate.locus))

			# Confirm the files were created
			self.assertTrue(os.path.isfile(test_plate_i7_file))
			self.assertTrue(os.path.isfile(test_plate_R1_file))
			self.assertTrue(os.path.isfile(test_plate_R2_file))

			# Assign the expected results path
			expected_results_path = os.path.join(self.expected_pipeline_path, test_plate.name, test_plate.locus)

			# Assign the filename of files to compare
			expected_plate_i7_file = os.path.join(expected_results_path, '%s_%s_i7.fastq.gz' % (test_plate.name, test_plate.locus))
			expected_plate_R1_file = os.path.join(expected_results_path, '%s_%s_R1.fastq.gz' % (test_plate.name, test_plate.locus))
			expected_plate_R2_file = os.path.join(expected_results_path, '%s_%s_R2.fastq.gz' % (test_plate.name, test_plate.locus))

			# Confirm the files contents were created as expected
			self.assertTrue(gzFileComp(test_plate_i7_file, expected_plate_i7_file, self.test_dir))
			self.assertTrue(gzFileComp(test_plate_R1_file, expected_plate_R1_file, self.test_dir))
			self.assertTrue(gzFileComp(test_plate_R2_file, expected_plate_R2_file, self.test_dir))

			# Check if empty files are to be created
			if test_plate.discard_empty_output == False:

				# Assign the test file path
				test_plate_i5_file = os.path.join(test_out_dir, '%s_%s_i5.fastq.gz' % (test_plate.name, test_plate.locus))

				# Confirm the file was created
				self.assertTrue(os.path.isfile(test_plate_i5_file))

				# Assign the filename of file to compare
				expected_plate_i5_file = os.path.join(expected_results_path, '%s_%s_i5.fastq.gz' % (test_plate.name, test_plate.locus))

				# Confirm the file contents were created as expected
				self.assertTrue(gzFileComp(test_plate_i5_file, expected_plate_i5_file, self.test_dir))

	# Check Multiplex movePlates function
	def test_05_movePlates (self):

		# Check if that Multiplex variable was assigned correctly
		if self.demultiplex_job == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Assign the output dir
		test_out_dir = self.demultiplex_job.out_path

		# Assign the plate using the i5 map
		type(self).demultiplex_job.movePlates()

		# Assign the test unmatched file paths
		test_unmatched_i7_file = os.path.join(test_out_dir, 'unmatched_i7.fastq.gz')
		test_unmatched_R1_file = os.path.join(test_out_dir, 'unmatched_R1.fastq.gz')
		test_unmatched_R2_file = os.path.join(test_out_dir, 'unmatched_R2.fastq.gz')

		# Confirm the unmatched files were created
		self.assertTrue(os.path.isfile(test_unmatched_i7_file))
		self.assertTrue(os.path.isfile(test_unmatched_R1_file))
		self.assertTrue(os.path.isfile(test_unmatched_R2_file))

		# Check if empty files are to be created
		if self.demultiplex_job.discard_empty_output == False:

			# Assign the expected funmatched ile path
			test_unmatched_i5_file = os.path.join(test_out_dir, 'unmatched_i5.fastq.gz')

			# Confirm the file was created
			self.assertTrue(os.path.isfile(test_unmatched_i5_file))

		# Loop the test plates
		for test_plate in type(self).demultiplex_job:

			# Check that the plate dir was created
			self.assertTrue(os.path.isdir(test_plate.out_path))

			# Assign the test file paths
			test_plate_i7_file = os.path.join(test_plate.out_path, f'{test_plate.name}_{test_plate.locus}_i7.fastq.gz')
			test_plate_R1_file = os.path.join(test_plate.out_path, f'{test_plate.name}_{test_plate.locus}_R1.fastq.gz')
			test_plate_R2_file = os.path.join(test_plate.out_path, f'{test_plate.name}_{test_plate.locus}_R2.fastq.gz')

			# Confirm the files were created
			self.assertTrue(os.path.isfile(test_plate_i7_file))
			self.assertTrue(os.path.isfile(test_plate_R1_file))
			self.assertTrue(os.path.isfile(test_plate_R2_file))

			# Check if empty files are to be created
			if test_plate.discard_empty_output == False:

				# Assign the test file path
				test_plate_i5_file = os.path.join(test_plate.out_path, f'{test_plate.name}_{test_plate.locus}_i5.fastq.gz')

				# Confirm the file was created
				self.assertTrue(os.path.isfile(test_plate_i5_file))

	# Check Multiplex removeUnmatched function
	def test_06_removeUnmatched (self):

		# Check if that Multiplex variable was assigned correctly
		if self.demultiplex_job == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Assign the output dir
		test_out_dir = self.demultiplex_job.out_path

		# Remove unmatched files (this should be an option in beta)
		type(self).demultiplex_job.removeUnmatched()

		# Assign the test unmatched file paths
		test_unmatched_i7_file = os.path.join(test_out_dir, 'unmatched_i7.fastq.gz')
		test_unmatched_R1_file = os.path.join(test_out_dir, 'unmatched_R1.fastq.gz')
		test_unmatched_R2_file = os.path.join(test_out_dir, 'unmatched_R2.fastq.gz')

		# Confirm the unmatched files were created
		self.assertFalse(os.path.isfile(test_unmatched_i7_file))
		self.assertFalse(os.path.isfile(test_unmatched_R1_file))
		self.assertFalse(os.path.isfile(test_unmatched_R2_file))

		# Check if empty files are to be created
		if self.demultiplex_job.discard_empty_output == False:

			# Assign the test funmatched ile path
			test_unmatched_i5_file = os.path.join(test_out_dir, 'unmatched_i5.fastq.gz')

			# Confirm the file was created
			self.assertFalse(os.path.isfile(test_unmatched_i5_file))

	# Check Multiplex assignWells function
	def test_07_assignWells (self):

		# Check if that Multiplex variable was assigned correctly
		if self.demultiplex_job == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Loop the plates in the multiplex job
		for test_plate in type(self).demultiplex_job:

			# Assign the well of the current plate
			test_plate.assignWells()

			# Loop well letters, A to H
			for expected_char in string.ascii_uppercase[:8]:

				# Loop well position, 1 to 12
				for expected_pos in range(1, 13):

					# Save the well ID
					expected_well_ID = expected_char + str(expected_pos)
					
					# Confirm the well ID is within the plate
					self.assertTrue(expected_well_ID in test_plate)

	# Check Multiplex deMultiplexPlate function
	def test_08_deMultiplexPlate (self):

		# Check if that Multiplex variable was assigned correctly
		if self.demultiplex_job == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Assign the i7 map
		test_i7_map = pkg_resources.resource_filename('kocher_tools', 'data/i7_map.txt')

		# Loop the plates in the multiplex job
		for test_plate in type(self).demultiplex_job:

			# Run the i7 barcode job using the i7 map
			test_plate.deMultiplexPlate(test_i7_map)

			# Loop each well within the plate
			for test_well in test_plate:

				# Assign the test file paths
				test_well_R1_file = os.path.join(test_well.out_path, '%s_R1.fastq.gz' % test_well.ID)
				test_well_R2_file = os.path.join(test_well.out_path, '%s_R2.fastq.gz' % test_well.ID)

				# Confirm the files were created
				self.assertTrue(os.path.isfile(test_well_R1_file))
				self.assertTrue(os.path.isfile(test_well_R2_file))

				# Assign the pipeline results path
				pipeline_results_path = os.path.join(self.expected_pipeline_path, test_plate.name, test_plate.locus, 'Demultiplexed')

				# Assign the filename of files to compare
				expected_well_R1_file = os.path.join(pipeline_results_path, '%s_R1.fastq.gz' % test_well.ID)
				expected_well_R2_file = os.path.join(pipeline_results_path, '%s_R2.fastq.gz' % test_well.ID)

				# Confirm the files contents were created as expected
				self.assertTrue(gzFileComp(test_well_R1_file, expected_well_R1_file, self.test_dir))
				self.assertTrue(gzFileComp(test_well_R2_file, expected_well_R2_file, self.test_dir))

				# Check if empty files are to be created
				if test_well.discard_empty_output == False:

					# Assign the test file path
					test_well_i7_file = os.path.join(test_well.out_path, '%s_i7.fastq.gz' % test_well.ID)

					# Confirm the file was created
					self.assertTrue(os.path.isfile(test_well_i7_file))

					# Assign the filename of the file to compare
					expected_well_i7_file = os.path.join(pipeline_results_path, '%s_i7.fastq.gz' % test_well.ID)

					# Confirm the file contents were created as expected
					self.assertTrue(gzFileComp(test_well_i7_file, expected_well_i7_file, self.test_dir))

	# Check Multiplex moveWells function
	def test_09_moveWells (self):

		# Check if that Multiplex variable was assigned correctly
		if self.demultiplex_job == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Loop the plates in the multiplex job
		for test_plate in type(self).demultiplex_job:

			# Move the wells into the Wells directory
			test_plate.moveWells()

			# Assign the filename for the test unmatched files
			test_unmatched_R1_file = os.path.join(test_plate.out_path, 'unmatched_R1.fastq.gz')
			test_unmatched_R2_file = os.path.join(test_plate.out_path, 'unmatched_R2.fastq.gz')

			# Confirm the files were created
			self.assertTrue(os.path.isfile(test_unmatched_R1_file))
			self.assertTrue(os.path.isfile(test_unmatched_R2_file))

			# Check if empty files are to be created
			if self.demultiplex_job.discard_empty_output == False:

				# Assign the test funmatched ile path
				test_unmatched_i7_file = os.path.join(test_plate.out_path, 'unmatched_i7.fastq.gz')

				# Confirm the file was created
				self.assertTrue(os.path.isfile(test_unmatched_i7_file))

			# Loop each well within the plate
			for test_well in test_plate:

				# Create the test updated well path
				test_well_updated_out_path = os.path.join(test_well.out_path, test_well.well_dir)

				# Assign the test file paths
				test_well_R1_file = os.path.join(test_well_updated_out_path, f'{test_well.ID}_R1.fastq.gz')
				test_well_R2_file = os.path.join(test_well_updated_out_path, f'{test_well.ID}_R2.fastq.gz')

				# Confirm the files were created
				self.assertTrue(os.path.isfile(test_well_R1_file))
				self.assertTrue(os.path.isfile(test_well_R2_file))

				# Check if empty files are to be created
				if test_well.discard_empty_output == False:

					# Assign the test file path
					test_well_i7_file = os.path.join(test_well_updated_out_path, f'{test_well.ID}_i7.fastq.gz')

					# Confirm the file was created
					self.assertTrue(os.path.isfile(test_well_i7_file))

	# Check Multiplex removeUnmatchedPlate function
	def test_10_removeUnmatchedPlate (self):

		# Check if that Multiplex variable was assigned correctly
		if self.demultiplex_job == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Loop the plates in the multiplex job
		for test_plate in type(self).demultiplex_job:

			# Remove any unmatched files for the current plate
			test_plate.removeUnmatchedPlate()

			# Create the test updated plate path
			test_updated_plate_path = os.path.join(test_plate.out_path, test_plate.name, test_plate.locus)

			# Assign the filename for the test unmatched files
			test_unmatched_R1_file = os.path.join(test_updated_plate_path, 'unmatched_R1.fastq.gz')
			test_unmatched_R2_file = os.path.join(test_updated_plate_path, 'unmatched_R2.fastq.gz')

			# Confirm the files were created
			self.assertFalse(os.path.isfile(test_unmatched_R1_file))
			self.assertFalse(os.path.isfile(test_unmatched_R2_file))

			# Check if empty files are to be created
			if self.demultiplex_job.discard_empty_output == False:

				# Assign the test funmatched ile path
				test_unmatched_i7_file = os.path.join(test_updated_plate_path, 'unmatched_i7.fastq.gz')

				# Confirm the file was created
				self.assertFalse(os.path.isfile(test_unmatched_i7_file))

	# Check Multiplex mergeWell function
	def test_11_mergeWell (self):

		# Check if that Multiplex variable was assigned correctly
		if self.demultiplex_job == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Loop the plates in the multiplex job
		for test_plate in type(self).demultiplex_job:

			# Loop each well within the plate
			for test_well in test_plate:

				# Merge the R1/R2 files for the current well
				test_well.mergeWell()

				# Check if a merged file was created
				if test_well.merged_file:

					# Create the test updated well path
					test_merged_out_path = os.path.join(test_well.out_path, test_well.merged_dir)

					# Assign the test file paths
					test_merged_file = os.path.join(test_merged_out_path, '%s_merged.fastq.gz' % test_well.ID)
					test_unmerged_R1_file = os.path.join(test_merged_out_path, '%s_notmerged_R1.fastq.gz' % test_well.ID)
					test_unmerged_R2_file = os.path.join(test_merged_out_path, '%s_notmerged_R2.fastq.gz' % test_well.ID)

					# Check if the file exists
					self.assertTrue(os.path.isfile(test_merged_file))
					self.assertTrue(os.path.isfile(test_unmerged_R1_file))
					self.assertTrue(os.path.isfile(test_unmerged_R2_file))

					# Assign the pipeline results path
					pipeline_results_path = os.path.join(self.expected_pipeline_path, test_plate.name, test_plate.locus, 'Merged')

					# Assign the filename to compare
					expected_merged_file = os.path.join(pipeline_results_path, '%s_merged.fastq.gz' % test_well.ID)

					# Confirm the file contents were created as expected
					self.assertTrue(gzFileComp(test_merged_file, expected_merged_file, self.test_dir))

	# Check Multiplex truncateWell function
	def test_12_truncateWell (self):

		# Check if that Multiplex variable was assigned correctly
		if self.demultiplex_job == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Loop the plates in the multiplex job
		for test_plate in type(self).demultiplex_job:

			# Loop each well within the plate
			for test_well in test_plate:

				# Truncate the merged file for the current well
				test_well.truncateWell()

				# Check if a truncated file was created
				if test_well.truncated_file:

					# Create the test updated well path
					test_truncated_out_path = os.path.join(test_well.out_path, test_well.truncated_dir)

					# Assign the test file path
					test_truncated_file = os.path.join(test_truncated_out_path, '%s_stripped.fastq.gz' % test_well.ID)

					# Check if the file exists
					self.assertTrue(os.path.isfile(test_truncated_file))

					# Assign the pipeline results path
					pipeline_results_path = os.path.join(self.expected_pipeline_path, test_plate.name, test_plate.locus, 'Truncated')

					# Assign the filename to compare
					expected_truncated_file = os.path.join(pipeline_results_path, '%s_stripped.fastq.gz' % test_well.ID)

					# Confirm the file contents were created as expected
					self.assertTrue(gzFileComp(test_truncated_file, expected_truncated_file, self.test_dir))

	# Check Multiplex filterWell function
	def test_13_filterWell (self):

		# Check if that Multiplex variable was assigned correctly
		if self.demultiplex_job == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Loop the plates in the multiplex job
		for test_plate in type(self).demultiplex_job:

			# Loop each well within the plate
			for test_well in test_plate:

				# Filter the truncated file for the current well
				test_well.filterWell()

				# Check if a filtered file was created
				if test_well.filtered_file:

					# Create the test updated well path
					test_filtered_out_path = os.path.join(test_well.out_path, test_well.filtered_dir)

					# Assign the test file path
					test_filtered_file = os.path.join(test_filtered_out_path, '%s_filtered.fasta.gz' % test_well.ID)

					# Check if the file exists
					self.assertTrue(os.path.isfile(test_filtered_file))

					# Assign the pipeline results path
					pipeline_results_path = os.path.join(self.expected_pipeline_path, test_plate.name, test_plate.locus, 'Filtered')

					# Assign the filename to compare
					expected_filtered_file = os.path.join(pipeline_results_path, '%s_filtered.fasta.gz' % test_well.ID)

					# Confirm the file contents were created as expected
					self.assertTrue(gzFileComp(test_filtered_file, expected_filtered_file, self.test_dir))

	# Check Multiplex dereplicateWell function
	def test_14_dereplicateWell (self):

		# Check if that Multiplex variable was assigned correctly
		if self.demultiplex_job == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Loop the plates in the multiplex job
		for test_plate in type(self).demultiplex_job:

			# Loop each well within the plate
			for test_well in test_plate:

				# Dereplicate the filtered file for the current well
				test_well.dereplicateWell()

				# Check if a dereplicated file was created
				if test_well.dereplicated_file:

					# Create the test updated well path
					test_dereplicated_out_path = os.path.join(test_well.out_path, test_well.dereplicated_dir)

					# Assign the test file path
					test_dereplicated_file = os.path.join(test_dereplicated_out_path, '%s_dereplicated.fasta.gz' % test_well.ID)

					# Check if the file exists
					self.assertTrue(os.path.isfile(test_dereplicated_file))

					# Assign the pipeline results path
					pipeline_results_path = os.path.join(self.expected_pipeline_path, test_plate.name, test_plate.locus, 'Dereplicated')

					# Assign the filename to compare
					expected_dereplicated_file = os.path.join(pipeline_results_path, '%s_dereplicated.fasta.gz' % test_well.ID)

					# Confirm the file contents were created as expected
					self.assertTrue(gzFileComp(test_dereplicated_file, expected_dereplicated_file, self.test_dir))

	# Check Multiplex clusterWell function
	def test_15_clusterWell (self):

		# Check if that Multiplex variable was assigned correctly
		if self.demultiplex_job == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Loop the plates in the multiplex job
		for test_plate in type(self).demultiplex_job:

			# Loop each well within the plate
			for test_well in test_plate:

				# Cluster the dereplicated file for the current well
				test_well.clusterWell()

				# Check if a clustered file was created
				if test_well.clustered_file:

					# Create the test updated well path
					test_clustered_out_path = os.path.join(test_well.out_path, test_well.clustered_dir)

					# Assign the test file path
					test_clustered_file = os.path.join(test_clustered_out_path, '%s_clustered.fasta.gz' % test_well.ID)

					# Check if the file exists
					self.assertTrue(os.path.isfile(test_clustered_file))

					# Assign the pipeline results path
					pipeline_results_path = os.path.join(self.expected_pipeline_path, test_plate.name, test_plate.locus, 'Clustered')

					# Assign the filename to compare
					expected_clustered_file = os.path.join(pipeline_results_path, '%s_clustered.fasta.gz' % test_well.ID)

					# Confirm the file contents were created as expected
					self.assertTrue(gzFileComp(test_clustered_file, expected_clustered_file, self.test_dir))

	# Check Multiplex mostAbundantWell function
	def test_16_mostAbundantWell (self):

		# Check if that Multiplex variable was assigned correctly
		if self.demultiplex_job == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Loop the plates in the multiplex job
		for test_plate in type(self).demultiplex_job:

			# Loop each well within the plate
			for test_well in test_plate:

				# Identify the most abundant reads
				test_well.mostAbundantWell()

				# Check if a common file was created
				if test_well.common_file:

					# Create the test updated well path
					test_common_out_path = os.path.join(test_well.out_path, test_well.common_dir)

					# Assign the test file path
					test_common_file = os.path.join(test_common_out_path, '%s_common.fasta.gz' % test_well.ID)

					# Check if the file exists
					self.assertTrue(os.path.isfile(test_common_file))

					# Assign the pipeline results path
					pipeline_results_path = os.path.join(self.expected_pipeline_path, test_plate.name, test_plate.locus, 'Common')

					# Assign the filename to compare
					expected_common_file = os.path.join(pipeline_results_path, '%s_common.fasta.gz' % test_well.ID)

					# Confirm the file contents were created as expected
					self.assertTrue(gzFileComp(test_common_file, expected_common_file, self.test_dir))

	# Check Multiplex yieldMostAbundant function
	def test_17_yieldMostAbundant (self):

		# Check if that Multiplex variable was assigned correctly
		if self.demultiplex_job == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Assign the well to test
		test_well = self.demultiplex_job['SD_07']['A1']

		# Assign the expected well data
		expected_data = ['SD_07-A1_1;size=4',
						 'SD_07-A1_2;size=3',
						 'SD_07-A1_3;size=3',
						 'SD_07-A1_4;size=2',
						 'SD_07-A1_5;size=2',
						 'SD_07-A1_6;size=2']

		# Yield the most abundant reads
		for read_pos, abundant_read in enumerate(test_well.yieldMostAbundant()):

			# Check if the file was created correctly
			self.assertEqual(expected_data[read_pos], abundant_read.id)

	# Check Multiplex compileMostAbundant function
	def test_18_compileMostAbundant (self):

		# Check if that Multiplex variable was assigned correctly
		if self.demultiplex_job == None:

			# Skip the test if so
			self.skipTest('Requires test_01 to pass')

		# Assign the test files
		test_compiled_filepath = os.path.join(self.test_pipeline_path, 'Common.fasta')

		# Compile the most abundant reads into a single file
		type(self).demultiplex_job.compileMostAbundant(test_compiled_filepath)

		# Check if the file exists
		self.assertTrue(os.path.isfile(test_compiled_filepath))

		# Assign the test files
		expected_compiled_filepath = os.path.join(self.expected_pipeline_path, 'Common.fasta')

		# Confirm the test file has the correct contents
		self.assertTrue(fileComp(test_compiled_filepath, expected_compiled_filepath))

if __name__ == "__main__":
	unittest.main(verbosity = 2)