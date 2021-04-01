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
import subprocess

from kocher_tools.blast import *
from tests.functions import fileComp, randomGenerator

# Run tests for blast.py
class test_blast (unittest.TestCase):

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

		# Assign the database file
		cls.blast_database = os.path.join(cls.expected_path, 'TestDB', 'TestDB.fasta')

	@classmethod
	def tearDownClass (cls):

		# Remove the test directory after the tests
		shutil.rmtree(cls.test_dir)

	# Check blast callBlast function
	def test_01_callBlast (self):

		# Check that the database path will work
		self.assertFalse(' ' in self.blast_database)

		# Assign the test input
		test_common_file = os.path.join(self.expected_path, 'test_blast_common.fasta')

		# Assign the test output
		test_blast_file = os.path.join(self.test_dir, 'test_BLAST.out')

		# Assign the expected output
		expected_blast_file = os.path.join(self.expected_path, 'test_blast_BLAST.out')

		# Define the output format
		test_output_format = '6 qseqid qlen sseqid slen pident length mismatch gapopen qstart qend sstart send evalue bitscore'

		# Create the blast argument list
		blast_args = ['-query', test_common_file, '-db', self.blast_database, 
						   '-max_target_seqs', '100', '-outfmt', test_output_format, 
						   '-num_threads', str(multiprocessing.cpu_count()),
						   '-out', test_blast_file]

		# Call BLAST
		callBlast(blast_args)

		# Check the file was created
		self.assertTrue(os.path.isfile(test_blast_file))

		# Check the file has the correct contents
		self.assertTrue(fileComp(test_blast_file, expected_blast_file))

	# Check blast pipeBlast function
	def test_02_pipeBlast (self):

		# Check that the database path will work
		self.assertFalse(' ' in self.blast_database)

		# Assign the test input
		test_common_file = os.path.join(self.expected_path, 'test_blast_common.fasta')

		# Assign the test output
		test_blast_file = os.path.join(self.test_dir, 'test_BLAST.out')

		# Assign the expected output
		expected_blast_file = os.path.join(self.expected_path, 'test_blast_BLAST_header.out')

		# Define the header list
		test_header_list = ['Query ID', 'Query Length', 'Subject ID', 'Subject Length', 'Percent Identity', 'Alignment Length', 
							'Mismatches', 'Gaps', 'Query Alignment Start', 'Query Alignment End', 'Subject Alignment Start', 
							'Subject Alignment End', 'E-Value', 'Bitscore']

		# Define the output format
		test_output_format = '6 qseqid qlen sseqid slen pident length mismatch gapopen qstart qend sstart send evalue bitscore'

		# Create the blast argument list
		blast_args = ['-query', test_common_file, '-db', self.blast_database, 
						   '-max_target_seqs', '100', '-outfmt', test_output_format, 
						   '-num_threads', str(multiprocessing.cpu_count())]

		# Call BLAST
		pipeBlast(blast_args, test_blast_file, header = '\t'.join(test_header_list))

		# Check the file was created
		self.assertTrue(os.path.isfile(test_blast_file))

		# Check the file has the correct contents
		self.assertTrue(fileComp(test_blast_file, expected_blast_file))

	# Check blast checkBlastForErrors function
	def test_03_checkBlastForErrors (self):

		# Assign the test input
		test_common_file = os.path.join(self.expected_path, 'test_blast_common.fasta')

		# Assign the test output
		test_blast_file = os.path.join(self.test_dir, 'test_BLAST.out')

		# Create the blast argument list
		blast_args = ['-query', test_common_file, 
						   '-max_target_seqs', '100', '-outfmt', '6', 
						   '-num_threads', str(multiprocessing.cpu_count()),
						   '-out', test_blast_file]

		# Check that the BLAST fails
		self.assertRaises(Exception, callBlast, blast_args)

	# Check blast blastTopHit function
	def test_04_blastTopHit (self):

		# Check that the database path will work
		self.assertFalse(' ' in self.blast_database)

		# Assign the test input
		test_common_file = os.path.join(self.expected_path, 'test_blast_common.fasta')

		# Assign the test output
		test_blast_file = os.path.join(self.test_dir, 'test_BLAST.out')

		# Assign the expected output
		expected_blast_file = os.path.join(self.expected_path, 'test_blast_BLAST_header.out')

		# Call the function
		blastTopHits(test_common_file, test_blast_file, self.blast_database, multiprocessing.cpu_count())

		# Check the file was created
		self.assertTrue(os.path.isfile(test_blast_file))

		# Check the file has the correct contents
		self.assertTrue(fileComp(test_blast_file, expected_blast_file))

if __name__ == "__main__":
	unittest.main(verbosity = 2)
