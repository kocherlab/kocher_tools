import os
import sys
import csv
import sqlite3

from Bio import SeqIO

from database import retrieveValues, insertValues, updateValues
from storage import convertPlateWell

def assignStorageIDs (database, table, id_key, blast_filename):

	# Create dict to store ID assignsments
	id_assignments = {}

	# Open the blast file
	with open(blast_filename) as blast_file:

		# Read the data file
		blast_reader = csv.reader(blast_file, delimiter = '\t')

		if sys.version_info[0] == 3:

			# Save the header
			header = next(blast_reader)

		else:
		
			# Save the header
			header = blast_reader.next()

		# Assign the query ID column
		query_id_index = header.index('Query ID')

		# Loop the rows of data
		for row in blast_reader:

			# Assign query information that needs formatting
			query = row[query_id_index]
			query_seqID = query.split(';')[0]
			query_plate = query.split('-')[0]
			query_well = query.split('-')[1].split('_')[0]

			# Assign the unique ID using the well and plate
			id_assignments[query_seqID] = convertPlateWell(database, table, id_key, query_plate, query_well)

	return id_assignments

def readSeqFiles (blast_filename, sequence_filename, id_assignment_dict):

	# Index the sequence file
	sequence_index = SeqIO.index(sequence_filename, 'fasta')

	# Open the blast file
	with open(blast_filename) as blast_file:

		# Read the data file
		blast_reader = csv.reader(blast_file, delimiter = '\t')

		if sys.version_info[0] == 3:

			# Save the header
			header = next(blast_reader)

		else:
		
			# Save the header
			header = blast_reader.next()

		# Assign the query ID column
		query_id_index = header.index('Query ID')

		# Assign the query length column
		query_len_index = header.index('Query Length')

		# Assign the percent identity column
		percent_ident_index = header.index('Percent Identity')
		
		# Assign the alignment length column
		alignment_len_index = header.index('Alignment Length')

		# Assign the subject ID column
		subject_id_index = header.index('Subject ID')

		# Loop the rows of data
		for row in blast_reader:

			# Assign query information that needs formatting
			query = row[query_id_index]
			query_seqID = query.split(';')[0]
			query_abundance = query.split('size=')[1]

			# Assign the unique ID using the well and plate
			unique_id = id_assignment_dict[query_seqID]

			# Assign the subject information that needs formatting
			subject = row[subject_id_index]
			subject_ident = subject.split('|')[0][1:]
			subject_species = subject.split('|')[1].replace('_', ' ')

			# Obtain sequence from blast index 
			subject_seq = sequence_index[query].format('fasta')

			# Convert the string into a blob
			subject_seq_blob = sqlite3.Binary(subject_seq.encode())

			# Assign the header
			header = ['Unique ID', 'Sequence ID', 'Species', 'Reads', 'Database Identifier', 'Percent Identity', 'Alignment Length', 'Sequence Length', 'Sequence']

			# Assign the row values
			row = [unique_id, query_seqID, subject_species, query_abundance, subject_ident, row[percent_ident_index], row[alignment_len_index], row[query_len_index], subject_seq_blob]

			# Return the header and row
			yield header, row

def addSeqFilesToDatabase (database, table, blast_filename, sequence_filename, storage_table, storage_key):

	# Assign the Unique IDs for each sample using the storage table
	id_assignment_dict = assignStorageIDs(database, storage_table, storage_key, blast_filename)

	# Loop the loc file by line
	for header, seq_data, in readSeqFiles(blast_filename, sequence_filename, id_assignment_dict):

		# Insert the sequence and speices into the database
		insertValues(database, table, header, seq_data)

def updateSeqFilesToDatabase (database, table, select_key, blast_filename, sequence_filename, storage_table, storage_key):

	# Assign the Unique IDs for each sample using the storage table
	id_assignment_dict = assignStorageIDs(database, storage_table, storage_key, blast_filename)

	# Loop the seq file by line
	for header, seq_data in readSeqFiles(blast_filename, sequence_filename, id_assignment_dict):

		# Check if the selection key isn't among the headers
		if select_key not in header:
			raise Exception('Selection key (%s) not found. Please check the input file' % select_key)

		# Create an empty set dict
		seq_set_dict = {}

		# Create an empty selection dict
		seq_select_dict = {}
		seq_select_dict['IN'] = {}

		# Loop the header and seq data
		for header_column, seq_value in zip(header, seq_data):

			# Check if the current column is the selection key
			if header_column == select_key:

				# Populate the selection dict
				seq_select_dict['IN'][header_column] = [seq_value]

			else:

				# Populate the selection dict
				seq_set_dict[header_column] = seq_value

		# Update the values for the selected value
		updateValues(database, table, seq_select_dict, seq_set_dict)
