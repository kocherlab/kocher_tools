import os
import sys
import csv
import json
import logging

from Bio import SeqIO

from kocher_tools.database import insertValues, updateValues, confirmValue
from kocher_tools.storage import convertPlateWell

def assignStorageIDs (cursor, table, id_key, blast_filename, failed_filename):

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
			query_well = query.split('-')[1].rsplit('_', 1)[0]

			# Assign the unique ID using the well and plate
			id_assignments[query_seqID] = convertPlateWell(cursor, table, id_key, query_plate, query_well)

	# Check if a failed filename was assigned
	if failed_filename:

		# Open the failed file
		with open(failed_filename) as failed_file:

			# Read in the JSON data
			failed_data = json.load(failed_file)

			# Loop the JSON data, sample by sample
			for failed_sample_dict in failed_data:
				
				# Get the name of the failed sample
				failed_sample = failed_sample_dict['Query ID']

				# Assign information that needs formatting
				failed_sample_seqID = failed_sample.split(';')[0]
				failed_sample_plate = failed_sample.split('-')[0]
				failed_sample_well = failed_sample.split('-')[1].rsplit('_', 1)[0]


				# Assign the unique ID using the well and plate
				id_assignments[failed_sample_seqID] = convertPlateWell(cursor, table, id_key, failed_sample_plate, failed_sample_well)

	# Update log
	logging.info('ID assignment using Plate/Well successful')

	return id_assignments

def readSeqFiles (blast_filename, sequence_index, failed_filename, id_assignment_dict, id_key):

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
		pct_ident_index = header.index('Percent Identity')
		
		# Assign the alignment length column
		aln_len_index = header.index('Alignment Length')

		# Assign the subject ID column
		subject_id_index = header.index('Subject ID')

		# Loop the rows of data
		for blast_line in blast_reader:

			# Assign query information that needs formatting
			query = blast_line[query_id_index]
			query_seqID = query.split(';')[0]
			query_abundance = query.split('size=')[1]

			# Obtain sequence from blast index 
			query_seq = sequence_index[query].format('fasta')

			# Convert the string into a blob
			query_seq_blob = sqlite3.Binary(query_seq.encode())

			# Assign the ID key value using the well and plate
			id_key_value = id_assignment_dict[query_seqID]

			# Assign the subject information that needs formatting
			subject = blast_line[subject_id_index]
			subject_ident = subject.split('|')[0]
			subject_species = subject.split('|')[1].replace('_', ' ')

			# Assign the header
			header = [id_key, 'Sequence ID', 'Species', 'Reads', 'BOLD Identifier', 'Percent Identity', 'Alignment Length', 'Sequence Length', 'Sequence', 'Status']

			# Assign the row values
			row = [id_key_value, query_seqID, subject_species, query_abundance, subject_ident, blast_line[pct_ident_index], blast_line[aln_len_index], blast_line[query_len_index], query_seq_blob, 'Species Identified']

			# Return the header and row
			yield header, row

	# Check if the failed filename was defined
	if failed_filename:

		# Open the failed file
		with open(failed_filename) as failed_file:

			# Read in the JSON data
			failed_data = json.load(failed_file)

			# Loop the JSON data, sample by sample
			for failed_sample_dict in failed_data:
				
				# Get the name of the failed sample
				failed_sample = failed_sample_dict['Query ID']

				# Assign information that needs formatting
				failed_sample_seqID = failed_sample.split(';')[0]
				failed_sample_abundance = failed_sample.split('size=')[1]

				# Obtain sequence from blast index 
				failed_sample_seq = sequence_index[failed_sample].format('fasta')

				# Convert the string into a blob
				failed_sample_seq_blob = sqlite3.Binary(failed_sample_seq.encode())

				# Assign the ID key value using the well and plate
				id_key_value = id_assignment_dict[failed_sample_seqID]

				# Assign the header
				header = [id_key, 'Sequence ID', 'Reads', 'Sequence', 'Status']

				# Assign the row values
				row = [id_key_value, failed_sample_seqID, failed_sample_abundance, failed_sample_seq_blob, failed_sample_dict['Status']]

				# Check if the failed status is Ambiguous Hits
				if failed_sample_dict['Status'] == 'Ambiguous Hits':

					# Extend the header 
					header.extend(['Ambiguous Hits', 'BOLD Bins'])

					# Create a string of the joined species
					species_str = ', '.join(failed_sample_dict['Species'])

					# Create a string of the joined bins
					bins_str = ', '.join(failed_sample_dict['Bins'])

					# Extend the row data
					row.extend([species_str, bins_str])

				# Check if the failed status has no hits
				else:

					# Extend the header 
					header.extend(['Ambiguous Hits', 'BOLD Bins'])

					# Extend the row data
					row.extend([None, None])

				# Return the header and row
				yield header, row

def addSeqFilesToDatabase (cursor, table, blast_filename, sequence_filename, failed_filename, storage_table, storage_key):

	# Create string for file output
	file_str = '%s, %s' % (blast_filename, sequence_filename)

	# Check if a failed file was specified
	if failed_filename:

		# Update the string
		file_str += ', %s' % failed_filename

	# Update log
	logging.info('Uploading barcode files (%s) to database' % file_str)

	# Assign the Unique IDs for each sample using the storage table
	id_assignment_dict = assignStorageIDs(cursor, storage_table, storage_key, blast_filename, failed_filename)

	# Index the sequence file
	sequence_index = SeqIO.index(sequence_filename, 'fasta')

	# Loop the loc file by line
	for header, seq_data in readSeqFiles(blast_filename, sequence_index, failed_filename, id_assignment_dict, storage_key):

		# Insert the sequence and speices into the database
		insertValues(cursor, table, header, seq_data)

	sequence_index.close()

	# Update log
	logging.info('Upload successful')

def updateSeqFilesToDatabase (cursor, table, select_key, blast_filename, sequence_filename, failed_filename, storage_table, storage_key):

	# Create string for file output
	file_str = '%s, %s' % (blast_filename, sequence_filename)

	# Check if a failed file was specified
	if failed_filename:

		# Update the string
		file_str += ', %s' % failed_filename

	# Update log
	logging.info('Uploading barcode files (%s) to database' % file_str)

	# Assign the Unique IDs for each sample using the storage table
	id_assignment_dict = assignStorageIDs(cursor, storage_table, storage_key, blast_filename, failed_filename)

	# Index the sequence file
	sequence_index = SeqIO.index(sequence_filename, 'fasta')

	# Loop the seq file by line
	for header, seq_data in readSeqFiles(blast_filename, sequence_index, failed_filename, id_assignment_dict, storage_key):

		# Check if the selection key isn't among the headers
		if select_key not in header:
			raise Exception('Selection key (%s) not found. Please check the input file' % select_key)

		# Create an empty string to store the select_value
		select_value = ''

		# Create an empty set dict
		seq_set_dict = {}

		# Create an empty selection dict
		seq_select_dict = {}
		seq_select_dict['IN'] = {}

		# Loop the header and seq data
		for header_column, seq_value in zip(header, seq_data):

			# Check if the current column is the selection key
			if header_column == select_key:

				# Update the select value
				select_value = seq_value

				# Populate the selection dict
				seq_select_dict['IN'][header_column] = [seq_value]

			else:

				# Populate the selection dict
				seq_set_dict[header_column] = seq_value

		# Check that selected value is present
		if not confirmValue(cursor, table, select_key, select_value):

			# If not, log warning
			logging.warning('Entry (%s) not found, unable to update. Please check input' % select_value)

		# Update the values for the selected value
		updateValues(cursor, table, seq_select_dict, seq_set_dict)

	sequence_index.close()

	# Update log
	logging.info('Upload successful')
