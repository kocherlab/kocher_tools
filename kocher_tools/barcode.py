import os
import sys
import csv
import json
import logging

from Bio import SeqIO

from kocher_tools.config_file import ConfigDB
from kocher_tools.database import *

def insertBarcodeFiles (config_file, schema, filepaths):

	def append_seq (query_id, seq_index):
		return seq_index[query_id].format('fasta')

	def join_list (value_list):
		try: return ', '.join([value_str for value_str in value_list if value_str != 'BOLD:N/A'])
		except: return ''

	# Assign the expected filetypes
	blast_filepath = None
	fasta_filepath = None
	failed_filepath = None

	# Loop the filepaths
	for filepath in filepaths:

		# Assign the blast file
		if filepath.endswith('.out'):
			if blast_filepath: raise Exception(f'BLAST file already assigned ({blast_filepath}), cannot assign: {filepath}')
			blast_filepath = filepath

		# Assign the fasta file
		if filepath.endswith('.fasta') or filepath.endswith('.fa') or filepath.endswith('.fas'):
			if fasta_filepath: raise Exception(f'FASTA file already assigned ({fasta_filepath}), cannot assign: {filepath}')
			fasta_filepath = filepath

		# Assign the failed file
		if filepath.endswith('.json'):
			if failed_filepath: raise Exception(f'Failed file already assigned ({failed_filepath}), cannot assign: {filepath}')
			failed_filepath = filepath

	# Confirm the insert is possible
	if not blast_filepath or not fasta_filepath: raise Exception(f'Both a BLAST and FASTA file are required to operate')

	# Open the config and assign the table
	config_data = ConfigDB.readConfig(config_file)
	sql_table_assign = config_data[schema]

	# Start the engine and connect to the database
	sql_engine = createEngineFromConfig(config_data)
	sql_connection = sql_engine.connect()

	# Assign the sequencing columns
	sequence_std_cols = ['sequence_id', 'seq_len', 'seq_percent_ident', 
						 'seq_align_len', 'sequence', 'reads', 'sample_id', 
						 'bold_id', 'species', 'sequence_status']
	
	# Assign the sequencing header data				 
	sequence_label_dict = {'Query ID': 'sequence_id', 
						   'Percent Identity': 'seq_percent_ident', 
						   'Alignment Length': 'seq_align_len',
						   'Query Length': 'seq_len'}

	# Index the sequence file
	sequence_index = SeqIO.index(fasta_filepath, 'fasta')

	# Read in the file as a pandas dataframe, prep for database
	input_dataframe = pd.read_csv(blast_filepath, dtype = str, sep = '\t')

	# Clean up the BLAST dataframe
	input_dataframe['sequence'] = input_dataframe['Query ID'].apply(append_seq, seq_index = sequence_index)
	input_dataframe[['Query ID', 'reads']] = input_dataframe['Query ID'].str.split(';size=', expand = True) 
	input_dataframe['sample_id'] = input_dataframe['Query ID'].str.split('_', expand = True)[0]
	input_dataframe[['bold_id', 'species']] = input_dataframe['Subject ID'].str.split('|', expand = True)[[0, 1]]
	input_dataframe['bold_id'] = input_dataframe['bold_id'].replace('_', ' ')
	input_dataframe['sequence_status'] = 'Species Identified'
	input_dataframe = input_dataframe.rename(columns = sequence_label_dict)

	# Remove the non standard columns
	sequence_non_std_cols = list(set(input_dataframe.columns) - set(sequence_std_cols))
	input_dataframe = input_dataframe.drop(columns =  sequence_non_std_cols)

	sql_insert = SQLInsert.fromConfig(config_data, sql_connection)
	sql_insert.addTableToInsert(sql_table_assign)
	sql_insert.addDataFrameValues(input_dataframe)
	sql_insert.insert()

	# Open the failed file
	with open(failed_filepath) as failed_file:

		# Assign the sequencing header data				 
		failed_label_dict = {'Query ID': 'sequence_id', 
							 'Status': 'sequence_status',
							 'Species': 'ambiguous_hits',
							 'Bins': 'bold_bins'}

		# Load the JSON into a dataframe
		failed_data = json.load(failed_file)
		failed_dataframe = pd.DataFrame(failed_data)
		
		# Join the lists, if found
		failed_dataframe['Species'] = failed_dataframe['Species'].apply(join_list)
		failed_dataframe['Bins'] = failed_dataframe['Bins'].apply(join_list)

		# Clean up the dataframe
		failed_dataframe['sequence'] = failed_dataframe['Query ID'].apply(append_seq, seq_index = sequence_index)
		failed_dataframe[['Query ID', 'reads']] = failed_dataframe['Query ID'].str.split(';size=', expand = True)
		failed_dataframe['sample_id'] = failed_dataframe['Query ID'].str.split('_', expand = True)[0] 
		failed_dataframe = failed_dataframe.rename(columns = failed_label_dict)
		
		sql_insert = SQLInsert.fromConfig(config_data, sql_connection)
		sql_insert.addTableToInsert(sql_table_assign)
		sql_insert.addDataFrameValues(failed_dataframe)
		sql_insert.insert()

	sequence_index.close()

'''
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
'''