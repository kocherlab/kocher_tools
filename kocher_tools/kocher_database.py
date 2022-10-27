import os
import json
import logging

import pandas as pd
import numpy as np

from dateutil import parser
from Bio import SeqIO

from kocher_tools.database import *
from kocher_tools.assignment import readInputFile

def insertCollectionFileUsingConfig (config_data, schema, filepath, uploader, ignore_previously_entered):

	def check_date (data, date = False):
		try:
			date_time = parser.parse(data, fuzzy=True)
			if date: date_time = date_time.date()
			return date_time
		except:
			return ''

	# Start the SQL session
	sql_connection = startSessionFromConfig(config_data)

	try:

		# Read in the file as a pandas dataframe and convert labels, if needed
		input_dataframe = readInputFile(filepath)

		colleciton_label_dict = {'Barcode': 'Unique ID',
								 'Date': 'Date Collected',
								 'Time': 'Time Entered',
								 'Has Pollen': 'Has Pollen?',
								 'Preservation Method': 'Sample Preservation Method',
								 'Head in PFA?': 'Head Preserved',
								 'From Nest': 'From Nest?',
								 'Field_ID': 'Species Guess'}
		input_dataframe = input_dataframe.rename(columns=colleciton_label_dict)

		# Prep the dataframe
		input_dataframe = prepDataFrameUsingConfig(config_data, schema, input_dataframe)
		input_dataframe['collection_file'] = os.path.basename(filepath)
		if uploader: input_dataframe['collected_by'] = ' '.join(uploader)

		# Check for the unique ID column
		#if 'unique_id' not in input_dataframe.columns: raise Exception(f'Unable to assign IDs, cannot locate the assignment column')

		# Update the dates
		input_dataframe['date_collected'] = input_dataframe['date_collected'].apply(check_date, date = True)
		input_dataframe['date_collected'] = pd.to_datetime(input_dataframe['date_collected']).dt.date

		# Clean up the data, with dates
		input_dataframe = input_dataframe.replace({np.nan: None})

		# Assign the table
		sql_table_assign = config_data[schema]

		# Ignore - i.e. don't raise an exception - if previously entered data is found
		if ignore_previously_entered:

			# Check for previously entered data, and filter out
			sql_column_assign = config_data.getSQLColumn(sql_table_assign.columns, 'unique_id')
			sql_select = SQLSelect.fromConfig(config_data, sql_connection)
			sql_select.addColumnToSelect(sql_column_assign)
			sql_select.addDictWhere({'unique_id':input_dataframe['unique_id'].values}, include = True, cmp_type = 'IN', dict_type = 'Column')
			sql_select.select()
			select_dataframe = sql_select.toDataFrame()
			if not select_dataframe.empty: input_dataframe = input_dataframe[~input_dataframe['unique_id'].isin(select_dataframe['unique_id'].values)]

			# Check if all the data was removed
			if input_dataframe.empty: 
				logging.warning(f'No new collection samples.')
				sql_connection.close()
				return

			# Check if any data was removed
			if not select_dataframe.empty:
				logging.warning(f'{select_dataframe.shape[0]} collection samples already in database.')

		# Insert the dataframe into the database
		sql_table_assign = config_data[schema]
		sql_insert = SQLInsert.fromConfig(config_data, sql_connection)
		sql_insert.addTableToInsert(sql_table_assign)
		sql_insert.addDataFrameValues(input_dataframe)
		sql_insert.insert()

	except:
		sql_connection.rollback()
		raise

	else:
		sql_connection.commit()

def insertBarcodeFilesUsingConfig (config_data, schema, filepaths):

	def append_seq (query_id, seq_index):
		return seq_index[query_id].format('fasta')

	def join_list (value_list):
		try: return ', '.join([value_str for value_str in value_list if value_str != 'BOLD:N/A'])
		except: return ''

	# Start the SQL session
	sql_connection = startSessionFromConfig(config_data)

	try:
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

		# Assign the table
		sql_table_assign = config_data[schema]

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
		input_dataframe['species'] = input_dataframe['species'].str.replace('_', ' ')
		input_dataframe['bold_id'] = input_dataframe['bold_id'].str.replace('_', ' ')
		input_dataframe['sequence_status'] = 'Species Identified'
		input_dataframe = input_dataframe.rename(columns = sequence_label_dict)
		input_dataframe = input_dataframe.replace({np.nan: None})

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
			failed_dataframe = failed_dataframe.replace({np.nan: None})
			
			sql_insert = SQLInsert.fromConfig(config_data, sql_connection)
			sql_insert.addTableToInsert(sql_table_assign)
			sql_insert.addDataFrameValues(failed_dataframe)
			sql_insert.insert()

		sequence_index.close()

	except:
		sql_connection.rollback()
		raise

	else:
		sql_connection.commit()

def insertStorageFileUsingConfig (config_data, filepath, schema = 'storage', plates_schema = 'plates', boxes_schema = 'boxes'):

	# Start the SQL session
	sql_connection = startSessionFromConfig(config_data)

	try:
		# Read in the file as a pandas dataframe, prep for database
		input_dataframe = pd.read_csv(filepath, dtype = str, sep = '\t')

		# Create the storage dataframe and prepare
		storage_dataframe = input_dataframe.copy()
		storage_dataframe = prepDataFrameUsingConfig(config_data, schema, storage_dataframe)
		storage_dataframe = storage_dataframe.replace({np.nan: None})

		# Insert the plates, if not already inserted
		plate_dataframe = storage_dataframe[['plate']].copy()
		plate_dataframe = plate_dataframe.drop_duplicates()
		plate_insert = SQLInsert.fromConfig(config_data, sql_connection)
		plate_insert.addTableToInsert(config_data['plates'])
		plate_insert.addDataFrameValues(plate_dataframe)
		plate_insert.addIgnore('plate')
		plate_insert.insertIgnore()

		# Confirm the input has a Box column
		if 'Box' in input_dataframe.columns:

			# Insert the boxes, if not already inserted
			box_dataframe = input_dataframe[['Box']].copy()
			box_dataframe = prepDataFrameUsingConfig(config_data, 'boxes', box_dataframe)
			box_dataframe = box_dataframe.drop_duplicates()
			box_dataframe = box_dataframe.replace({np.nan: None})
			box_insert = SQLInsert.fromConfig(config_data, sql_connection)
			box_insert.addTableToInsert(config_data['boxes'])
			box_insert.addDataFrameValues(box_dataframe)
			box_insert.addIgnore('box')
			box_insert.insertIgnore()
		
			# Create a dataframe with just the plates and boxes
			plate_update_dataframe = input_dataframe[['Plate', 'Box']].copy()
			plate_update_dataframe = plate_update_dataframe.rename(columns = {'Plate':'plate', 'Box':'box'})
			plate_update_dataframe = plate_update_dataframe.drop_duplicates()
			plate_update_dataframe = plate_update_dataframe.replace({'': None})
			plate_update_dataframe = plate_update_dataframe.dropna(axis = 'index')

			# Check for possible assignment errors
			if plate_update_dataframe['plate'].duplicated().any(): raise Exception('Plate found in more than one box')

			# Convert the dataframe into a list of dicts
			for update_dict in plate_update_dataframe.to_dict('records'):

				# Create the where and value dicts
				where_dict = {'plate': update_dict['plate']}
				value_dict = {key:value for key, value in update_dict.items() if key != 'plate'}

				# Confirm there are values to add
				if not value_dict: continue 

				plate_update = SQLUpdate.fromConfig(config_data, sql_connection)
				plate_update.addTableToUpdate(config_data['plates'])
				plate_update.addDictValues(value_dict)
				plate_update.addDictWhere(where_dict, dict_type = 'Column', include = True, cmp_type = 'EQ')
				plate_update.update()

			# Insert a boxes dataframe, for updating if possible
			box_update_dataframe = input_dataframe.copy()
			box_update_dataframe = prepDataFrameUsingConfig(config_data, 'boxes', box_update_dataframe)
			box_update_dataframe = box_dataframe.drop_duplicates()
			box_update_dataframe = box_update_dataframe.replace({'': None})
			box_update_dataframe = box_update_dataframe.dropna(axis = 'index')

			# Check for possible assignment errors
			if box_update_dataframe['box'].duplicated().any(): raise Exception('Box found in multiple locations')

			# Convert the dataframe into a list of dicts
			for update_dict in box_update_dataframe.to_dict('records'):

				# Create the where and value dicts
				where_dict = {'box': update_dict['box']}
				value_dict = {key:value for key, value in update_dict.items() if key != 'box'}

				# Confirm there are values to add
				if not value_dict: continue 

				box_update = SQLUpdate.fromConfig(config_data, sql_connection)
				box_update.addTableToUpdate(config_data['boxes'])
				box_update.addDictValues(value_dict)
				box_update.addDictWhere(where_dict, dict_type = 'Column', include = True, cmp_type = 'EQ')
				box_update.update()

		storage_insert = SQLInsert.fromConfig(config_data, sql_connection)
		storage_insert.addTableToInsert(config_data[schema])
		storage_insert.addDataFrameValues(storage_dataframe)
		storage_insert.insert()

	except:
		sql_connection.rollback()
		raise

	else:
		sql_connection.commit()