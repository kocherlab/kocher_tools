import os
import sys
import logging
import subprocess
import signal

from kocher_tools.misc import confirmExecutable

import re
import glob
import pandas as pd

class fastqMultx ():
	def __init__ (self, index = '', index_format = None, i7_reverse_complement = False, i5_reverse_complement = False, pipeline_log_filename = None, 
						fastq_multx_summary_filename = None, index_barcode_len = 8, excel_sheet = None, index_header = None, keep_unknown = False, 
						keep_indices = False, **kwargs):

		# Check if the fastq-multx executable was found
		self._fastq_multx_path = confirmExecutable('fastq-multx')
		if not self._fastq_multx_path: raise IOError('fastq-multx not found. Please confirm the executable is installed')
		self._fastq_multx_call_args = []

		# Assign the index-based arguments
		self.index = index
		self.index_format = index_format
		self.excel_sheet = excel_sheet
		self.index_header = index_header
		self._index_well_col = 'Name'
		self._index_i7_col = 'i7'
		self._index_i5_col = 'i5'
		self._index_barcode_len = index_barcode_len

		# Assign general arguments
		self.i7_reverse_complement = i7_reverse_complement
		self.i5_reverse_complement = i5_reverse_complement
		self._fastq_multx_summary_filename = fastq_multx_summary_filename
		self._pipeline_log_filename = pipeline_log_filename
		self._fastq_extension = 'fastq'

		# Assign the output options
		self._keep_unknown = keep_unknown
		self._keep_indices = keep_indices

		# Process the index
		self._processIndex()

	@property
	def _barcode_cols (self):
		return [self._index_i7_col, self._index_i5_col]

	@property
	def _index_cols (self):
		return [self._index_well_col, self._index_i7_col, self._index_i5_col]

	@property
	def _output_cols (self):
		return [self._index_well_col, self._index_i7_col + self._index_i5_col]

	@property
	def fastq_multx_arg_list (self):
		return [self._fastq_multx_path, '-B', self.index] + list(map(str, self._fastq_multx_call_args))

	@classmethod
	def withIndex (cls, index, index_format, i7_reverse_complement = False, i5_reverse_complement = False, **kwargs):
		return cls(index = index, index_format = index_format, i7_reverse_complement = i7_reverse_complement, i5_reverse_complement = i5_reverse_complement, **kwargs)

	def demultiplexFASTQs (self, out_dir, i7_read_file, i5_read_file, r1_file, r2_file = None):

		def _processOptionalOutput (type_regex, optional_out_dir = None):

			# Assign the optional output path, if specified
			if optional_out_dir:
				optional_out_path = os.path.join(out_dir, optional_out_dir)
				if not os.path.exists(optional_out_path): os.makedirs(optional_out_path)
			else: optional_out_path = None

			# Process the optional file
			for optional_file in glob.glob(os.path.join(out_dir, type_regex)):
				if not optional_out_path: os.remove(optional_file)
				else:
					optional_filename = os.path.basename(optional_file)
					optional_out_file = os.path.join(optional_out_path, optional_filename)
					shutil.move(optional_file, optional_out_file)

		logging.info('Started FASTQ paired-index demultiplex')

		# Create the output prefix and regex
		if not os.path.exists(out_dir): os.makedirs(out_dir)
		output_regex = os.path.join(out_dir, '%%_%s.') + f'{self._fastq_extension}.gz'

		# Assign the FASTQ arguments
		self._fastq_multx_call_args.extend([i7_read_file, i5_read_file, r1_file])
		if r2_file: self._fastq_multx_call_args.append(r2_file)
		if not self._keep_indices: self._fastq_multx_call_args.extend(['-o', 'n/a', '-o', 'n/a'])
		else: self._fastq_multx_call_args.extend(['-o', output_regex  % 'i7', '-o', output_regex % 'i5'])
		self._fastq_multx_call_args.extend(['-o', output_regex  % 'R1', '-o', output_regex % 'R2'])

		# Demultiplex
		self._call()

		'''
		Process the optional output files in one of two ways:
		1) Removal of the optional output (Default)
		2) Store the files within a sub-directory
		''' 
		_processOptionalOutput('unmatched_*.*', 'Unknown' if self._keep_unknown else None)
		_processOptionalOutput('*_i[57].*', 'Indices' if self._keep_indices else None)

		logging.info('Finished FASTQ paired-index demultiplex')

	def _processIndex (self):

		def _updateExcelHeader (excel_dataframe):

			# Create arguments to store header column names
			well_col = None
			i7_col = None
			i5_col = None

			# Assign the header column names
			for index_col in excel_dataframe.columns:
				if 'sample' in str(index_col).lower() or 'well' in str(index_col).lower():
					if well_col: raise Exception(f'Well column assignment error: {self.index_format}')
					well_col = index_col
				elif 'i7' in str(index_col).lower():
					if i7_col: raise Exception(f'i7 column assignment error: {self.index_format}')
					i7_col = index_col
				elif 'i5' in str(index_col).lower():
					if i5_col: raise Exception(f'i5 column assignment error: {self.index_format}')
					i5_col = index_col

			# Confirm all the headers were found
			if not well_col or not i7_col or not i5_col: raise Exception(f'Unable to update header')

			# Return the updated header names
			return excel_dataframe.rename({i7_col: self._index_i7_col, i5_col: self._index_i5_col, well_col: self._index_well_col}, axis = 'columns')

		def _hasWhitespace (cols):

			if not isinstance(cols, list): cols = [cols]
			for col in cols:
				if index_dataframe[col].str.contains('\s+').any(): return True
			return False

		def _removeWhitespace (cols):

			if not isinstance(cols, list): cols = [cols]
			for col in cols:
				index_dataframe[col] = index_dataframe[col].str.replace(' ', '')

		def _revCompBarcodes (barcode):

			# Reverse complement the barcode
			complements = str.maketrans('ATCG','TAGC')
			return barcode[::-1].translate(complements)

		# Open index file, if possible
		if self.index_format == 'excel':
			index_dataframe = pd.read_excel(self.index, sheet_name = self.excel_sheet, engine = 'openpyxl')
			index_dataframe = _updateExcelHeader(index_dataframe)
		elif self.index_format == 'tsv': index_dataframe = pd.read_csv(self.index, sep = '\t')
		else: raise Exception(f'Unknown index format: {self.index_format}')

		# Check for empty rows
		empty_rows = index_dataframe.isnull().all(1).any()
		if empty_rows: index_dataframe = index_dataframe.dropna(how='all')

		# Confirm the headers are correct
		if not set(self._index_cols) <= set(index_dataframe.columns): raise Exception(f'Unable to confirm the correct headers')

		# Check the number of columns are not too few or too many
		number_of_columns = len(index_dataframe.columns)
		if number_of_columns < len(self._index_cols): raise Exception(f'Expected {len(self._index_cols)} columns, found {number_of_columns} columns')
		elif number_of_columns > len(self._index_cols): index_dataframe = index_dataframe[self._index_cols]

		# Remove any whitespace is the wells/samples, if needed
		sample_whitespace = _hasWhitespace(self._index_well_col)
		if sample_whitespace: _removeWhitespace(self._index_well_col)

		# Remove any whitespace in the barcodes, if needed
		barcode_whitespace = _hasWhitespace(self._barcode_cols)
		if barcode_whitespace: _removeWhitespace(self._barcode_cols)

		# Check the length of the barcodes - whitespace must be removed prior
		if not (index_dataframe[self._index_i7_col].str.len() == self._index_barcode_len).all():
			raise Exception(f'i7 barcodes not {self._index_barcode_len} bases long')
		if not (index_dataframe[self._index_i5_col].str.len() == self._index_barcode_len).all():
			raise Exception(f'i5 barcodes not {self._index_barcode_len} bases long')

		# Reverse complement the bacodes, if needed
		if self.i7_reverse_complement: index_dataframe[self._index_i7_col] = index_dataframe[self._index_i7_col].apply(_revCompBarcodes)
		if self.i5_reverse_complement: index_dataframe[self._index_i5_col] = index_dataframe[self._index_i5_col].apply(_revCompBarcodes)

		# Check if the file needs to be formatted
		if self.index_format == 'excel' and not empty_rows and not self.i7_reverse_complement and not self.i5_reverse_complement and not sample_whitespace and not barcode_whitespace and number_of_columns == len(self._index_cols): return
		
		# Combine the barcodes and assign the output columns
		index_dataframe[self._index_i7_col + self._index_i5_col] = index_dataframe[self._index_i7_col].astype(str) + '-' + index_dataframe[self._index_i5_col].astype(str)
		index_dataframe = index_dataframe[self._output_cols]

		# Create the new index file
		self.index = f'{self.index}.fastq_multx.formatted'
		index_dataframe.to_csv(self.index , sep = '\t', index = False, header = False)

		logging.info(f'Index file ({self.index}) processed and assigned.')

	def _call (self):
		'''
			Standard call of fastq-multx

			The function calls fastq-multx.

			Raises
			------
			Exception
				If fastq-multx stderr returns an error
		'''

		# fastq-multx subprocess call
		fastq_multx_call = subprocess.Popen(self.fastq_multx_arg_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

		# Wait for fastq-multx to finish
		fastq_multx_out, fastq_multx_err = fastq_multx_call.communicate()

		# If needed, convert bytes to string
		if sys.version_info[0] == 3:
			fastq_multx_out = fastq_multx_out.decode()
			fastq_multx_err = fastq_multx_err.decode()

		logging.info('fastq-multx call complete')

		# Create the summary file
		fastq_multx_summary_file = open(self._fastq_multx_summary_filename, 'w')
		fastq_multx_summary_file.write(fastq_multx_out)
		fastq_multx_summary_file.close()

		# Check that the log file was created correctly
		self._check_for_errors(fastq_multx_err)

		# Assign the stderr lines
		start_message = ('#' * 12) + ' Start fastq-multx stdout  ' + ('#' * 12)
		end_massage = ('#' * 13) + ' End fastq-multx stdout  ' + ('#' * 13)
		spacer_line = '#' * 44

		# Append the log file
		logging.info(f'\n{spacer_line}\n{start_message}\n{fastq_multx_err}{fastq_multx_out}{end_massage}\n{spacer_line}\n')

	@staticmethod
	def _check_for_errors (fastq_multx_stderr):
		'''
			Checks the plink stderr for errors

			Parameters
			----------
			fastq_multx_stderr : str
				plink stderr

			Raises
			------
			IOError
				If plink stderr returns an error
		'''

		# Print errors, if found
		if 'End used: start' not in fastq_multx_stderr: raise Exception(fastq_multx_stderr)


def assignOutput (out_path, discard_empty_output, barcode_type, r2_given):

	# Create list to hold commands
	output_list = []

	# Define the basic filename
	output_filename = os.path.join(out_path, '%%_%s.fastq.gz')

	# Check if the barcode type is i5
	if barcode_type == 'i5':

		# Discard empty output, if specified
		if discard_empty_output: output_list.extend(['-o', 'n/a']) 
		else: output_list.extend(['-o', output_filename % 'i5'])

		# Assign the i7 file
		output_list.extend(['-o', output_filename % 'i7'])

	# Check if the barcode type is i7
	elif barcode_type == 'i7':
	
		# Discard empty output, if specified
		if discard_empty_output: output_list.extend(['-o', 'n/a']) 
		else: output_list.extend(['-o', output_filename % 'i7'])

	# Assign the R1 and R2 output
	output_list.extend(['-o', output_filename % 'R1'])
	if r2_given: output_list.extend(['-o', output_filename % 'R2'])

	return output_list

def i5ReformatMap (i5_map_filename, reformatted_filename, reverse_complement_barcodes):

	# Assign the base complements, if required
	if reverse_complement_barcodes: complements = str.maketrans('ATCG','TAGC')

	# Open the reformatted file
	reformatted_i5_map = open(reformatted_filename, 'w')

	# Open the i5 map file
	with open(i5_map_filename) as i5_map_file:

		# Loop the i5 map file, line by line
		for i5_map_line in i5_map_file:

			# Split the line into: plate, barcode, and locus (if given)
			try: i5_plate, i5_barcode, i5_locus = i5_map_line.split()
			except:
				i5_plate, i5_barcode = i5_map_line.split()
				i5_locus = ''

			# Reverse complemente the barcode, if required
			if reverse_complement_barcodes: i5_barcode = i5_barcode[::-1].translate(complements)
				
			# Define the locus string
			locus_str = f'_{i5_locus}' if i5_locus else ''

			# Write to the reformatted file
			reformatted_i5_map.write(f'{i5_plate}{locus_str}\t{i5_barcode}\n')

	# Close the file
	reformatted_i5_map.close()

def i5BarcodeJob (i5_map_filename, i5_input, i7_input, r1_input, r2_input, out_path, discard_i5, reverse_complement_barcodes):

	# Define the reformatted i5 map filename
	reformatted_i5_map_filename = i5_map_filename + '.reformatted'

	# Reformat the i5 map
	i5ReformatMap(i5_map_filename, reformatted_i5_map_filename, reverse_complement_barcodes)

	# Create the basic input arg list
	multiplex_call_args = ['-B', reformatted_i5_map_filename, i5_input, i7_input, r1_input]
	if r2_input: multiplex_call_args.append(r2_input)

	# Add the output args, using the path, if empty files should be kept, and set the barcode type as i5
	multiplex_call_args.extend(assignOutput(out_path, discard_i5, 'i5', r2_input != None))

	# Call fastq-multz with the argus
	callFastqMultx(multiplex_call_args)

	# Remove the reformatted i5 map
	os.remove(reformatted_i5_map_filename)

def i7BarcodeJob (i7_map_filename, i7_input, r1_input, r2_input, out_path, discard_i7):

	# Create the basic input arg list
	multiplex_call_args = ['-B', i7_map_filename, i7_input, r1_input]
	if r2_input: multiplex_call_args.append(r2_input)

	# Add the output args, using the path, if empty files should be kept, and set the barcode type as i5
	multiplex_call_args.extend(assignOutput(out_path, discard_i7, 'i7', r2_input != None))

	# Call fastq-multz with the argus
	callFastqMultx(multiplex_call_args)

def checkFastqMultxForErrors (fastq_multx_stderr):

	# Loop the stderr line by line
	for fastq_multx_stderr_lines in fastq_multx_stderr:

		# Ignore barcode file statement
		if 'Using Barcode File:' in fastq_multx_stderr:
			pass

		# Ignore end used file statement
		elif 'End used:' in fastq_multx_stderr:
			pass

		# Ignore the skipped (due to distance) statement
		elif 'Skipped because of distance' in fastq_multx_stderr:
			pass

		# Check if there is an error
		elif 'Error:' in fastq_multx_stderr:

			# Report the error
			raise Exception(fastq_multx_stderr)

		# Check if there are other messages
		else:

			print ('Error? - %s' % fastq_multx_stderr)

def callFastqMultx (fastq_multx_call_args):

	fastq_multx_executable = confirmExecutable('fastq-multx')

	# Check if executable is installed
	if not fastq_multx_executable:
		raise IOError('fastq-multx not found. Please confirm the executable is installed')

	# fastq-multx subprocess call
	fastq_multx_call = subprocess.Popen([fastq_multx_executable] + fastq_multx_call_args, stderr = subprocess.PIPE, stdout = subprocess.PIPE, preexec_fn = lambda: signal.signal(signal.SIGPIPE, signal.SIG_DFL))

	# Get stdout and stderr from subprocess
	fastq_multx_stdout, fastq_multx_stderr = fastq_multx_call.communicate()

	# Check if code is running in python 3
	if sys.version_info[0] == 3:
		
		# Convert bytes to string
		fastq_multx_stdout = fastq_multx_stdout.decode()
		fastq_multx_stderr = fastq_multx_stderr.decode()

	# Check the stderr for errors
	checkFastqMultxForErrors(fastq_multx_stderr)