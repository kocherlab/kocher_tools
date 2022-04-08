import os
import re
import sys
import glob
import string
import shutil
import random
import logging
import subprocess

import pandas as pd

from kocher_tools.misc import confirmExecutable

class deML (list):
	def __init__ (self, index = '', index_format = None, i7_reverse_complement = False, i5_reverse_complement = False, pipeline_log_filename = None, 
						deML_summary_filename = None, index_barcode_len = 8, excel_sheet = None, index_header = None, keep_unknown = False, 
						keep_failed = False, keep_indices = False, **kwargs):

		# Check if the deML executable was found
		self._deML_path = confirmExecutable('deML')
		if not self._deML_path: raise IOError('deML not found. Please confirm the executable is installed')
		self._deML_call_args = []

		# Assign the index-based arguments
		self.index = index
		self.index_format = index_format
		self.excel_sheet = excel_sheet
		self.index_header = index_header
		self._index_well_col = 'Name'
		self._index_i7_col = '#Index1'
		self._index_i5_col = 'Index2'
		self._index_barcode_len = index_barcode_len

		# Assign general arguments
		self.i7_reverse_complement = i7_reverse_complement
		self.i5_reverse_complement = i5_reverse_complement
		self._deML_summary_filename = deML_summary_filename
		self._pipeline_log_filename = pipeline_log_filename

		# Assign the output options
		self._keep_unknown = keep_unknown
		self._keep_failed = keep_failed
		self._keep_indices = keep_indices

		# Process the index
		self._processIndex()

	@property
	def _barcode_cols (self):
		return [self._index_i7_col, self._index_i5_col]

	@property
	def _index_cols (self):
		return [self._index_i7_col, self._index_i5_col, self._index_well_col]

	@property
	def deML_arg_list (self):
		return [self._deML_path, '--index', self.index, '--summary', self._deML_summary_filename] + list(map(str, self._deML_call_args))

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
					optional_out_file = os.path.join(optional_out_path, optional_filename.replace('tmp_', ''))
					shutil.move(optional_file, optional_out_file)

		logging.info('Started FASTQ paired-index demultiplex')

		# Create the output prefix
		if not os.path.exists(out_dir): os.makedirs(out_dir)
		out_prefix = os.path.join(out_dir, 'tmp')

		# Assign the FASTQ arguments
		self._deML_call_args.extend(['-o', out_prefix, '-if1', i7_read_file, '-if2', i5_read_file,'-f', r1_file])
		if r2_file: self._deML_call_args.extend(['-r', r2_file])

		# Demultiplex
		self._call()

		'''
		Process the optional output files in one of two ways:
		1) Removal of the optional output (Default)
		2) Store the files within a sub-directory
		''' 
		_processOptionalOutput('*_unknown_*.fq.gz', 'Unknown' if self._keep_unknown else None)
		_processOptionalOutput('*.fail.fq.gz', 'Failed' if self._keep_failed else None)
		_processOptionalOutput('*_i[1-2].fq.gz', 'Indices' if self._keep_indices else None)

		# Rename the R1/2 demultiplexed reads
		for deML_filename in os.listdir(out_dir):
			deML_file = os.path.join(out_dir, deML_filename)
			if not os.path.isfile(deML_file): continue
			os.rename(deML_file, re.sub(r'tmp_(.*)r([1-2].fq.gz)', r'\1R\2', deML_file))

		# Log the contents of the summary file
		self._cleanSummary()
		self._logSummary()

		logging.info('Finished FASTQ paired-index demultiplex')

	def _cleanSummary (self):

		# Read in the orginal contents of the summary file
		with open(self._deML_summary_filename, 'r') as summary_file:
			summary_lines = summary_file.readlines()

		# Keep the orginal contents and remove the dash lines
		with open(self._deML_summary_filename, 'w') as summary_file:
			for summary_line in summary_lines:
				if summary_line.strip() == '-' * 32: continue
				summary_file.write(summary_line)

	def _logSummary (self):

		# Assign the log lines
		start_message = ('#' * 12) + ' Start deML Summary ' + ('#' * 12)
		end_massage = ('#' * 13) + ' End deML Summary ' + ('#' * 13)
		spacer_line = '#' * 44

		# Append the log file
		deML_log = open(self._deML_summary_filename, 'r').read()
		logging.info(f'\n{spacer_line}\n{start_message}\n{deML_log}{end_massage}\n{spacer_line}')

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
		
		# Reorder and rename the columns
		index_dataframe = index_dataframe[self._index_cols]

		# Create the new index file
		self.index = f'{self.index}.deML.formatted'
		index_dataframe.to_csv(self.index , sep = '\t', index = False)

		logging.info(f'Index file ({self.index}) processed and assigned')

	def _call (self):
		'''
			Standard call of deML

			The function calls deML.

			Raises
			------
			Exception
				If deML stderr returns an error
		'''

		# deML subprocess call
		deML_call = subprocess.Popen(self.deML_arg_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

		# Wait for deML to finish
		deML_out, deML_err = deML_call.communicate()

		# If needed, convert bytes to string
		if sys.version_info[0] == 3:
			deML_out = deML_out.decode()
			deML_err = deML_err.decode()

		logging.info('deML call complete')

		# Check that the log file was created correctly
		self._check_for_errors(deML_err)

		# Assign the stderr lines
		start_message = ('#' * 12) + ' Start deML stderr  ' + ('#' * 12)
		end_massage = ('#' * 13) + ' End deML stderr  ' + ('#' * 13)
		spacer_line = '#' * 44

		# Append the log file
		logging.info(f'\n{spacer_line}\n{start_message}\n{deML_err}{end_massage}\n{spacer_line}')

	@staticmethod
	def _check_for_errors (deML_stderr):
		'''
			Checks the plink stderr for errors

			Parameters
			----------
			deML_stderr : str
				plink stderr

			Raises
			------
			IOError
				If plink stderr returns an error
		'''

		def _skipTextBeforeStr (stderr_str):
			
			return_text = []
			print_line = False
			for deML_stderr_line in deML_stderr.splitlines():
				if stderr_str in deML_stderr_line: print_line = True
				if not print_line: continue
				return_text.append(deML_stderr_line)
			return return_text


		# Print errors, if found
		if 'ERROR' in deML_stderr: raise Exception(deML_stderr)

		# Report file limit warning
		elif 'WARNING:' in deML_stderr and 'ulimit' in deML_stderr:
			raise Exception('\n'.join(_skipTextBeforeStr('WARNING:')))
		
		# Report any pair conflicts
		else:
			pair_conflicts = _skipTextBeforeStr('Conflicts for pairs:')
			if len(pair_conflicts) > 1: raise Exception(pair_conflicts)
