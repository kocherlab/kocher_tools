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
						index_barcode_len = 8, excel_sheet = None, index_header = None, keep_unknown = False, keep_failed = False, keep_indices = False, **kwargs):

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
		self._deML_log_filename = f'tmp.deML.{self.randStr()}.log'
		self._pipeline_log_filename = pipeline_log_filename

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
		return [self._deML_path, '--index', self.index, '--summary', self._deML_log_filename ] + list(map(str, self._deML_call_args))

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
				else: shutil.copy(optional_file, optional_out_path)

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
		_processOptionalOutput('*.fail.fq.gz', 'Failed' if self._keep_unknown else None)
		_processOptionalOutput('*_i[1-2].fq.gz', 'Indices' if self._keep_unknown else None)

		# Rename the R1/2 demultiplexed reads
		for deML_filename in os.listdir(out_dir):
			deML_file = os.path.join(out_dir, deML_filename)
			if not os.path.isfile(deML_file): continue
			os.rename(deML_file, re.sub(r'tmp_(.*)r([1-2].fq.gz)', r'\1R\2', deML_file))

		# Append and remove the deML log
		self._appendLog()

		logging.info('Finished FASTQ paired-index demultiplex')

	def _appendLog (self):

		# Assign the log lines
		start_message = ('#' * 12) + ' Start deML Summary ' + ('#' * 12)
		end_massage = ('#' * 13) + ' End deML Summary ' + ('#' * 13)
		spacer_line = '#' * 44

		# Append the log file
		deML_log = open(self._deML_log_filename, 'r').read()
		logging.info(f'\n{spacer_line}\n{start_message}\n{deML_log}{end_massage}\n{spacer_line}\n')

		# Remove the old deML log
		os.remove(self._deML_log_filename)

	def _processIndex (self):

		def _updateExcelHeader (excel_dataframe):

			well_col = None
			i7_col = None
			i5_col = None

			for index_col in excel_dataframe.columns:
				if 'well' in str(index_col).lower():
					if well_col: raise Exception(f'Well column assignment error: {self.index_format}')
					well_col = index_col
				elif 'i7' in str(index_col).lower():
					if i7_col: raise Exception(f'i7 column assignment error: {self.index_format}')
					i7_col = index_col
				elif 'i5' in str(index_col).lower():
					if i5_col: raise Exception(f'i5 column assignment error: {self.index_format}')
					i5_col = index_col

			if not well_col or not i5_col or not i5_col: raise Exception(f'Unable to update excel header')

			return excel_dataframe.rename({i7_col: '#Index1', i5_col: 'Index2', well_col: 'Name'}, axis = 'columns')

		def _hasWhitespace ():
			if index_dataframe[self._index_i7_col].str.contains('\s+').any(): return True
			elif index_dataframe[self._index_i5_col].str.contains('\s+').any(): return True
			else: return False

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

		# Remove any whitespace in barcodes, if needed
		barcode_whitespace = _hasWhitespace()
		if barcode_whitespace: index_dataframe[self._index_i5_col] = index_dataframe[self._index_i5_col].str.replace(' ', '')
		if barcode_whitespace: index_dataframe[self._index_i7_col] = index_dataframe[self._index_i7_col].str.replace(' ', '')

		# Check the length of the barcodes - whitespace must be removed prior
		if not (index_dataframe[self._index_i7_col].str.len() == self._index_barcode_len).all():
			raise Exception(f'i7 barcodes not {self._index_barcode_len} bases long')
		if not (index_dataframe[self._index_i5_col].str.len() == self._index_barcode_len).all():
			raise Exception(f'i5 barcodes not {self._index_barcode_len} bases long')

		# Reverse complement the bacodes, if needed
		if self.i7_reverse_complement: index_dataframe[self._index_i7_col] = index_dataframe[self._index_i7_col].apply(_revCompBarcodes)
		if self.i5_reverse_complement: index_dataframe[self._index_i5_col] = index_dataframe[self._index_i5_col].apply(_revCompBarcodes)

		# Check if the file needs to be formatted
		if not empty_rows and not self.i7_reverse_complement and not self.i5_reverse_complement and not barcode_whitespace and number_of_columns == len(self._index_cols): return
		
		# Reorder and rename the columns
		index_dataframe = index_dataframe[self._index_cols]

		# Create the new index file
		self.index = f'{self.index}.deML.formatted'
		index_dataframe.to_csv(self.index , sep = '\t', index = False)

		logging.info(f'Index file ({self.index}) processed and assigned.')

	def _call (self):
		'''
			Standard call of deML

			The function calls deML.

			Raises
			------
			Exception
				If deML stderr returns an error
		'''

		# plink subprocess call
		deML_call = subprocess.Popen(self.deML_arg_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

		# Wait for plink to finish
		deML_out, deML_err = deML_call.communicate()

		# If needed, convert bytes to string
		if sys.version_info[0] == 3:
			deML_out = deML_out.decode()
			deML_err = deML_err.decode()

		logging.info('deML call complete')

		# Check that the log file was created correctly
		self._check_for_errors(deML_err)

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

		# Print errors, if found
		if 'ERROR' in deML_stderr: raise Exception(deML_stderr)

	@staticmethod
	def randStr (str_len = 6):
		return ''.join(random.choice(string.ascii_uppercase + string.digits) for i in range(str_len))