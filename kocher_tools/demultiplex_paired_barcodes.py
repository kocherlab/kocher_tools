#!/usr/bin/env python
import os
import sys
import argparse
import shutil
import logging
import pkg_resources

from kocher_tools.multiplex import Multiplex
from kocher_tools.logger import startLogger, logArgs
from kocher_tools.deML import *
from kocher_tools.fastq_multx import fastqMultx

def deMultiplexParser ():
	'''
	Barcode Pipeline Parser

	Assign the parameters for the barcode pipeline

	Parameters
	----------
	sys.argv : list
		Parameters from command lind

	Raises
	------
	IOError
		If the specified files do not exist
	'''

	def parser_confirm_file ():
		'''Custom action to confirm file exists'''
		class customAction(argparse.Action):
			def __call__(self, parser, args, value, option_string=None):
				if not os.path.isfile(value):
					raise IOError('%s not found' % value)
				setattr(args, self.dest, value)
		return customAction

	# Create the top-level parser
	demultiplex_parser = argparse.ArgumentParser(formatter_class = argparse.ArgumentDefaultsHelpFormatter)

	# Map files
	demultiplex_parser.add_argument('--paired-map', help = 'Defines the filename of a paired index map', type = str, action = parser_confirm_file(), required = True)

	format_types = ['tsv', 'excel']
	format_default = 'excel'
	demultiplex_parser.add_argument('--index-format', help = 'Defines the format of the paired index', type = str, choices = format_types, default = format_default)
	demultiplex_parser.add_argument('--excel-sheet', help = 'Defines the excel sheet to use. Default is the first sheet - i.e. 1', type = int, default = 1)

	# Method options
	method_types = ['fastq-multx', 'deML']
	method_default = 'fastq-multx'
	demultiplex_parser.add_argument('--method', help = 'Defines the demultiplex method. 1) fastq-multx - Fast, less reads; 2) deML Slow, more reads', type = str, choices = method_types, default = method_default)

	# Map options
	i5_revcomp_parser = demultiplex_parser.add_mutually_exclusive_group()
	i5_revcomp_parser.add_argument('--novaseq', dest = 'i5_revcomp', help = 'Reads were sequenced using NovaSeq', action='store_true')
	i5_revcomp_parser.add_argument('--reverse-complement-i5', dest = 'i5_revcomp', help = 'Reverse complement the i5 map (equivalent to --novaseq)', action='store_true')

	# Read Files
	demultiplex_parser.add_argument('--i7', help = 'Defines the filename of the i7 reads (i.e. Read 2 Index)', type = str, action = parser_confirm_file(), required = True)
	demultiplex_parser.add_argument('--i5', help = 'Defines the filename of the i5 reads (i.e. Read 3 Index)', type = str, action = parser_confirm_file(), required = True)
	demultiplex_parser.add_argument('--R1', help = 'Defines the filename of the R1 reads (i.e. Read 1)', type = str, action = parser_confirm_file(), required = True)
	demultiplex_parser.add_argument('--R2', help = 'Defines the filename of the R2 reads (i.e. Read 4)', type = str, action = parser_confirm_file())

	# Optional arguments
	demultiplex_parser.add_argument('--keep-unknown', help = 'Defines if unknown output should be kept', action = 'store_true')
	demultiplex_parser.add_argument('--keep-failed', help = 'Defines if failed output should be kept', action = 'store_true')
	demultiplex_parser.add_argument('--keep-indices', help = 'Defines if indices output should be kept', action = 'store_true')
	demultiplex_parser.add_argument('--keep-all', help = 'Defines if all output should be kept', action = 'store_true')

	# Output arguments
	demultiplex_parser.add_argument('--out-dir', help = 'Defines the output directory', type = str, default = 'Pipeline_Output')
	demultiplex_parser.add_argument('--out-log', help = 'Defines the filename of the log file', type = str, default = 'demultiplex_pipeline.log')
	demultiplex_parser.add_argument('--summary-log', help = 'Defines the filename of the log file', type = str, default = 'demultiplex_summary.tsv')
	demultiplex_parser.add_argument('--overwrite', help = 'Defines if previous output should be overwritten', action = 'store_true')
	
	# Return the arguments
	return demultiplex_parser.parse_args()


def main():

	# Assign the demultiplex arguments
	demultiplex_args = deMultiplexParser()

	# Check for previous output
	if not demultiplex_args.overwrite:

		# Check for a previous log file
		if os.path.isfile(demultiplex_args.out_log): 
			raise Exception(f'Found log found: {demultiplex_args.out_log}. Please rename using --out-log or use --overwrite')

		# Check for a previous summary file
		if os.path.isfile(demultiplex_args.summary_log): 
			raise Exception(f'Found summary found: {demultiplex_args.summary_log}. Please rename using --summary-log or use --overwrite')

		# Check for a previous pipeline output
		if os.path.isdir(demultiplex_args.out_dir):
			raise Exception(f'Found pipeline output found: {demultiplex_args.out_dir}. Please rename using --out-dir or use --overwrite')

	# Remove previous output, if specified
	elif os.path.isdir(demultiplex_args.out_dir): shutil.rmtree(demultiplex_args.out_dir)

	# Start the log
	startLogger(demultiplex_args.out_log)
	logArgs(demultiplex_args)

	# Update the excel sheet from 1-based to 0-based
	if demultiplex_args.excel_sheet < 1: raise Exception(f'Unable to assign excel sheet: {demultiplex_args.excel_sheet}. Please note that --excel-sheet begin at 1 (i.e. 1-based indexing)')
	excel_sheet_zero_based = demultiplex_args.excel_sheet - 1

	# Check if all output should be kept, and if so, assign the arguments
	if demultiplex_args.keep_all:
		if demultiplex_args.keep_unknown or demultiplex_args.keep_failed or demultiplex_args.keep_indices:
			raise Exception('--keep-all cannot be combined with the other --keep-{} arguments')
		
		# Assign arguments 
		demultiplex_args.keep_unknown = demultiplex_args.keep_failed = demultiplex_args.keep_indices = True

	# Demultiplex using fastq-multx
	if demultiplex_args.method == 'fastq-multx':

		# Create the demultiplex job using the index
		demultiplex_job = fastqMultx.withIndex(demultiplex_args.paired_map, 
										 demultiplex_args.index_format,
										 pipeline_log_filename = demultiplex_args.out_log,
										 fastq_multx_summary_filename = demultiplex_args.summary_log,
										 excel_sheet = excel_sheet_zero_based,
										 keep_unknown = demultiplex_args.keep_unknown,
										 keep_indices = demultiplex_args.keep_indices,
										 i5_reverse_complement = demultiplex_args.i5_revcomp)
		
		# Demultiplex the following files
		demultiplex_job.demultiplexFASTQs(out_dir = demultiplex_args.out_dir,
										  i7_read_file = demultiplex_args.i7, 
										  i5_read_file = demultiplex_args.i5, 
										  r1_file = demultiplex_args.R1, 
										  r2_file = demultiplex_args.R2)

	# Demultiplex using deML
	elif demultiplex_args.method == 'deML': 

		# Create the demultiplex job using the index
		demultiplex_job = deML.withIndex(demultiplex_args.paired_map, 
										 demultiplex_args.index_format,
										 pipeline_log_filename = demultiplex_args.out_log,
										 deML_summary_filename = demultiplex_args.summary_log,
										 excel_sheet = excel_sheet_zero_based,
										 keep_unknown = demultiplex_args.keep_unknown,
										 keep_failed = demultiplex_args.keep_failed,
										 keep_indices = demultiplex_args.keep_indices,
										 i5_reverse_complement = demultiplex_args.i5_revcomp)
		
		# Demultiplex the following files
		demultiplex_job.demultiplexFASTQs(out_dir = demultiplex_args.out_dir,
										  i7_read_file = demultiplex_args.i7, 
										  i5_read_file = demultiplex_args.i5, 
										  r1_file = demultiplex_args.R1, 
										  r2_file = demultiplex_args.R2)

	else:
		raise Exception(f'Unrecognized method: {demultiplex_args.method}')


if __name__== "__main__":
	main()

