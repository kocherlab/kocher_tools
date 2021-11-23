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
	demultiplex_parser.add_argument('--excel-sheet', help = 'Defines the excel sheet to use. Default is the first sheet - i.e. 0', type = int, default = 0)

	# Map options
	demultiplex_parser.add_argument('--miseq', help = 'Reads were sequenced using MiSeq', action='store_true')

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
	demultiplex_parser.add_argument('--out-log', help = 'Defines the filename of the log file', type = str, default = 'barcode_pipeline.log')
	demultiplex_parser.add_argument('--overwrite', help = 'Defines if previous output should be overwritten', action = 'store_true')
	
	# Return the arguments
	return demultiplex_parser.parse_args()


def main():

	# Assign the demultiplex arguments
	demultiplex_args = deMultiplexParser()

	# Start the log
	startLogger(demultiplex_args.out_log)
	logArgs(demultiplex_args)

	# Check if all output should be kept, and if so, assign the arguments
	if demultiplex_args.keep_all:
		if demultiplex_args.keep_unknown or demultiplex_args.keep_failed or demultiplex_args.keep_indices:
			raise Exception('--keep-all cannot be combined with the other --keep-{} arguments')
		
		# Assign arguments 
		demultiplex_args.keep_unknown = demultiplex_args.keep_failed = demultiplex_args.keep_indices = True

	# Create the demultiplex job using the index
	demultiplex_job = deML.withIndex(demultiplex_args.paired_map, 
									 demultiplex_args.index_format,
									 pipeline_log_filename = demultiplex_args.out_log,
									 excel_sheet = demultiplex_args.excel_sheet,
									 keep_unknown = demultiplex_args.keep_unknown,
									 keep_failed = demultiplex_args.keep_failed,
									 keep_indices = demultiplex_args.keep_indices)

	
	
	# Demultiplex the following files
	demultiplex_job.demultiplexFASTQs(out_dir = demultiplex_args.out_dir,
									  i7_read_file = demultiplex_args.i7, 
									  i5_read_file = demultiplex_args.i5, 
									  r1_file = demultiplex_args.R1, 
									  r2_file = demultiplex_args.R2)

if __name__== "__main__":
	main()

