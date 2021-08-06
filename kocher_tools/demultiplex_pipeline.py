#!/usr/bin/env python
import os
import sys
import argparse
import shutil
import logging
import pkg_resources

from kocher_tools.multiplex import Multiplex
from kocher_tools.logger import startLogger, logArgs

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

	demultiplex_parser = argparse.ArgumentParser(formatter_class = argparse.ArgumentDefaultsHelpFormatter)

	# Map files
	demultiplex_parser.add_argument('--i5-map', help = 'Defines the filename of the i5 map', type = str, action = parser_confirm_file(), required = True)
	demultiplex_parser.add_argument('--i7-map', help = 'Defines the filename of the i7 map (if not the default map)', type = str, action = parser_confirm_file())

	# Map options
	i5_revcomp_parser = demultiplex_parser.add_mutually_exclusive_group()
	i5_revcomp_parser.add_argument('--novaseq', dest = 'i5_revcomp', help = 'Reads were sequenced using NovaSeq', action='store_true')
	i5_revcomp_parser.add_argument('--reverse-complement-i5map', dest = 'i5_revcomp', help = 'Reverse complement the i5 map (equivalent to --novaseq)', action='store_true')

	# Read Files
	demultiplex_parser.add_argument('--i5-read-file', help = 'Defines the filename of the i5 reads (i.e. Read 3 Index)', type = str, action = parser_confirm_file(), required = True)
	demultiplex_parser.add_argument('--i7-read-file', help = 'Defines the filename of the i7 reads (i.e. Read 2 Index)', type = str, action = parser_confirm_file(), required = True)
	demultiplex_parser.add_argument('--R1-read-file', help = 'Defines the filename of the R1 reads (i.e. Read 1)', type = str, action = parser_confirm_file(), required = True)
	demultiplex_parser.add_argument('--R2-read-file', help = 'Defines the filename of the R2 reads (i.e. Read 4)', type = str, action = parser_confirm_file())

	# Output arguments
	demultiplex_parser.add_argument('--out-dir', help = 'Defines the output directory', type = str, default = 'Pipeline_Output')
	demultiplex_parser.add_argument('--out-log', help = 'Defines the filename of the log file', type = str, default = 'barcode_pipeline.log')
	demultiplex_parser.add_argument('--overwrite', help = 'Defines if previous output should be overwritten', action = 'store_true')

	# Return the arguments
	return demultiplex_parser.parse_args()

def main():

	# Assign the demultiplex args
	demultiplex_args = deMultiplexParser()
	
	# Assign the i7 map path from the package
	if not demultiplex_args.i7_map:
		i7_map_path = pkg_resources.resource_filename('kocher_tools', 'data/i7_map.txt')
		if not os.path.exists(i7_map_path): raise IOError('Cannot assign i7 map from package')
		demultiplex_args.i7_map = i7_map_path

	# Create the log file and log the args
	startLogger(demultiplex_args.out_log)
	logArgs(demultiplex_args)

	# Check for previous output
	if os.path.exists(demultiplex_args.out_dir):
		if demultiplex_args.overwrite: shutil.rmtree(demultiplex_args.out_dir) 
		else: raise Exception(f'{demultiplex_args.out_dir} already exists. Please alter --out-dir or use --overwrite')

	# Start a demultiplex job with the files
	demultiplex_job = Multiplex.fromFiles(i5_read_file = demultiplex_args.i5_read_file, 
										  i7_read_file = demultiplex_args.i7_read_file, 
										  r1_file = demultiplex_args.R1_read_file, 
										  r2_file = demultiplex_args.R2_read_file)

	# Assign the output path for all demultiplexed files
	demultiplex_job.assignOutputPath(demultiplex_args.out_dir)

	# Assign the plate using the i5 map
	demultiplex_job.assignPlates(demultiplex_args.i5_map)

	# Run the i5 barcode job using the i5 map
	demultiplex_job.deMultiplex(demultiplex_args.i5_map, reverse_complement_barcodes = demultiplex_args.i5_revcomp)

	logging.info('Finished i5 deMultiplex')

	# Remove unmatched files
	demultiplex_job.removeUnmatched()

	# Loop the plates in the multiplex job
	for plate in demultiplex_job:

		# Assign the well of the current plate
		plate.assignWells()

		logging.info(f'Starting {plate.name} i7 deMultiplex')

		# Run the i7 barcode job using the i7 map
		plate.deMultiplexPlate(demultiplex_args.i7_map, plate_prefix = True)

		logging.info(f'Finished {plate.name} i7 deMultiplex')

		# Remove unnecessary files
		plate.removeUnmatchedPlate()
		plate.removePlate()



	'''
	# Create the multiplex job
	demultiplex_job = Multiplex()

	# Assign the output path for all demultiplexed files
	demultiplex_job.assignOutputPath(demultiplex_args.out_dir)

	# Assign the read file for the multiplex job
	demultiplex_job.assignFiles(demultiplex_args.i5_read_file, demultiplex_args.i7_read_file, demultiplex_args.R1_read_file, demultiplex_args.R2_read_file)

	# Assign the plate using the i5 map
	demultiplex_job.assignPlates(demultiplex_args.i5_map)

	logging.info('Starting i5 deMultiplex')

	# Run the i5 barcode job using the i5 map
	demultiplex_job.deMultiplex(demultiplex_args.i5_map)

	logging.info('Finished i5 deMultiplex')

	# Move the plates into directories of their plate and locus names 
	demultiplex_job.movePlates()

	# Remove unmatched files (this should be an option in beta)
	demultiplex_job.removeUnmatched()

	# Loop the plates in the multiplex job
	for plate in demultiplex_job:

		# Assign the well of the current plate
		plate.assignWells()

		logging.info(f'Starting {plate.name} i7 deMultiplex')

		# Run the i7 barcode job using the i7 map
		plate.deMultiplexPlate(demultiplex_args.i7_map)

		logging.info(f'Finished {plate.name} i7 deMultiplex')

		# Move the wells into the Wells directory. Should this be user specified?
		plate.moveWells()

		# Remove any unmatched files for the current plate
		plate.removeUnmatchedPlate()
	'''

if __name__== "__main__":
	main()