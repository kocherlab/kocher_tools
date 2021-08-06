#!/usr/bin/env python
import os
import sys
import argparse
import shutil
import logging
import multiprocessing
import pkg_resources

from kocher_tools.multiplex import Multiplex
from kocher_tools.blast import blastTopHits
from kocher_tools.logger import startLogger, logArgs

def barcodePipelineParser ():
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

	pipeline_parser = argparse.ArgumentParser(formatter_class = argparse.ArgumentDefaultsHelpFormatter)

	# Map files
	pipeline_parser.add_argument('--i5-map', help = 'Defines the filename of the i5 map', type = str, action = parser_confirm_file(), required = True)
	pipeline_parser.add_argument('--i7-map', help = 'Defines the filename of the i7 map (if not the default map)', type = str, action = parser_confirm_file())

	# Map options
	i5_revcomp_parser = pipeline_parser.add_mutually_exclusive_group()
	i5_revcomp_parser.add_argument('--novaseq', dest = 'i5_revcomp', help = 'Reads were sequenced using NovaSeq', action='store_true')
	i5_revcomp_parser.add_argument('--reverse-complement-i5map', dest = 'i5_revcomp', help = 'Reverse complement the i5 map (equivalent to --novaseq)', action='store_true')

	# Read Files
	pipeline_parser.add_argument('--i5-read-file', help = 'Defines the filename of the i5 reads (i.e. Read 3 Index)', type = str, action = parser_confirm_file(), required = True)
	pipeline_parser.add_argument('--i7-read-file', help = 'Defines the filename of the i7 reads (i.e. Read 2 Index)', type = str, action = parser_confirm_file(), required = True)
	pipeline_parser.add_argument('--R1-read-file', help = 'Defines the filename of the R1 reads (i.e. Read 1)', type = str, action = parser_confirm_file(), required = True)
	pipeline_parser.add_argument('--R2-read-file', help = 'Defines the filename of the R2 reads (i.e. Read 4)', type = str, action = parser_confirm_file(), required = True)

	# Output arguments
	pipeline_parser.add_argument('--out-dir', help = 'Defines the output directory', type = str, default = 'Pipeline_Output')
	pipeline_parser.add_argument('--out-compiled', help = 'Defines the filename of the compiled reads (i.e. most abundant)', type = str, default = 'Common.fasta')
	pipeline_parser.add_argument('--out-blast', help = 'Defines the filename of the BLAST output', type = str, default = 'BLAST.out')
	pipeline_parser.add_argument('--out-log', help = 'Defines the filename of the log file', type = str, default = 'barcode_pipeline.log')
	pipeline_parser.add_argument('--overwrite', help = 'Defines if previous output should be overwritten', action = 'store_true')

	# Optional arguments
	pipeline_parser.add_argument('--threads', help = 'Defines the number of threads. Default is all available threads', type = int, default = multiprocessing.cpu_count())
	pipeline_parser.add_argument('--blast-database', help = 'Defines the blast database. Default is the COI database', type = str, default = 'BLAST_DBs/Filtered_BOLD.fasta')

	# Return the arguments
	return pipeline_parser.parse_args()

def main():

	# Assign the barcode args
	barcode_args = barcodePipelineParser()
	
	# Assign the i7 map path from the package
	if not barcode_args.i7_map:
		i7_map_path = pkg_resources.resource_filename('kocher_tools', 'data/i7_map.txt')
		if not os.path.exists(i7_map_path): raise IOError('Cannot assign i7 map from package')
		barcode_args.i7_map = i7_map_path

	# Create the log file
	startLogger(barcode_args.out_log)
	
	# Log the arguments used
	logArgs(barcode_args)

	# Check for previous output
	if os.path.exists(barcode_args.out_dir):

		# Check if previous output should be overwritten
		if barcode_args.overwrite:

			# Remove the previous output
			shutil.rmtree(barcode_args.out_dir) 

		# Check if previous output shouldn't be overwritten
		else: 

			# Raise an exception
			raise Exception('%s already exists. Please alter --out-dir or use --overwrite' % barcode_args.out_dir)

	# Create the multiplex job
	demultiplex_job = Multiplex.fromFiles(i5_read_file = barcode_args.i5_read_file, 
										  i7_read_file = barcode_args.i7_read_file, 
										  r1_file = barcode_args.R1_read_file, 
										  r2_file = barcode_args.R2_read_file)

	# Assign the output path for all multiplex files
	demultiplex_job.assignOutputPath(barcode_args.out_dir)

	# Assign the plate using the i5 map
	demultiplex_job.assignPlates(barcode_args.i5_map)

	logging.info('Starting i5 deMultiplex')

	# Run the i5 barcode job using the i5 map
	demultiplex_job.deMultiplex(barcode_args.i5_map, reverse_complement_barcodes = barcode_args.i5_revcomp)

	logging.info('Finished i5 deMultiplex')

	# Move the plates into directories of their plate and locus names 
	demultiplex_job.movePlates()

	# Remove unmatched files (this should be an option in beta)
	demultiplex_job.removeUnmatched()

	# Loop the plates in the multiplex job
	for plate in demultiplex_job:

		# Assign the well of the current plate
		plate.assignWells()

		logging.info('Starting %s i7 deMultiplex' % plate.name)

		# Run the i7 barcode job using the i7 map
		plate.deMultiplexPlate(barcode_args.i7_map)

		logging.info('Finished %s i7 deMultiplex' % plate.name)

		# Move the wells into the Wells directory. Should this be user specified?
		plate.moveWells()

		# Remove any unmatched files for the current plate
		plate.removeUnmatchedPlate()

		logging.info('Starting %s abundant reads identification' % plate.name)

		# Loop each well
		for well in plate:

			# Merge the R1/R2 files for the current well
			well.mergeWell()

			# Truncate the merged file for the current well
			well.truncateWell()

			# Filter the truncated file for the current well
			well.filterWell()

			# Dereplicate the filtered file for the current well
			well.dereplicateWell()

			# Cluster the dereplicated file for the current well
			well.clusterWell()

			# Identify the most abundant reads
			well.mostAbundantWell()

		logging.info('Finished %s abundant reads identification' % plate.name)

	# Assign the compiled file path
	compiled_file_path = os.path.join(barcode_args.out_dir, barcode_args.out_compiled)

	# Assign the blast output file path
	blast_file_path = os.path.join(barcode_args.out_dir, barcode_args.out_blast)

	# Compile the most abundant reads into a single file
	demultiplex_job.compileMostAbundant(compiled_file_path)

	logging.info('Starting BLAST')

	# Get the top BLAST hits for each sequence in the compiled file
	blastTopHits(compiled_file_path, blast_file_path, barcode_args.blast_database, barcode_args.threads)

	logging.info('Finished BLAST')

if __name__== "__main__":
	main()