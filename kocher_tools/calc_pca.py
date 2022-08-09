#!/usr/bin/env python
import os
import shutil
import argparse

from kocher_tools.plink2 import Plink2

def pcaParser ():
	'''
	Argument parser for Fst calc

	Raises
	------
	IOError
		If the input, or other specified files do not exist
	'''

	def confirmFile ():
		'''Custom action to confirm file exists'''
		class customAction(argparse.Action):
			def __call__(self, parser, args, value, option_string=None):
				if not os.path.isfile(value):
					raise IOError('%s not found' % value)
				setattr(args, self.dest, value)
		return customAction

	def toList ():
		'''Custom action to add items to a list'''
		class customAction(argparse.Action):
			def __call__(self, parser, args, value, option_string=None):
				value = [item.strip(',') for item in value]
				if not getattr(args, self.dest):
					setattr(args, self.dest, value)
				else:
					getattr(args, self.dest).extend(value)
		return customAction

	def metavarList (var_list):
		'''Create a formmated metavar list for the help output'''
		return '{' + ', '.join(var_list) + '}'

	pca_parser = argparse.ArgumentParser(formatter_class = argparse.ArgumentDefaultsHelpFormatter)

	# Input arguments
	input_method = pca_parser.add_mutually_exclusive_group(required = True)
	input_method.add_argument('--vcf-filename', help = 'Filename of the VCF file', type = str, action = confirmFile())
	input_method.add_argument('--bed-prefix', help = 'Filename of the Binary-PED prefix', type = str)

	# Model input arguments
	pca_parser.add_argument('--model-file', help = 'Filename of the Model file', type = str, action = confirmFile())
	pca_parser.add_argument('--model', help = 'Model to use', type = str)

	# Optional arguments
	pca_parser.add_argument('--allele-freqs', help = 'Filename of the allele frequencies file', type = str, action = confirmFile())

	# Filter arguments
	sample_filter = pca_parser.add_mutually_exclusive_group()
	sample_filter.add_argument('--include-samples', help = 'Samples to include', type = str, nargs = '+', action = toList())
	sample_filter.add_argument('--exclude-samples', help = 'Samples to exclude', type = str, nargs = '+', action = toList())
	bed_filter = pca_parser.add_mutually_exclusive_group()
	bed_filter.add_argument('--include-bed', help = 'BED file of positions to include', type = str, action = confirmFile())
	bed_filter.add_argument('--exclude-bed', help = 'BED file of positions to exclude', type = str, action = confirmFile())
	pca_parser.add_argument('--bed1', help = 'If using BED1 rather than BED0', action = 'store_true')
	chr_filter = pca_parser.add_mutually_exclusive_group()
	chr_filter.add_argument('--include-chr', help = 'Chromosomes to include', type = str, nargs = '+', action = toList())
	chr_filter.add_argument('--exclude-chr', help = 'Chromosomes to exclude', type = str, nargs = '+', action = toList())
	pca_parser.add_argument('--from-bp', help = 'From bp. Only usable with a single chromosomes', type = str)
	pca_parser.add_argument('--to-bp', help = 'To bp. Only usable with a single chromosomes', type = str)

	# PCA arguments
	pca_parser.add_argument('--pc-count', help = 'PC count to report', type = int, default = 10)
	pca_types = ['sample-wts', 'allele-wts', 'biallelic-var-wts']
	pca_parser.add_argument('--pca-type', metavar = metavarList(pca_types), help = 'PCA type to use', type = str, choices = pca_types, default = 'sample-wts')
	pca_modifiers = ['approx', 'meanimpute']
	pca_parser.add_argument('--pca-modifier', metavar = metavarList(pca_modifiers), help = 'PCA modifier to use', type = str, choices = pca_modifiers)

	# Output arguments
	pca_parser.add_argument('--out-prefix', help = 'Output prefix for Fst files', type = str, default = 'out')
	pca_parser.add_argument('--out-dir', help = 'Output directory name', type = str, default = 'PCA_Output')

	# Other arguments
	pca_parser.add_argument('--overwrite', help = 'Overwrite previous output', action = 'store_true')

	return pca_parser.parse_args()

# Assign the arguments
pca_args = pcaParser()

# Check for previous files, and overwrite if specified
if not pca_args.overwrite and os.path.isdir(pca_args.out_dir):
	raise Exception(f'Previous output found at: {pca_args.out_dir}. Please specify i) a different --out-dir or ii) use --overwrite')
elif pca_args.overwrite and os.path.isdir(pca_args.out_dir): 
	shutil.rmtree(pca_args.out_dir)

# Calculate PCA
if pca_args.model_file and pca_args.model: 
	pca_run = Plink2.usingModelFile(**vars(pca_args))
else:
	pca_run = Plink2.standard(**vars(pca_args))
pca_run.filter(**vars(pca_args))
pca_run.calcPCA(pca_type = pca_args.pca_type, pc_count = pca_args.pc_count, pca_modifier = pca_args.pca_modifier, afreq_filename = pca_args.allele_freqs)