#!/usr/bin/env python
import os
import argparse
import pandas as pd
from collections import defaultdict
from kocher_tools.gff import *

def gffArgs ():

	'''
	Argument parser

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

	gff_parser = argparse.ArgumentParser(formatter_class = argparse.ArgumentDefaultsHelpFormatter)

	# Assign I/O arguments
	gff_parser.add_argument('--gff-file', help = "Defines the GFF filename", type = str, action = confirmFile(), required = True)
	gff_parser.add_argument('--chrom-size-file', help = "Defines the chromosome size filename (as tsv)", type = str, action = confirmFile(), required = True)
	gff_parser.add_argument('--out-file', help = "Defines the output filename (as gff3). Defaults to out.gff3", type = str, default = 'out.gff3')

	# Assign Add/Skip arguments
	gff_parser.add_argument('--add-utrs', help = 'Defines the option to add UTRs', action = 'store_true')
	gff_parser.add_argument('--add-introns', help = 'Defines the option to add introns', action = 'store_true')
	gff_parser.add_argument('--label-first-intron', help = 'Defines if the first intron should be labeled as first_intron', action = 'store_true')
	gff_parser.add_argument('--add-tss-elements', help = 'Defines the option to add TSS elements', action = 'store_true')
	gff_parser.add_argument('--tss-upstream-bp', help = "Defines the TSS upstream length", type = int, default = 200)
	gff_parser.add_argument('--tss-flanks-bp', help = "Defines the TSS upstream/downstream flank length", type = int, default = 50)

	# Assign general arguments
	gff_parser.add_argument('--overwrite', help = 'Defines if the features being added should overwrite previous entries', action = 'store_true')

	# Return arguments
	return gff_parser.parse_args()

# Assign the arguments using argparse
gff_args = gffArgs()

# Check that at least one feature is being added
if not gff_args.add_introns and not gff_args.add_utrs and not gff_args.add_tss_elements:
	raise Exception('No features selected to add. Please use --add-introns, --add-utrs, and/or --add-tss-elements')

# Create the database
gff_db = createDatabase(gff_args.gff_file)

# Check that the features being added are not within the database
if not gff_args.overwrite:
	features_in_db = gff_db.featuretypes()
	if gff_args.add_introns and ('tss_upstream' in features_in_db or 'tss_flanks' in features_in_db): raise Exception('TSS elements already in database')
	if gff_args.add_introns and ('intron' in features_in_db): raise Exception('Introns already in database')
	if gff_args.add_utrs and ('five_prime_UTR' in features_in_db or 'three_prime_UTR' in features_in_db): raise Exception('UTRs already in database')

# Read in the chromosome sizes file and confirm the sizes are ints
chrom_sizes = pd.read_csv(gff_args.chrom_size_file, sep = '\t', header = None, usecols=[0, 1], dtype = {0: str, 1: int})
	
try:
	'''
	Create the chrom size and new feature dicts, once created
	loop the gff by genes
	'''
	new_feature_dict = {'TSS':{}, 'intron':{}, '5prime_UTR':{}, '3prime_UTR':{}}
	chrom_size_dict = {_c:_s for _c, _s in chrom_sizes.values}
	for gene in gff_db.features_of_type(['gene', 'rRNA', 'tRNA']):
		if gff_args.add_tss_elements:
			new_feature_dict['TSS'].update(createTSSElements(gff_db, gene, chrom_size_dict[gene.seqid], gff_args.tss_upstream_bp, gff_args.tss_flanks_bp))
		if gff_args.add_introns: 
			new_feature_dict['intron'].update(createIntrons(gff_db, gene, gff_args.label_first_intron))
		if gff_args.add_utrs:
			_5utr, _3utr = createUTRs(gff_db, gene)
			new_feature_dict['5prime_UTR'].update(_5utr)
			new_feature_dict['3prime_UTR'].update(_3utr)

	# Create a new GFF file with the added elements
	outputGff(gff_args.gff_file, gff_args.out_file, new_feature_dict, **vars(gff_args))

# Report any exceptions, if none delete the database
except: raise
else: removeDatabase(gff_args.gff_file)
