#!/usr/bin/env python
import os
import sys
import csv
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
	gff_parser.add_argument('--out-file', help = "Defines the output filename (as tsv). Defaults to out.tsv", type = str, default = 'out.tsv')
	gff_parser.add_argument('--delete-db', help = "Defines if the database should be deleted when finished", action = 'store_true')

	# Assign the bp values for added features
	gff_parser.add_argument('--promoter-bp', help = "Defines the promoter length", type = int, default = 5000)
	gff_parser.add_argument('--upstream-bp', help = "Defines the upstream length", type = int, default = 10000)
	gff_parser.add_argument('--downstream-bp', help = "Defines the downstream length", type = int, default = 10000)

	'''
	Assign features arguments. Some features appear multiple times due to alternative 
	feature names - i.e. ['five_prime_UTR', 'five_utr']. This will not cause an error
	as features not found within the file will be ignored.
	'''
	feature_set = ['tss_upstream', 'tss_flanks', 'five_prime_UTR', 'five_utr', 'exon', 'CDS', 'first_intron', 'intron', 'three_prime_UTR', 'three_utr', 'intergenic']
	gff_parser.add_argument('--feature-set', help = "Defines the feature set. Feature not it list will be ignored", type = str, nargs = '+', default = feature_set)
	priority_list = ['tss_upstream', 'tss_flanks', 'promoter', 'upstream', 'downstream', 'five_prime_UTR', 'five_utr', 'exon', 'CDS',  'first_intron', 'intron', 'three_prime_UTR', 'three_utr', 'intergenic']
	gff_parser.add_argument('--priority-order', help = "Defines the priority of features", type = str, nargs = '+', default = priority_list)

	# Return arguments
	return gff_parser.parse_args()

# Assign the arguments using argparse
gff_args = gffArgs()

# Assign a set with all the possible features
feature_set = gff_args.feature_set + ['upstream', 'promoter', 'downstream']

# Check if features in the priority are not in the feature set
unique_priority_features = list(set(gff_args.priority_order) - set(feature_set))
if unique_priority_features: raise Exception(f'Feature(s) found in --priority-order not in --feature-set: %s' % ', '.join(unique_priority_features))

# Create the database
gff_db = createDatabase(gff_args.gff_file)

'''
Read in the chromosome sizes file and confirm the sizes are ints
then read in the chromosome positions file and group the positions
'''
chrom_sizes = pd.read_csv(gff_args.chrom_size_file, sep = '\t', header = None, usecols=[0, 1], dtype = {0: str, 1: int})

try:
	# Create defaultdict to store the feature counts
	feature_counts_no_priority = defaultdict(int)
	feature_counts_w_priority = defaultdict(int)

	# Loop the GFF database by each chromosome, use the size to know when to stop iter
	for chrom_name, chrom_size in chrom_sizes.values:
		chrom_feature_counts_w_priority = defaultdict(int)
		chrom_feature_counts_w_priority = chromCounts(gff_db, chrom_name, chrom_size, chrom_feature_counts_w_priority, prioritize = True, **vars(gff_args))
		if sum(chrom_feature_counts_w_priority.values()) != chrom_size: raise Exception(f'Unable to parse, likely contains additional GFF entries: {chrom_name}')
		for _f, _c in chrom_feature_counts_w_priority.items(): feature_counts_w_priority[_f] += _c
		feature_counts_no_priority = chromCounts(gff_db, chrom_name, chrom_size, feature_counts_no_priority, **vars(gff_args))

	# Check if features in the priority list were not found in the GFF
	missing_features = list(set(gff_args.priority_order) - set(list(feature_counts_no_priority)))
	if missing_features: 
		missing_features_str = 'Feature(s) not in gff: %s' % ', '.join(missing_features)
		print(missing_features_str)

	# Save the counts into a dataframe, write the dataframe to a tsv
	output_file = open(gff_args.out_file, 'w')
	output_file.write('#%s\n' % ' '.join(sys.argv))
	if missing_features: output_file.write('#%s\n' % missing_features_str)
	output_dataframe = pd.DataFrame.from_dict({'Total Feature Counts': feature_counts_no_priority, 'Feature Counts w/ Priority': feature_counts_w_priority}, orient='index')
	output_dataframe = output_dataframe.fillna(0)
	output_dataframe = output_dataframe[[_pc for _pc in gff_args.priority_order if _pc in output_dataframe.columns]]
	output_dataframe = output_dataframe.astype(int)
	output_dataframe.to_csv(output_file, sep = '\t')
	output_file.close()

	# Delete the database, if specified
	if gff_args.delete_db: os.remove(f'{gff_args.gff_file}.db')

except:	raise