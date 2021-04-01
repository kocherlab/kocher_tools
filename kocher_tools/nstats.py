import os
import sys
import gzip
import argparse

from Bio import SeqIO
from collections import defaultdict

def nStatArgs ():

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

	nstat_parser = argparse.ArgumentParser(formatter_class = argparse.ArgumentDefaultsHelpFormatter)

	# Assign I/O arguments
	nstat_parser.add_argument('--genome-fasta', help = "Defines the genome filename (in fasta format)", type = str, action = confirmFile(), required = True)
	nstat_parser.add_argument('--output', help = "Defines the output n-statistics filename", type = str, required = True)

	# Return arguments
	return nstat_parser.parse_args()

def fastaStats (fasta_filename):

	# Read in the fasta file
	if fasta_filename.endswith('.gz'): fasta_file = gzip.open(fasta_filename, 'rt')
	else: fasta_file = open(fasta_filename, 'r')
	
	# Record the length for each sequence - i.e. chromosome, scaffold
	seq_lens = []
	n_count = 0
	for seq_record in SeqIO.parse(fasta_file, 'fasta'):
		seq_lens.append(len(seq_record))
		n_count += seq_record.seq.count('N')

	# Sort and return
	seq_lens = sorted(seq_lens, reverse = True)
	return seq_lens, n_count

def nStats (len_list, n_floats = [0.1, 0.5, 0.9], n_float = None):

	# Check if a  n_float was given
	if n_float: n_floats = [n_float]

	# Check the floats
	for _nf in n_floats: 
		if not isinstance(_nf, float): raise Exception(f'N calc must be float: {_nf}')

	# Obtain the total genome length and %n length
	genome_length = sum(len_list)
	n_cutoff_dict = {(genome_length * _nf):f'N{int((_nf) * 100)}'for _nf in n_floats}
	n_min_cutoff = min(list(n_cutoff_dict))
	
	# Calc the n stat
	n_stats = ''
	n_sum = 0
	n_count = 0
	for len_int in len_list:
		n_sum += len_int
		n_count += 1
		if n_sum >= n_min_cutoff: 
			n_stats += f'{n_cutoff_dict[n_min_cutoff]} = {len_int}, n = {n_count}\n'
			del n_cutoff_dict[n_min_cutoff]
			if not n_cutoff_dict: break
			else: n_min_cutoff = min(list(n_cutoff_dict))
	
	# Return the results
	return n_stats

# Assign the arguments using argparse
nStat_args = nStatArgs()

# Calc the sequence stats
scaffold_lens, n_count = fastaStats(nStat_args.genome_fasta)

# Create the output stat file
n_stat_output = open(nStat_args.output, 'w')
n_stat_output.write(f'stats for {nStat_args.genome_fasta}\n')
n_stat_output.write(f'sum = {sum(scaffold_lens)}, n = {len(scaffold_lens)}, ave = {sum(scaffold_lens)/len(scaffold_lens)}, largest = {max(scaffold_lens)}\n')
n_stat_output.write(nStats(scaffold_lens))
n_stat_output.write(f'N_count = {n_count}\n')
n_stat_output.close()