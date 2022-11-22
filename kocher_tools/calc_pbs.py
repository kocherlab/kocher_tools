#!/usr/bin/env python
import os
import sys
import copy
import shutil
import argparse

import pandas as pd
import numpy as np

class PBS (list):
	def __init__ (self, pop_list = [], fst_dataframe = pd.DataFrame(), branch_length_dataframe = pd.DataFrame(), out_dir = '', **kwargs):
		self.out_dir = out_dir
		self.pop_list = pop_list
		self.fst_dataframe = fst_dataframe
		self.branch_length_dataframe = branch_length_dataframe
		self.pbs_dataframe = None

		if not fst_dataframe.empty and branch_length_dataframe.empty: self._convertFstToBl()

		# Create the output directory, if needed
		if self.out_dir and not os.path.exists(self.out_dir):
			os.makedirs(self.out_dir)

	@classmethod
	def assignDataFromDir (cls, fst_dir, **kwargs):

		# Create lists to store: the dataframes, the pop names, the fst headers
		merged_dataframe = pd.DataFrame()
		pop_list = []

		# Assign the Fst files, rename the fst column, and add them to the list
		for fst_filename in os.listdir(fst_dir):

			# Skip non-fst files
			if not fst_filename.endswith('.fst.var') and not fst_filename.endswith('weir.fst'): continue

			# Assign the populations
			fst_pop1, fst_pop2 = fst_filename.split('.')[1:3]
			pop_list.extend([fst_pop1, fst_pop2])

			# Open the Fst file, clean up the header
			fst_dataframe = pd.read_csv(os.path.join(fst_dir, fst_filename), sep = '\t')
			fst_dataframe = fst_dataframe.rename(columns = {'#CHROM': 'CHROM'})
			fst_dataframe = fst_dataframe.drop(columns = ['ID', 'OBS_CT'], errors = 'ignore')
			
			# Assign the Fst column, check for possible assignment error
			fst_col = [_col for _col in fst_dataframe.columns if '_FST' in _col]
			if len(fst_col) == 1: fst_col = fst_col[0]
			else: raise Exception('Fst column assignment error')

			# Rename the Fst column
			fst_dataframe = fst_dataframe.rename(columns = {fst_col: f'{fst_pop1}_{fst_pop2}'})

			# Merge the dataframes
			if merged_dataframe.empty: merged_dataframe = fst_dataframe
			else: merged_dataframe = merged_dataframe.merge(fst_dataframe, how = 'left', on = ['CHROM', 'POS'])

		# Assign populations w/ consistent order
		pops_seen = set()
		pops_unique = [pop for pop in pop_list if not (pop in pops_seen or pops_seen.add(pop))]

		# Check that the PBS calc is possible
		if len(pops_unique) != 3: raise Exception(f'3 populations required. Found: {pops_unique}')

		# Clean up the dataframe
		merged_dataframe = merged_dataframe.dropna()
		merged_dataframe[merged_dataframe.columns[2:]] = merged_dataframe[merged_dataframe.columns[2:]].clip(lower=0)

		if merged_dataframe.empty: raise Exception(f'All variants removed due to NAs. Unable to calculate PBS')

		return cls(fst_dataframe = merged_dataframe, pop_list = pops_unique, **kwargs)

	def _convertFstToBl (self):

		# Convert Fst to branch lengths
		pop_columns = self.fst_dataframe.columns[2:]
		self.branch_length_dataframe = copy.deepcopy(self.fst_dataframe)
		self.branch_length_dataframe[pop_columns] = self.branch_length_dataframe[pop_columns].apply(self._fst_to_bl)

		# Clean up
		self.branch_length_dataframe = self.branch_length_dataframe.replace({np.inf:np.nan, -np.inf:np.nan})
		self.branch_length_dataframe = self.branch_length_dataframe.replace({-0: 0})
		self.branch_length_dataframe = self.branch_length_dataframe.dropna()

	@staticmethod	
	def _fst_to_bl (fst):
		np.seterr(divide = 'ignore')
		return -np.log(1 - fst)

	@staticmethod
	def pbs (series, pop1_to_pop2, pop1_to_pop3, pop2_to_pop3):
		return (series[pop1_to_pop2] + series[pop1_to_pop3] - series[pop2_to_pop3]) / 2

	def calculate (self, pbs_value_cutoff = None, top_percent_cutoff = None):

		# Assign the columns
		pos_columns = list(self.branch_length_dataframe.columns[:2])
		pop_columns = list(self.branch_length_dataframe.columns[2:])

		# Create the PBS dataframe, add the annotation column, update the column list
		self.pbs_dataframe = self.branch_length_dataframe[pos_columns].copy()

		# Calc the PBS for each pop
		for pop in self.pop_list:

			# Assign the populations
			pop1_to_pops = [col for col in pop_columns if pop in col]
			pop2_to_pop3 = [col for col in pop_columns if pop not in col]

			# Assign pop1 to pop2/3
			if len(pop1_to_pops) == 2: pop1_to_pop2, pop1_to_pop3 = pop1_to_pops
			else: raise Exception('Assignment Error. POP1 to POP2/POP3 branch lengths')

			# Assign pop2 to pop3
			if len(pop2_to_pop3) == 1: pop2_to_pop3 = pop2_to_pop3[0]
			else: raise Exception('Assignment Error. POP2 to POP3 branch length')

			# Calculate PBS
			self.pbs_dataframe[pop] = self.branch_length_dataframe.apply(self.pbs, pop1_to_pop2 = pop1_to_pop2, pop1_to_pop3 = pop1_to_pop3, pop2_to_pop3 = pop2_to_pop3, axis = 1)
			
			# Apply the top percent cutoff
			if top_percent_cutoff != None:

				# Assign the cutoff value
				top_05_value = round(len(self.pbs_dataframe.index) * top_percent_cutoff)
				if not top_05_value: top_05_value = 1

				# Create a dataframe with the positions that passed the cutoff
				pbs_dataframe_top05 = self.pbs_dataframe.nlargest(top_05_value, pop).copy()
				pbs_dataframe_top05[pos_columns + [pop]].to_csv(os.path.join(self.out_dir, f'{pop}_PBS_percent_cutoff.csv'), index = False)

				# Add the top 0.05% values to the main dataframe
				self.pbs_dataframe[pop + f' (percent cutoff = {top_percent_cutoff})'] = False
				self.pbs_dataframe.loc[pbs_dataframe_top05.index, pop + f' (percent cutoff = {top_percent_cutoff})'] = True

			# Apply the PBS value cutoff
			if pbs_value_cutoff != None:

				# Create a dataframe with the PBS values that passed the cutoff
				pbs_dataframe_value_cutoff = self.pbs_dataframe.loc[self.pbs_dataframe[pop] > pbs_value_cutoff, pos_columns + [pop]]
				pbs_dataframe_value_cutoff.to_csv(os.path.join(self.out_dir, f'{pop}_PBS_value_cutoff.csv'), index = False)

				# Add the top 0.05% values to the main dataframe
				self.pbs_dataframe[pop + f' (value cutoff > {pbs_value_cutoff})'] = False
				self.pbs_dataframe.loc[pbs_dataframe_value_cutoff.index, pop + f' (value cutoff > {pbs_value_cutoff})'] = True

		# Save the whole dataframe
		self.pbs_dataframe.to_csv(os.path.join(self.out_dir, 'PBS.csv'), index = False)

def pbsParser ():
	'''
	Argument parser for PBS calc

	Raises
	------
	IOError
		If the input, or other specified files do not exist
	'''

	def confirmDir ():
		'''Custom action to confirm file exists'''
		class customAction(argparse.Action):
			def __call__(self, parser, args, value, option_string=None):
				if not os.path.isdir(value):
					raise IOError('%s not found' % value)
				setattr(args, self.dest, value)
		return customAction

	def metavarList (var_list):
		'''Create a formmated metavar list for the help output'''
		return '{' + ', '.join(var_list) + '}'

	pbs_parser = argparse.ArgumentParser(formatter_class = argparse.ArgumentDefaultsHelpFormatter)

	# Input arguments
	pbs_parser.add_argument('--fst-dir', help = 'Directory with Fst output files', type = str, action = confirmDir())

	# Value arguments
	pbs_parser.add_argument('--pbs-value-cutoff', help = 'Defines the PBS value cutoff', type = float)
	pbs_parser.add_argument('--top-percent-cutoff', help = 'Defines the highes PBS value percent cutoff', type = float)

	# Output arguments
	pbs_parser.add_argument('--out-dir', help = 'Output directory name', type = str, default = 'PBS_Output')

	# Other arguments
	pbs_parser.add_argument('--overwrite', help = 'Overwrite previous output', action = 'store_true')

	return pbs_parser.parse_args()

# Assign the arguments from the command line
pbs_args = pbsParser()

# Check for previous files, and overwrite if specified
if not pbs_args.overwrite and os.path.isdir(pbs_args.out_dir):
	raise Exception(f'Previous output found at: {pbs_args.out_dir}. Please specify i) a different --out-dir or ii) use --overwrite')
elif pbs_args.overwrite and os.path.isdir(pbs_args.out_dir): 
	shutil.rmtree(pbs_args.out_dir)

# Calculate PBS
pbs_run = PBS.assignDataFromDir(**vars(pbs_args))
pbs_run.calculate(pbs_value_cutoff = pbs_args.pbs_value_cutoff, top_percent_cutoff = pbs_args.top_percent_cutoff)
