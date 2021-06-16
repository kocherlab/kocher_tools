import os
import sys
import logging
import subprocess

import pandas as pd

from kocher_tools.model import readModelFile
from kocher_tools.misc import confirmExecutable

class Plink2 (list):
	def __init__ (self, vcf_filename = '', bed_prefix = '', sample_model_dict = {}, out_prefix = 'out', out_dir = '', **kwargs):

		# Check if the plink2 executable was found
		self.plink2_path = confirmExecutable('plink2')
		if not self.plink2_path: raise IOError('plink2 not found. Please confirm the executable is installed')
		self._plink2_call_args = []

		self.vcf_filename = vcf_filename
		self.bed_prefix = bed_prefix
		self.out_prefix = os.path.join(out_dir, out_prefix)
		self.out_dir = out_dir
		self.sample_model_dict = sample_model_dict

		# Check if the Binary-Ped files exists
		if self.bed_prefix and (os.path.isfile(f'{self.bed_prefix}.bed') and os.path.isfile(f'{self.bed_prefix}.bim') and os.path.isfile(f'{self.bed_prefix}.fam')): self.bed_exists = True
		else: self.bed_exists = False
			
		# Check if the VCF file exists
		if self.vcf_filename and os.path.isfile(self.vcf_filename): self.vcf_exists = True
		else: self.vcf_exists = False

		# Prepare the files, if needed
		self._prepFiles()

	@property
	def plink2_arg_list (self):
		return [self.plink2_path] + list(map(str, self._plink2_call_args))
	
	def _prepFiles(self):

		# Return error if no files were assigned
		if not self.vcf_exists and not self.bed_exists:
			raise IOError(f'Cannot find VCF ({self.vcf_filename}) or Binary-PED ({self.bed_prefix}.*). PLINK2 requires either a VCF or Binary-PED file to operate')

		# Create the Binary-PED, if needed
		if self.vcf_exists and not self.bed_exists: self._cvtToPlink()

		# Assign the input/output arg
		if self.bed_prefix: self._plink2_call_args.extend(['--bfile', self.bed_prefix])
		else: raise IOError('No Binary-PED prefix assigned')
		self._plink2_call_args.append('--allow-extra-chr')
		self._plink2_call_args.extend(['--out', self.out_prefix])

		# Create the output directory, if needed
		if self.out_dir and not os.path.exists(self.out_dir):
			os.makedirs(self.out_dir)

	def _cvtToPlink (self):

		# Assign the bed-prefix
		if self.vcf_filename.endswith('vcf.gz'): self.bed_prefix = self.vcf_filename[:-7]
		elif self.vcf_filename.endswith('vcf'): self.bed_prefix = self.vcf_filename[:-4]
		else: raise Exception(f'VCF has non-standard file extension. Please use .vcf or .vcf.gz')

		# Assign the conversion args
		self._plink2_call_args.extend(['--vcf', self.vcf_filename, '--make-bed', '--out', self.bed_prefix, '--double-id', '--allow-extra-chr'])

		# Call plink2
		self._call()

		# Cleanup and rename the log
		self._plink2_call_args = []
		os.rename(f'{self.bed_prefix}.log', f'{self.bed_prefix}.cvt.log')
	
	@classmethod
	def usingModelFile (cls, model_file, model, **kwargs):
		if not os.path.isfile(model_file): raise IOError('Unable to find Model file')
		models = readModelFile(model_file)
		if model not in models: raise IOError('Unable to assign Model')
		ind_dict = {_ind:_m for _m, _inds in models[model].ind_dict.items() for _ind in _inds}
		return cls(sample_model_dict = ind_dict, **kwargs)

	def filter (self, include_bed = None, exclude_bed = None, bed1 = False, include_chr = [], exclude_chr = [], from_bp = None, to_bp = None, **kwargs):

		# Check for incompatible filters
		if (include_bed or exclude_bed) and (include_chr or exclude_chr or from_bp or to_bp):
			raise Exception('File-based filters (i.e. --include-bed/--exclude-bed) are incompatible with position-based filter (i.e. --include-chr/--exclude-chr and/or --from-bp/--to-bp)')

		# Assign BED filetype and file, if needed
		if include_bed or exclude_bed:
			bed_type = 'bed0' if not bed1 else 'bed1'
			if include_bed: self._plink2_call_args.extend(['--extract', bed_type, include_bed])
			if exclude_bed: self._plink2_call_args.extend(['--exclude', bed_type, exclude_bed])
			 
		# Assign the Chromosome(s)
		if include_chr or exclude_chr:
			if include_chr: self._plink2_call_args.extend(['--chr'] + include_chr)
			if exclude_chr: self._plink2_call_args.extend(['--not-chr'] + exclude_chr)

		# Assign the bp range
		if from_bp or to_bp:
			if not include_chr or len(include_chr) > 1: raise Exception('--from-bp/--to-bp must be used with --include-chr, and with only one chromosome.')
			if from_bp: self._plink2_call_args.extend(['--from-bp', from_bp])
			if to_bp: self._plink2_call_args.extend(['--to-bp', to_bp])
		
	def calcFst (self, method = 'hudson', category = 'Pops', report_variants = True):

		# Check the fam file was found
		if not os.path.isfile(f'{self.bed_prefix}.fam'): raise IOError(f'Unable to locate {self.bed_prefix}.fam')

		# Create the pheno dataframe
		try: id_df = pd.read_csv(f'{self.bed_prefix}.fam', sep = ' ', header = None, usecols = [0, 1], names = ['#FID', 'IID'])
		except: id_df = pd.read_csv(f'{self.bed_prefix}.fam', sep = '\t', header = None, usecols = [0, 1], names = ['#FID', 'IID'])

		# Assign and confirm pops
		id_df[category] = id_df['IID'].map(self.sample_model_dict)
		id_df = id_df[id_df[category].notna()]
		if id_df[category].isnull().values.any(): raise Exception('Population assignment error')

		# Create the pheno file
		pheno_filename = f'{self.bed_prefix}.pheno'
		id_df.to_csv(pheno_filename, sep = '\t', index = False)

		# Assign the Fst arguments
		self._plink2_call_args.extend(['--fst', category])
		if report_variants: self._plink2_call_args.append('report-variants')
		self._plink2_call_args.append(f'method={method}')
		self._plink2_call_args.extend(['--pheno', pheno_filename])

		# Call plink2
		self._call()

		# Cleanup and rename the log
		os.remove(pheno_filename)
		os.rename(f'{self.out_prefix}.log', f'{self.out_prefix}.fst.log')

	def _call (self):
		'''
			Standard call of plink2

			The function calls plink2. Returns the stderr of plink to create a log
			file of the call.

			Parameters
			----------
			plink2_call_args : list
				plink2 arguments

			Raises
			------
			Exception
				If plink2 stderr returns an error
		'''

		# plink subprocess call
		plink2_call = subprocess.Popen(self.plink2_arg_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

		# Wait for plink to finish
		plink2_out, plink2_err = plink2_call.communicate()

		# If needed, convert bytes to string
		if sys.version_info[0] == 3:
			plink2_out = plink2_out.decode()
			plink2_err = plink2_err.decode()

		#logging.info('plink2 call complete')

		# Check that the log file was created correctly
		self._check_for_errors(plink2_err)

	@staticmethod
	def _check_for_errors (plink_stderr):
		'''
			Checks the plink stderr for errors

			Parameters
			----------
			plink_stderr : str
				plink stderr

			Raises
			------
			IOError
				If plink stderr returns an error
		'''

		# Print warning, if found
		if 'Warning' in plink_stderr:
			logging.warning(plink_stderr.replace('\n',' '))

		# Print output if error found. Build up as errors are discovered
		elif plink_stderr:
			raise Exception(plink_stderr)