#!/usr/bin/env python
import os
import re
import sys
import json
import argparse
import itertools

import numpy as np
import pandas as pd

from Bio.Seq import Seq
from Bio import AlignIO, SeqIO
from Bio.SeqRecord import SeqRecord
from pysam import VariantFile
from collections import defaultdict
from scipy.stats import fisher_exact

from kocher_tools.alignment import *
from kocher_tools.mafft import alignMafft

class MKOrtholog (dict):
	def __init__(self, name = '', species_pair = [], *arg, **kw):
		super(MKOrtholog, self).__init__(*arg, **kw)
		self.name = name
		self.species_pair = species_pair

		self._out_dir = 'MK_TestData'
		self._MSF_filename = ''
		self._MSA_AA_filename = ''
		self._MSA_Codon_filename = ''

	@classmethod
	def fromJSON (cls, json_data, species_pair):
		mk_ortholog = cls()
		mk_ortholog.name = json_data['name']
		mk_ortholog.species_pair = species_pair
		mk_ortholog.update({_sp['name']:MKTranscript.fromJSON(_sp) for _sp in json_data['orthologs']})
		return mk_ortholog

	def loadSpeciesVCF (self, species, vcf_file):

		if species not in self.species_pair: return

		for transcript in self.values():
			if transcript.species == species: transcript.parseVCF(vcf_file)

	def mk_test (self):

		# Create the output path for the test data
		output_path = os.path.join(self._out_dir, '_'.join(self.species_pair))
		if not os.path.exists(output_path): os.makedirs(output_path)

		self._MSF_filename = os.path.join(output_path, f'{self.name}.fasta')
		self._MSA_AA_filename = os.path.join(output_path, f'{self.name}.aa.aln')
		self._MSA_Codon_filename = os.path.join(output_path, f'{self.name}.codon.aln')

		self._cleaveTerminal()
		self._filter()
		self._translate()
		self._createMSF()
		self._createMafftMSA()
		self._mapGaps()

		# Create list to store results
		mk_results = []

		# Iterate the species pair combinations
		for species, outgroup in itertools.permutations(self.species_pair):

			# Calculate MK
			sf, sp, nf, np, ni, alpha, fet_ts, fet_greater = self._calcMK(species, outgroup)

			# Append the results OrderedDict({("a", 1), ("b", 2), ("c", 3)})
			mk_result_dict = {'OG':self.name, 'Species':species, 'Outgroup':outgroup, 'NI':ni,
							  'Alpha': alpha, 'SF':sf, 'SP':sp, 'NF':nf, 'NP':np,
							  "Fisher's exact test (two-sided) oddsratio":fet_ts[0], 
							  "Fisher's exact test (two-sided) p-value":fet_ts[1],
							  "Fisher's exact test (one-sided) oddsratio":fet_greater[0], 
							  "Fisher's exact test (one-sided) p-value":fet_greater[1]}
			mk_results.append(mk_result_dict)

		return mk_results

	def _cleaveTerminal (self, terminal_stop_percentage = 0.4):

		for transcript in self.values(): transcript.cleaveTerminal(terminal_stop_percentage)

	def _filter (self, missing_data_percentage = 0.20, min_sample_size = 4):

		for transcript in self.values(): transcript.filter(missing_data_percentage, min_sample_size)

	def _translate (self, stop_symbol = 'X'):

		for transcript in self.values(): transcript.translate(stop_symbol)

	def _createMSF (self, seq_format = 'fasta'):

		# Create the file
		output_file = open(self._MSF_filename, 'w')
		for transcript in self.values(): SeqIO.write(transcript.translated_records, output_file, seq_format)
		output_file.close()

	def _createMafftMSA (self):

		# Create the file
		alignMafft(self._MSF_filename, self._MSA_AA_filename)

	def _mapGaps (self):

		# Create dict to act as a sequence index
		seq_dict = {}
		for transcript in self.values():
			for cds_record in transcript.cds_records: seq_dict[cds_record.id] = cds_record

		# Create the file
		mapGapsFromIndex(seq_dict, self._MSA_AA_filename, self._MSA_Codon_filename, confirm_match = False)

	def _calcMK (self, species, outgroup, seq_format = 'fasta'):

		def _neutrality_index ():
			try: return ((NP / SP) / (NF / SF))
			except: return np.nan

		def _alpha ():
			try: return (1 - ((SF * NP) / (NF * SP)))
			except: return np.nan

		def _fisher_exact (sided = 'two-sided'):
			if sided == 'two-sided': return fisher_exact([[NF, NP], [SF, SP]])
			elif sided == 'greater': return fisher_exact([[NF, NP], [SF, SP]], alternative = 'greater')
			elif sided == 'less': return fisher_exact([[NF, NP], [SF, SP]], alternative = 'less')

		def _is_fixed (species_variant_set):

			if len(species_variant_set) == 1: return True
			else: return False

		def _is_synonymous ():
				
			def _ambiguousCodonPos (codon):

				nt_list = list(codon)
				nt_list[mk_site] = 'N'
				return ''.join(nt_list)

			# Set to None for assignment confirmation
			is_synonymous = None

			# Loop the species codons
			for species_codon in species_codon_set:

				# Assign AA and ambiguous codon
				species_aa = Seq(species_codon).translate()
				ambiguous_species_codon = _ambiguousCodonPos(species_codon)
				
				# Check if the outgroup has a matching ambiguous codons
				ambiguous_aa_set = set()
				for outgroup_codon in outgroup_codon_set:
					ambiguous_outgroup_codon = _ambiguousCodonPos(outgroup_codon)
					if ambiguous_species_codon == ambiguous_outgroup_codon:
						ambiguous_aa_set.add(Seq(outgroup_codon).translate())
				if len(ambiguous_aa_set) > 0:
					if species_aa not in ambiguous_aa_set: return False 
					else: is_synonymous = True

				# Mutate check
				mutated_aa_set = set()
				for outgroup_codon in outgroup_codon_set:
					mutated_codon = ambiguous_species_codon.replace('N', outgroup_codon[mk_site])
					mutated_aa = Seq(mutated_codon).translate()
					mutated_aa_set.add(mutated_aa)

				if species_aa not in mutated_aa_set: return False 
				else: is_synonymous = True

			# Return error if the codon was not assigned
			if is_synonymous == None: raise Exception('Error in synonymous status assignment')

			return is_synonymous

		SF = 0
		SP = 0
		NF = 0
		NP = 0

		# Open the MSA
		with open(self._MSA_Codon_filename) as codon_msa_file:
			codon_msa = AlignIO.read(codon_msa_file, seq_format)

			# Record the positions of the species and outgroup sequences
			species_pos_list = []
			outgroup_pos_list = []
			for record_pos, codon_record in enumerate(codon_msa):
				if codon_record.id[:4] == outgroup: outgroup_pos_list.append(record_pos)
				elif codon_record.id[:4] == species: species_pos_list.append(record_pos)
				else: raise Exception(f'Error in record postion assignment for: {self.name}')

			# Loop alignment by codon
			for align_pos in range(0, codon_msa.get_alignment_length(), 3):

				# Create simple codon set for filtering
				codon_set = set(str(_sr.seq) for _sr in codon_msa[:, align_pos:align_pos + 3])

				# Skip codon if gaps or missing data are found
				filter_re = re.compile('^.*(-|N)+.*$')
				if any(filter(filter_re.match, codon_set)): continue

				# Skip codon if no changes were observed
				if len(codon_set) == 1: continue

				# Assign the relevant data to the species and outgroup sets
				species_codon_set = set()
				outgroup_codon_set = set()
				for record_pos, codon_record in enumerate(codon_msa[:, align_pos:align_pos + 3]):
					if record_pos in outgroup_pos_list: outgroup_codon_set.add(str(codon_record.seq))
					elif record_pos in species_pos_list: species_codon_set.add(str(codon_record.seq))

				'''
				Sample Filter:

				Remove a codon column if at least a single sample has more than
				one variable site. For example:

				Codon column 1:
				S1-1: ATA   | This codon column passes the filter as sample S1
				S1-2: ATC   | and sample S2 only have single variable site: ATM.
				S2-1: ATA   |
				S2-2: ATC   |

				Codon column 2:
				S1-1: ATA   | This codon column fails the filter as sample S1
				S1-2: AGC   | has two variable sites: AKM.
				S2-1: ATA   |
				S2-2: ATC   |
				'''
				mk_test_sites = []
				sample_polymorphic_sites = defaultdict(int)
				for codon_pos in range(3):

					species_alleles = defaultdict(set)
					outgroup_alleles = defaultdict(set)

					# Assign and store the sample alleles (both chromosomes)
					for record_pos, msa_sample in enumerate(codon_msa):
						msa_sample_id = msa_sample.id[:-2]
						if record_pos in outgroup_pos_list:
							outgroup_alleles[msa_sample_id].add(msa_sample.seq[align_pos + codon_pos])
						elif record_pos in species_pos_list:
							species_alleles[msa_sample_id].add(msa_sample.seq[align_pos + codon_pos])
						
					# Assign the bases at this position
					species_nt_set = set(itertools.chain(*species_alleles.values()))
					outgroup_nt_set  = set(itertools.chain(*outgroup_alleles.values()))

					# Check if the current position may be skipped
					if species_nt_set == outgroup_nt_set: continue

					# Report sample polymorphic sites
					for sample_id, sample_alleles in {**species_alleles, **outgroup_alleles}.items():
						if len(sample_alleles) > 1: sample_polymorphic_sites[sample_id] += 1
	
					# Assign the site
					mk_test_sites.append(codon_pos)

				# Skip codons w/ too many polymorphic sites in a sample
				if any(_pn > 1 for _pn in sample_polymorphic_sites.values()): continue


				# Create amino acid sets
				species_aa_set = {str(Seq(_sc).translate()) for _sc in species_codon_set}
				outgroup_aa_set = {str(Seq(_oc).translate()) for _oc in outgroup_codon_set}

				# Loop the mk sites
				for mk_site in mk_test_sites:

					# Check if the species variants are fixed
					species_mk_nt_set = {_sc[mk_site] for _sc in species_codon_set}
					variants_fixed = _is_fixed(species_mk_nt_set)
		
					# Check if species variants at this site are synonymous with the outgroup
					variants_synonymous = _is_synonymous()

					# Assign and count
					if variants_synonymous and variants_fixed: SF += 1
					elif not variants_synonymous and variants_fixed: NF += 1
					elif variants_synonymous and not variants_fixed: SP += 1
					elif not variants_synonymous and not variants_fixed: NP += 1
					else: raise Exception('MK Assignment error. This error should not be possible')


		# Return the results
		return SF, SP, NF, NP, _neutrality_index(), _alpha(), _fisher_exact(), _fisher_exact(sided = 'greater')

class MKTranscript ():
	def __init__ (self, species = '', name = '', chrom = '', strand = '', exons = []):
		self.species = species
		self.name = name
		self.chrom = chrom
		self.strand = strand
		self.exons = exons

		self.cds_records = []
		self.translated_records = []

	def __len__ (self):
		return sum([((_e[1] + 1) - _e[0]) for _e in self.exons])

	@classmethod
	def fromJSON (cls, json_data):

		# Create a MKGene object from the JSON dat
		return cls(species = json_data['species'],
				   name = json_data['name'],
				   chrom = json_data['chrom'],
				   strand = json_data['strand'],
				   exons = [(_sp, _ep) for _sp, _ep in json_data['exons']])

	def parseVCF (self, vcf_file):

		def _blank_exon_seq (exon_length):

			exon_seq_dict = defaultdict(list)
			for _s in vcf_samples:
				exon_seq_dict[f'{_s}-{0}'] = ['N'] * exon_length
				exon_seq_dict[f'{_s}-{1}'] = ['N'] * exon_length
			return exon_seq_dict

		def _append_exon_dict ():

			for _s in vcf_samples:
				sequence_dict[f'{_s}-{0}'] += ''.join(exon_seq_dict[f'{_s}-{0}'])
				sequence_dict[f'{_s}-{1}'] += ''.join(exon_seq_dict[f'{_s}-{1}'])
			return exon_seq_dict

		def _parse ():

			if self.strand == '+': 
				for _vs in vcf_file.fetch(self.chrom, exon_start - 1, exon_end): yield abs(_vs.pos - exon_start), _vs
			elif self.strand == '-': 
				for _vs in reversed(list(vcf_file.fetch(self.chrom, exon_start - 1, exon_end))): yield abs(_vs.pos - exon_end), _vs
			else: raise Exception(f'Strand not defined for: {self.name}')

		# Create dict to store the sequences
		sequence_dict = defaultdict(str)

		# Create base complement table
		base_complements = str.maketrans('ATCG', 'TAGC')

		# Assign the samples from the VCF
		vcf_samples = list(vcf_file.header.samples)

		# Loop each vcf position within the exons
		for exon_start, exon_end in self.exons:

			# Create blank exon to account for missing data
			exon_seq_dict = _blank_exon_seq((exon_end + 1) - exon_start)

			# Assign the genotype(s) for each sample
			for variant_pos, variant_site in _parse():
				for sample_name, sample_attributes in variant_site.samples.items():
					for sample_num, allele_pos in enumerate(sample_attributes['GT']):
						if allele_pos == None: continue
						sample_allele = variant_site.alleles[allele_pos]
						if self.strand == '-': sample_allele = sample_allele.translate(base_complements)
						
						# Report SNPs, report Indels as N
						if len(sample_allele) != 1: exon_seq_dict[f'{sample_name}-{sample_num}'][variant_pos] = 'N'
						else: exon_seq_dict[f'{sample_name}-{sample_num}'][variant_pos] = sample_allele

			# Append the exon
			_append_exon_dict()

		# Save as sequence records
		for sample_name, sample_sequence in sequence_dict.items():
			fasta_id = f'{self.name}_{sample_name}'
			self.cds_records.append(SeqRecord(Seq(sample_sequence), id = fasta_id, description = ''))

	def cleaveTerminal (self, terminal_stop_percentage):

		# Assign the stop codons
		stop_codons = ['TAG', 'TAA', 'TGA']

		# Assign the number of records
		total_records = len(self.cds_records)

		# Count the number of stop codons
		terminal_stops = 0
		for record in self.cds_records:
			if record.seq[-3:] in stop_codons: terminal_stops += 1

		# Check if there is no need to cleave
		if terminal_stops == 0: return

		# Check if unable to cleave
		elif terminal_stops < (total_records * terminal_stop_percentage):
			raise Exception(f'Unable to cleave terminal stop for: {self.name}')

		# Cleave the terminal stop codons
		for record_pos in range(0, total_records):
			self.cds_records[record_pos].seq = self.cds_records[record_pos].seq[:-3]

	def filter(self, missing_data_percentage, min_sample_size):

		def _calc_missing_data (record):
			return record.seq.count('N') / len(record)

		# Only run if there are cds records
		if not self.cds_records: return

		# Filter the records for missing data
		samples_to_remove = []
		for sample_pos, sample_record in enumerate(self.cds_records):
			if _calc_missing_data(sample_record) > missing_data_percentage:
				samples_to_remove.append(sample_pos)

		# Check if too many samples were removed
		if len(self.cds_records) - len(samples_to_remove) < min_sample_size:
			raise Exception(f'Too many samples filtered for: {self.name}')

		# Remove samples by index (in reverse to preserve index)
		for sample_index in samples_to_remove[::-1]:
			del self.cds_records[sample_index]

	def translate (self, stop_symbol):

		# Translate the record, update the header
		for sample_record in self.cds_records:
			translated_record = sample_record.translate(stop_symbol = stop_symbol)
			translated_record.id = sample_record.id
			translated_record.description = ''
			self.translated_records.append(translated_record)

def speciesVCFs (vcf_dir, species_list, species_prefix_len = 4):

	for vcf_file in os.listdir(vcf_dir):
		if not vcf_file.endswith('.vcf.gz'): continue
		vcf_path = os.path.join(vcf_dir, vcf_file)
		vcf_species = vcf_file[:species_prefix_len]
		if vcf_species not in species_list: continue
		yield vcf_species, VariantFile(vcf_path)

def mkArgs ():

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

	def confirmDir ():
		'''Custom action to confirm directory exists'''
		class customAction(argparse.Action):
			def __call__(self, parser, args, value, option_string=None):
				if not os.path.isdir(value):
					raise IOError('%s not found' % value)
				setattr(args, self.dest, value)
		return customAction

	mk_parser = argparse.ArgumentParser(formatter_class = argparse.ArgumentDefaultsHelpFormatter)

	# Assign I/O arguments
	mk_parser.add_argument('--orthology-json', help = "Defines the orthology JSON filename", type = str, action = confirmFile(), required = True)
	mk_parser.add_argument('--species-pairs', help = "Defines the species pairs filename", type = str, action = confirmFile(), required = True)
	mk_parser.add_argument('--vcf-dir', help = "Defines the VCF directory", type = str, action = confirmDir(), required = True)
	mk_parser.add_argument('--out-file', help = "Defines the output filename (as tsv). Defaults to MK_Results.tsv", type = str, default = 'MK_Results.tsv')

	# Return arguments
	return mk_parser.parse_args()

def main():

	# Assign the arguments
	mk_args = mkArgs()

	# Opening JSON file
	with open(mk_args.orthology_json, 'r') as json_file: json_orthologs = json.load(json_file)

	# Assign the species pairs from the file
	species_pair_list = pd.read_csv(mk_args.species_pairs, header = None).values

	# Create output dataframe
	mk_results = pd.DataFrame(columns = ['OG', 'Species', 'Outgroup', 'NI', 
										 'Alpha', 'SF', 'SP', 'NF', 'NP',
										 "Fisher's exact test (two-sided) oddsratio",
										 "Fisher's exact test (two-sided) p-value",
										 "Fisher's exact test (one-sided) oddsratio",
										 "Fisher's exact test (one-sided) p-value",])

	# Perform the MK test for each pair of species
	for species_pair in species_pair_list:
		print(species_pair)
		
		# Create the orthologs for the species pair
		mk_orthologs = [MKOrtholog.fromJSON(json_ortholog, species_pair) for json_ortholog in json_orthologs]

		# Load the relevant VCFs
		for species, species_vcf in speciesVCFs(mk_args.vcf_dir, species_pair):
			for mk_ortholog in mk_orthologs: mk_ortholog.loadSpeciesVCF(species, species_vcf)

		# Perform the MK test for each ortholog
		for mk_ortholog in mk_orthologs:
			mk_results_list = mk_ortholog.mk_test()

			for mk_result in mk_results_list: 
				mk_results = mk_results.append(mk_result, ignore_index = True, sort = False)

	# Save the results as a tsv
	mk_results.to_csv(mk_args.out_file, sep = '\t', index = False)

if __name__ == "__main__":
	main()