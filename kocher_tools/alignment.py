#!/usr/bin/env python
import os
import sys
import re

from Bio import AlignIO, SeqIO
from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord
 
def countFFDegenerates (codon_msa_filename, reference, seq_format = 'fasta'):

	# Create list to store the results
	ffd_sites = []

	# Open the MSA
	with open(codon_msa_filename) as codon_msa_file:
		codon_msa = AlignIO.read(codon_msa_file, seq_format)

		# Get reference position
		reference_pos = -1
		for record_pos, codon_record in enumerate(codon_msa):
			if codon_record.id == reference:
				reference_pos = record_pos
				break
		if reference_pos == -1: raise Exception(f'Unable to assign reference: {reference}')

		""" Create a counter for the codon pos. Rather that the
		alignment pos (this will remove the imapact of gaps) but
		also requires the reference sequence to be known
		"""
		codon_pos = 0

		# Loop the MSA by codons
		for align_pos in range(0, codon_msa.get_alignment_length(), 3):
			codon_set = set(str(_sr.seq) for _sr in codon_msa[:, align_pos:align_pos + 3])

			# Skip codon if gaps are found
			gap_re = re.compile('^.*-+.*$')
			if any(filter(gap_re.match, codon_set)):
				codon = str(codon_msa[reference_pos, align_pos:align_pos + 3].seq)
				codon_pos += (3 - codon.count('-'))
				continue

			# Remove wobble base from codons
			codon_set_wo_wobble = set(_sr[0:2] for _sr in list(codon_set))
			
			# Skip codons that cannot be FFD (i.e. NS)
			if len(codon_set_wo_wobble) == 1:

				# Check if the column is FFD
				ff_degenerates = {'GC', 'CG', 'GG', 'CT', 'CC', 'TC'}
				if codon_set_wo_wobble <= ff_degenerates: ffd_sites.append(codon_pos + 2)

			# Increase the count of the codon pos
			codon_pos += 3

	return ffd_sites

def mapGapsFromIndex (seq_index, aa_msa_filename, output_filename, seq_format = 'fasta', confirm_match = True):

	# Create the output file
	output_file = open(output_filename, 'w')

	with open(aa_msa_filename) as aa_msa_file:
		for aa_record in SeqIO.parse(aa_msa_file, seq_format):

			# Get the DNA sequence for the record
			dna_record = seq_index[aa_record.id]

			# Map the DNA sequence
			aa_count = 0
			mapped_seq = ''
			for aa_pos, amino_acid in enumerate(aa_record.seq):

				# Map the gap
				if amino_acid == '-': mapped_seq += '---'
				
				# Map the codon
				else:

					# Assign and map the codon
					codon = dna_record.seq[aa_count * 3: (aa_count * 3) + 3]
					mapped_seq += codon
					aa_count += 1

					# Confirm the codon, raise exception if not possible
					if confirm_match and amino_acid != codon.translate():
						output_file.close()
						os.remove(output_filename)
						raise Exception(f'Codon does not match to amino acid in {aa_record.id}')
		
			# Save the mapped record
			SeqIO.write(SeqRecord(Seq(mapped_seq), id = aa_record.id, description = aa_record.id), output_file, seq_format)

	# Close the output file
	output_file.close()
