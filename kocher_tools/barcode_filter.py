#!/usr/bin/env python
import os
import sys
import csv
import argparse
import logging
import json
import copy

import pandas as pd
from collections import defaultdict

from kocher_tools.logger import startLogger

def barcodeFilterParser ():
	'''
	Barcode Filter Parser

	Assign the parameters for the barcode filter.

	Parameters
	----------
	sys.argv : list
		Parameters from command lind

	Raises
	------
	IOError
		If the specified files do not exist
	'''

	def confirmFile ():
		'''Custom action to confirm file exists'''
		class customAction(argparse.Action):
			def __call__(self, parser, args, value, option_string=None):
				if not os.path.isfile(value):
					raise IOError('%s not found' % value)
				setattr(args, self.dest, value)
		return customAction

	def metavarList (var_list):
		'''Create a formmated metavar list for the help output'''
		return '{' + ', '.join(var_list) + '}'

	filter_parser = argparse.ArgumentParser(formatter_class = argparse.ArgumentDefaultsHelpFormatter)

	# Input file args
	filter_parser.add_argument('--blast-file', help = 'Filename of the BLAST output', type = str, action = confirmFile(), required = True)

	# Control filters
	filter_parser.add_argument('--negative-control', help = 'Negative control ID', type = str, nargs = '+')
	filter_parser.add_argument('--negative-control-file', help = 'File of negative control IDs', type = str, action = confirmFile())

	# Basic filter cutoff args\
	filter_parser.add_argument('--abundance-cutoff', help = 'Read abundance Cutoff', type = int, default = 25)
	filter_parser.add_argument('--evalue-cutoff', help = 'E-Value Cutoff', type = float, default = 0.00001)
	filter_parser.add_argument('--coverage-cutoff', help = 'Coverage cutoff - i.e. percentage of sequence aligned', type = float, default = 0.90)
	filter_parser.add_argument('--identity-cutoff', help = 'Identity cutoff - i.e. identity percentage between alignment sequences', type = float, default = 0.95)

	# Best hit args
	sort_by = ('percent_ident', 'align_len')
	filter_parser.add_argument('--sort-best-hits-by', metavar = metavarList(sort_by), help = 'Sort method of best hits', choices = sort_by, default = ['percent_ident'], nargs = '+', type = str)
	filter_parser.add_argument('--dont-merge-species', help = 'Do not merge best hits within a single species', action = 'store_true')

	# Output file args
	filter_parser.add_argument('--out-prefix', help = 'Output prefix for the BLAST file', type = str, default = 'Filtered_')
	filter_parser.add_argument('--out-blast', help = 'Output filename for the BLAST file. Overrides --out-prefix', type = str)
	filter_parser.add_argument('--out-failed', help = 'Output filename for BLAST failures json file', type = str, default = 'barcode_failures.json')
	filter_parser.add_argument('--out-log', help = 'Filename of the log file', type = str, default = 'barcode_filter.log')
	filter_parser.add_argument('--overwrite', help = 'Overwrite previous output', action = 'store_true')

	# Return the arguments
	return filter_parser.parse_args()

def yieldBestHits (blast_reader):

	# Create a string to store the query being filtered
	filter_query = None

	# Create a float to store the lowest e-value
	filter_lowest_evalue = None

	# Create list to store the best hits of the current query
	filter_best_hits = []

	# Read the file, line by line
	for blast_dict in blast_reader:

		# Save the current query ID
		current_query = blast_dict['Query ID']

		# Save the current evalue
		current_evalue = float(blast_dict['E-Value'])

		# Check if the current query doesn not match the filter query
		if filter_query != current_query:

			# Check if any best hits have been assigned
			if filter_best_hits:

				yield filter_query, filter_best_hits

			# Assign the new filter query
			filter_query = current_query

			# Assign the new lowest e-value
			filter_lowest_evalue = current_evalue

			# Assign the new best hit
			filter_best_hits = [blast_dict]

		else:

			# Check if the current e-value is lower than the filtered e-value
			if filter_lowest_evalue > current_evalue:

				# Assign the new lowest e-value
				filter_lowest_evalue = current_evalue

				# Assign the new best hit
				filter_best_hits = [blast_dict]

			# Check if the current e-value is equal to the filtered e-value
			elif filter_lowest_evalue == current_evalue:

				# Append the best blast hit
				filter_best_hits.append(blast_dict)

	# Check if there is data after the loop is finished
	if filter_query and filter_best_hits:

		# Yield the data
		yield filter_query, filter_best_hits

def sortBestHits (best_hits, method):

	# Create list to store the sorted best hits
	sorted_best_hits = []

	# Check if the method is sort by percent identity
	if method == 'percent_ident':

		# Create a float to store the highest percent identity
		best_percent_identity = 0.0

		# Loop the filtered hits
		for blast_hit in best_hits:

			# Save the current identity
			current_identity = float(blast_hit['Percent Identity'])

			# Check if this hit has a higher percent identity
			if best_percent_identity < current_identity:

				# Update the best hit
				sorted_best_hits = [blast_hit]

				# Update the highest percent identity
				best_percent_identity = current_identity

			# Check if this hit has a higher percent identity
			elif best_percent_identity == current_identity:

				# Update the best hit
				sorted_best_hits.append(blast_hit)

	# Check if the method is sort by percent identity
	elif method == 'align_len':

		# Create a float to store the highest alignment length
		best_align_len = 0.0

		# Loop the filtered hits
		for blast_hit in best_hits:

			# Save the current alignment length
			current_align_len = float(blast_hit['Alignment Length'])

			# Check if this hit has a higher alignment length
			if best_align_len < current_align_len:

				# Update the best hit
				sorted_best_hits = [blast_hit]

				# Update the highest alignment length
				best_align_len = current_align_len

			# Check if this hit has a higher alignment length
			elif best_align_len == current_align_len:

				# Update the best hit
				sorted_best_hits.append(blast_hit)

	# Return the sorted best hits
	return sorted_best_hits

def main():

	# Assign the barcode args
	barcode_args = barcodeFilterParser()

	# Create the log file
	startLogger(barcode_args.out_log)

	# Check if a BLAST output filename was defined
	if barcode_args.out_blast:
		
		# Define the BLAST output filename
		blast_out_filename = barcode_args.out_blast

		# Get the output dirname
		out_dirname = os.path.dirname(blast_out_filename)

		# Define the JSON output filename
		failed_out_filename = os.path.join(out_dirname, barcode_args.out_failed)

	# If not, define using a prefix
	else:

		# Get the BLAST basename
		blast_basename = os.path.basename(barcode_args.blast_file)

		# Get the BLAST dirname
		blast_dirname = os.path.dirname(barcode_args.blast_file)

		# Define the BLAST output filename
		blast_out_filename = os.path.join(blast_dirname, barcode_args.out_prefix + blast_basename)

		# Define the JSON output filename
		failed_out_filename = os.path.join(blast_dirname, barcode_args.out_failed)

	# Check if previous output should be overwritten
	if barcode_args.overwrite:

		# Remove the previous output, if it exists
		if os.path.exists(blast_out_filename):
			os.remove(blast_out_filename)
		if os.path.exists(failed_out_filename):
			os.remove(failed_out_filename)

	# Check if previous output shouldn't be overwritten
	else:

		# Check if previous output exists
		if os.path.exists(blast_out_filename) or os.path.exists(failed_out_filename):

			# Raise an exception
			raise Exception('Output already exists. Please alter the output arguments or use --overwrite')

	# Create a list of negative control IDs
	negative_control_list = []

	# Check if negative control IDs were given on the command line
	if barcode_args.negative_control:

		# Assign the negative control IDs
		negative_control_list.extend(barcode_args.negative_control)

	# Check if a file of negative control IDs was given
	if barcode_args.negative_control_file:

		# Open the file
		with open(barcode_args.negative_control_file, 'r') as negative_control_file:

			# Read the file, line by line
			for negative_control_line in negative_control_file:

				# Assign the negative control ID
				negative_control_list.append(negative_control_line.strip())

	# Create a list to store failed sample dictonaries
	failed_samples_list = []

	# Open the BLAST input file
	with open(barcode_args.blast_file) as blast_file:

		# Read the blast using DictReader
		blast_reader = csv.DictReader(blast_file, delimiter = '\t')

		# Create the blast output file
		blast_out_file = open(blast_out_filename, 'w')

		# Create the blast writer using DictReader
		blast_writer = csv.DictWriter(blast_out_file, fieldnames = blast_reader.fieldnames, delimiter = '\t')

		# Write the header for the output file
		blast_writer.writeheader()

		# Loop each query with its best hits
		for current_query, best_blast_hits in yieldBestHits(blast_reader):

			# Create a list to store the filtered blast hits
			filtered_blast_hits = []

			# Save the current ID
			current_ID = current_query.rsplit('_', 1)[0]

			# Save the current abundance
			current_abundance = int(current_query.split('=')[1])

			# Check if an abundance cutoff was specified
			if barcode_args.abundance_cutoff:

				# Check if the current abundance is below the cutoff
				if current_abundance < barcode_args.abundance_cutoff:

					# Save warning message
					warning_message = 'Insufficent read abundance'
					
					# Create dict to hold all relevant information for the failed sample
					failed_sample_dict = {'Query ID':current_query, 'Status':'Insufficent reads'}

					# Add the dict to the list
					failed_samples_list.append(failed_sample_dict)

					# Log the failure
					logging.warning('%s: %s' % (current_query.split(';')[0], warning_message))

					continue

			# Loop each best hit
			for blast_hit in best_blast_hits:

				# Save the current evalue
				current_evalue = float(blast_hit['E-Value'])

				# Save the current query length
				current_query_length = float(blast_hit['Query Length'])

				# Save the current alignment length
				current_alignment_length = float(blast_hit['Alignment Length'])

				# Save the current identity
				current_identity = float(blast_hit['Percent Identity'])

				# Check if an E-Value cutoff was specified
				if barcode_args.evalue_cutoff:

					# Check if the current evalue is larger than the cutoff
					if current_evalue > barcode_args.evalue_cutoff:
						continue
						
				# Check if a coverage cutoff was specified
				if barcode_args.coverage_cutoff:

					# Save the percent coverage
					current_coverage = current_alignment_length / current_query_length

					# Check if the current coverage is samller than the cutoff
					if current_coverage < barcode_args.coverage_cutoff:
						continue

				# Check if an identity cutoff was specified
				if barcode_args.identity_cutoff:

					# Check if the current identity is smaller than the cutoff
					if current_identity < barcode_args.identity_cutoff:
						continue

				# Add the blast hit to the filtered list, if passed
				filtered_blast_hits.append(blast_hit)

			# Check if no blast hits passed the filter
			if len(filtered_blast_hits) == 0:

				# Save warning message
				warning_message = 'Filtered resulted in the removal of all data'
				
				# Create dict to hold all relevant information for the failed sample
				failed_sample_dict = {'Query ID':current_query, 'Status':'No Hits'}

				# Add the dict to the list
				failed_samples_list.append(failed_sample_dict)

				# Log the failure
				logging.warning('%s: %s' % (current_query.split(';')[0], warning_message))

			# Copy the filtered list before sorting
			sorted_best_hits = copy.deepcopy(filtered_blast_hits)

			# Loop the sort methods
			for sort_method in barcode_args.sort_best_hits_by:

				# Sort the best hits
				sorted_best_hits = sortBestHits(sorted_best_hits, sort_method)

			# Check if there is more than one sorted best hit
			if len(sorted_best_hits) > 1:

				# Create a list for checking the number of species
				sorted_species_list = []

				# Create a list for checking the number of bins
				sorted_bin_list = []

				# Loop the sorted best hits
				for sorted_best_hit in sorted_best_hits:

					# Assign the species of the hit
					sorted_species = sorted_best_hit['Subject ID'].split('|')[1].replace('_', ' ')

					# Append the species to the list
					sorted_species_list.append(sorted_species)

					# Assign the bin of the hit
					sorted_bin = sorted_best_hit['Subject ID'].split('|')[2]

					# Append the bin to the list
					sorted_bin_list.append(sorted_bin)

				# Remove duplicate species
				sorted_species_list = list(set(sorted_species_list))

				# Remove duplicate bins
				sorted_bin_list = list(set(sorted_bin_list))

				# Check if a BOLD:N/A was found
				if 'BOLD:N/A' in sorted_bin_list and len(sorted_bin_list) > 1:

					# Move the BOLD:N/A to the end of the list
					sorted_bin_list.append(sorted_bin_list.pop(sorted_bin_list.index('BOLD:N/A'))) 

				# Check if more than a single species among the best hits
				if len(sorted_species_list) > 1:

					# Save warning message
					warning_message = 'Multiple species identified'
					
					# Create dict to hold all relevant information for the failed sample
					failed_sample_dict = {'Query ID':current_query, 'Status':'Ambiguous Hits', 
					                      'Species':sorted_species_list , 'Bins':sorted_bin_list}

					# Add the dict to the list
					failed_samples_list.append(failed_sample_dict)

					# Log the failure
					logging.warning('%s: %s' % (current_query.split(';')[0], warning_message))

					continue

				# Check if merging should occur, and if there is only a single species among the best hits
				elif not barcode_args.dont_merge_species and len(sorted_species_list) == 1:

					# Take the first sorted hit, update this later with preferences
					sorted_best_hits = [sorted_best_hits[0]]

			# Check if any negative control IDs are assigned
			if sorted_best_hits and negative_control_list and current_ID in negative_control_list:

				logging.warning('Negative control (%s) passed filters. Please consider stricter cutoffs' % current_ID)

				continue

			# Loop the sorted best hits
			for sorted_best_hit in sorted_best_hits:

				# Write the passed blast entry to the output
				blast_writer.writerow(sorted_best_hit)

		# Close the output file
		blast_out_file.close()

	# Check if any samples failed to find best hits
	if failed_samples_list:

		logging.warning('Samples (%s) failed filters. Samples may be found in: %s' % (len(failed_samples_list), failed_out_filename))

	# Open the json failure file
	with open(failed_out_filename, 'w') as json_failed_file:

		# Dump the failed sample list to the JSON file
		json.dump(failed_samples_list, json_failed_file, indent = 4)

if __name__== "__main__":
	main()