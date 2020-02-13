import os
import sys
import logging
import subprocess

from kocher_tools.misc import confirmExecutable

def checkVsearchForErrors (vsearch_stderr):

	# Check if an error was reported in the stderr
	if 'Fatal error' in vsearch_stderr:

		# Report the error
		raise Exception(vsearch_stderr)

def callVsearch (vsearch_call_args):

	# Find the vsearch executable
	vsearch_executable = confirmExecutable('vsearch')

	# Check if executable is installed
	if not vsearch_executable:
		raise IOError('vsearch not found. Please confirm the executable is installed')

	# vsearch subprocess call
	vsearch_call = subprocess.Popen([vsearch_executable] + vsearch_call_args, stderr = subprocess.PIPE, stdout = subprocess.PIPE)

	# Get stdout and stderr from subprocess
	vsearch_stdout, vsearch_stderr = vsearch_call.communicate()

	# Check if code is running in python 3
	if sys.version_info[0] == 3:
		
		# Convert bytes to string
		vsearch_stdout = vsearch_stdout.decode()
		vsearch_stderr = vsearch_stderr.decode()

	# Check for errors
	checkVsearchForErrors(vsearch_stderr)

def mergePairs (sample, r1_file, r2_file, merged_file, unmerged_R1_file, unmerged_R2_file, min_merge_len = 360, max_diff = 20):

	# Assign the R1 and R2 files
	merge_pairs_args = ['--fastq_mergepairs', r1_file, '--reverse', r2_file]

	# Assign the merged output file
	merge_pairs_args.extend(['--fastqout', merged_file])

	# Relabel with the sample name
	merge_pairs_args.extend(['--relabel', '%s_' % sample])

	# Assign the unmerged files
	merge_pairs_args.extend(['--fastqout_notmerged_fwd', unmerged_R1_file, '--fastqout_notmerged_rev', unmerged_R2_file])

	# Assign the quality control args
	merge_pairs_args.extend(['--fastq_minmergelen', str(min_merge_len), '--fastq_maxdiffs', str(max_diff)])

	# Call VSEARCH
	callVsearch(merge_pairs_args)

def truncateFastq (input_file, truncated_file, strip_left_n = 26, strip_right_n = 26):

	# Assign the input file
	truncate_args = ['--fastq_filter', input_file]

	# Assign the truncated output file
	truncate_args.extend(['--fastqout', truncated_file])

	# Assign the truncate args
	truncate_args.extend(['--fastq_stripleft',  str(strip_left_n), '--fastq_stripright', str(strip_right_n)])

	# Call VSEARCH
	callVsearch(truncate_args)

def filterFastq (input_file, filtered_file, maxee_float = 0.5):

	# Assign the input file
	filter_args = ['--fastq_filter', input_file]

	# Assign the filtered output file
	filter_args.extend(['--fastaout', filtered_file])

	# Assign the filter args
	filter_args.extend(['--fastq_maxee', str(maxee_float)])

	# Call VSEARCH
	callVsearch(filter_args)

def dereplicateFasta (plate, sample, input_file, dereplicated_file, min_unique_int = 2):

	# Assign the input file
	dereplicate_args = ['--derep_fulllength', input_file]

	# Assign the dereplicated output file
	dereplicate_args.extend(['--output', dereplicated_file])

	# Relabel with the sample name
	dereplicate_args.extend(['--relabel', '%s-%s_' % (plate, sample)])

	# Assign the dereplicate args
	dereplicate_args.extend(['--minuniquesize', str(min_unique_int), '--sizeout'])

	# Call VSEARCH
	callVsearch(dereplicate_args)

def clusterFasta (plate, sample, input_file, clustered_file, id_float = 1.0):

    # Assign the input file
	cluster_args = ['--cluster_smallmem', input_file]

	# Assign the clustered output file
	cluster_args.extend(['--centroids', clustered_file])

	# Relabel with the sample name
	cluster_args.extend(['--relabel', '%s-%s_' % (plate, sample)])

	# Assign the clustering args
	cluster_args.extend(['--id', str(id_float), '--sizein', '--sizeout', '--usersort'])

	# Call VSEARCH
	callVsearch(cluster_args)

def sortFasta (plate, sample, input_file, sorted_file):

    # Assign the input file
	sort_args = ['--sortbysize', input_file]

	# Assign the sorted output file
	sort_args.extend(['--output', sorted_file])

	# Relabel with the sample name
	sort_args.extend(['--relabel', '%s-%s_' % (plate, sample)])

	# Assign the sort args
	sort_args.extend(['--sizeout'])

	# Call VSEARCH
	callVsearch(sort_args)

