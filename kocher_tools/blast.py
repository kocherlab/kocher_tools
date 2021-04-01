import os
import sys
import subprocess
import logging

from kocher_tools.misc import confirmExecutable

def checkBlastForErrors (blast_stderr):

	# Check if the stderr isn't empty
	if blast_stderr:

		# Report the error
		raise Exception(blast_stderr)

def callBlast (blast_call_args):

	# Find the blast executable
	blast_executable = confirmExecutable('blastn')

	# Check if executable is installed
	if not blast_executable:
		raise IOError('blast not found. Please confirm the executable is installed')

	# blast subprocess call
	blast_call = subprocess.Popen([blast_executable] + blast_call_args, stderr = subprocess.PIPE, stdout = subprocess.PIPE)

	# Get stdout and stderr from subprocess
	blast_stdout, blast_stderr = blast_call.communicate()

	# Check if code is running in python 3
	if sys.version_info[0] == 3:
		
		# Convert bytes to string
		blast_stdout = blast_stdout.decode()
		blast_stderr = blast_stderr.decode()

	# Check the stderr for errors
	checkBlastForErrors(blast_stderr)

def pipeBlast (blast_call_args, blast_output, header = None):

	# Find the blast executable
	blast_executable = confirmExecutable('blastn')

	# Check if executable is installed
	if not blast_executable:
		raise IOError('blast not found. Please confirm the executable is installed')

	# Open the output file
	blast_output_file = open(blast_output, 'w')

	# Check if a header was specified
	if header:
		
		# Write the head to the output file
		blast_output_file.write(header + '\n')

		# Flush the file
		blast_output_file.flush()

	# blast subprocess call
	blast_call = subprocess.Popen([blast_executable] + blast_call_args, stderr = subprocess.PIPE, stdout = blast_output_file)

	# Get stdout and stderr from subprocess
	blast_stdout, blast_stderr = blast_call.communicate()

	# Check if code is running in python 3
	if sys.version_info[0] == 3:
		
		# Convert bytes to string
		blast_stderr = blast_stderr.decode()

	# Check the stderr for errors
	checkBlastForErrors(blast_stderr)

	# Close the file
	blast_output_file.close()

def blastTopHits (query_file, blast_output, database_file, num_threads):

	# Define the header list
	header_list = ['Query ID', 'Query Length', 'Subject ID', 'Subject Length', 'Percent Identity', 'Alignment Length', 'Mismatches', 'Gaps', 
	               'Query Alignment Start', 'Query Alignment End', 'Subject Alignment Start', 'Subject Alignment End', 'E-Value', 'Bitscore']

	# Define the output format
	output_format = '6 qseqid qlen sseqid slen pident length mismatch gapopen qstart qend sstart send evalue bitscore'

	# Create the blast argument list
	blast_call_args = ['-query', query_file, '-db', database_file, '-max_target_seqs', '100', '-outfmt', output_format, '-num_threads', str(num_threads)]

	# Call BLAST
	pipeBlast(blast_call_args, blast_output, header = '\t'.join(header_list))

def primerBLAST (query_file, blast_output, database_file, num_threads):

	# Define the output format
	output_format = '6'

	# Create the blast argument list
	blast_call_args = ['-query', query_file, '-db', database_file, '-max_target_seqs', '1', '-outfmt', output_format, '-num_threads', str(num_threads), '-out', blast_output]

	# Call BLAST
	callBlast(blast_call_args)