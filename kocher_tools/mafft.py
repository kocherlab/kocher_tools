#!/usr/bin/env python
import os
import sys
import subprocess
import logging

from kocher_tools.misc import confirmExecutable

def checkMafftForErrors (mafft_stderr):

	# Check if the stderr is empty
	if not mafft_stderr: return

	# Return with no error if the output looks normal
	if 'Strategy' in mafft_stderr and 'done' in mafft_stderr: return

	# Report the error
	raise Exception(mafft_stderr)

def pipeMafft (mafft_call_args, mafft_output, accurate = True):

	# Find the Mafft executable
	if not accurate: mafft_executable = confirmExecutable('mafft')
	else: mafft_executable = confirmExecutable('mafft-linsi')

	# Check if executable is installed
	if not mafft_executable:
		raise IOError('MAFFT not found. Please confirm the executable is installed')

	# Open the output file
	mafft_output_file = open(mafft_output, 'w')

	# Mafft subprocess call
	mafft_call = subprocess.Popen([mafft_executable] + mafft_call_args, stderr = subprocess.PIPE, stdout = mafft_output_file)

	# Get stdout and stderr from subprocess
	mafft_stdout, mafft_stderr = mafft_call.communicate()

	# Convert bytes to string, if needed
	if sys.version_info[0] == 3: mafft_stderr = mafft_stderr.decode()

	# Check the stderr for errors
	checkMafftForErrors(mafft_stderr)

	# Close the file
	mafft_output_file.close()

def alignMafft (unaligned_filename, output_filename):

	# Align the file
	pipeMafft([unaligned_filename], output_filename)