import os
import sys
import subprocess
import gzip

from kocher_tools.misc import confirmExecutable

def gzipCompress (gzip_filename, return_filename = False, overwrite = True):

	# Find the gzip executable
	gzip_executable = confirmExecutable('gzip')

	# Check if executable is installed
	if not gzip_executable:
		raise IOError('gzip not found. Please confirm the executable is installed')

	# Assign the overwrite argument
	overwrite_arg = []

	# Check if gzip shouldnt overwrite
	if overwrite:

		# Add the overwrite arg
		overwrite_arg.append('-f')

	# vsearch subprocess call
	gzip_call = subprocess.Popen([gzip_executable, gzip_filename] + overwrite_arg, stderr = subprocess.PIPE, stdout = subprocess.PIPE)

	# Get stdout and stderr from subprocess
	gzip_stdout, gzip_stderr = gzip_call.communicate()

	# Check if code is running in python 3
	if sys.version_info[0] == 3:
		
		# Convert bytes to string
		gzip_stdout = gzip_stdout.decode()
		gzip_stderr = gzip_stderr.decode()

	# Update when starting logs
	#print(gzip_stdout)
	#print(gzip_stderr)

	# Check if the filename is to be returned
	if return_filename:
		return gzip_filename + '.gz'

def gzipIsEmpty (gzip_filename):

	# Open the gzip file
	with gzip.open(gzip_filename, 'rb') as gzip_file:

		# Read the file
		gzip_data = gzip_file.read(1)

	# Check if the file is empty
	if len(gzip_data) == 0:

		# Return true, if empty
		return True

	# Otherwise, return false
	return False