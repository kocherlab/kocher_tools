import os
import sys
import subprocess

def confirmExecutable (executable):

	'''
	    Confirm if an executable exists.

	    Parameters
	    ----------
	    executable : str
	        Executable to confirm
	'''

	# Loop potental locations of executables
	for path in os.environ['PATH'].split(os.pathsep):

		# Join current path and executable
		executable_file = os.path.join(path, executable)

		# Check if the executable path exists, and if so, is an executable
		if os.path.isfile(executable_file) and os.access(executable_file, os.X_OK):

			#logging.info('Calling executable: %s' % executable_file)

			# Return the path if the executable was found
			return executable_file

	# Return None if the executable was not found
	return None

def gzip_compress (filename, return_filename = False, overwrite = True):

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
	gzip_call = subprocess.Popen([gzip_executable, filename] + overwrite_arg, stderr = subprocess.PIPE, stdout = subprocess.PIPE)

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
		return filename + '.gz'