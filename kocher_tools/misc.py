import os
import logging

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

			logging.info('Calling executable: %s' % executable_file)

			# Return the path if the executable was found
			return executable_file

	# Return None if the executable was not found
	return None