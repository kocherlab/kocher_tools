import logging

def startLogger (log_filename = 'out.log'):

	# Config the log file
	logging.basicConfig(filename = log_filename, filemode = 'w', level = 'INFO', format = '%(asctime)s - %(funcName)s - %(levelname)s: %(message)s')

	# Start logging to stdout
	stdout_log = logging.StreamHandler()

	# Assign the stdout logging level
	stdout_log.setLevel(logging.WARNING)

	# Define the stdout format
	console_format = logging.Formatter('%(funcName)s - %(levelname)s: %(message)s')

	# Assign the format
	stdout_log.setFormatter(console_format)

	# Add the stdout handler
	logging.getLogger('').addHandler(stdout_log)