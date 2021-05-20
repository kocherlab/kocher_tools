import logging
import sys

def startLogger (log_filename = None, filemode = 'w'):

	# Close any old loggers
	for handler in logging.root.handlers[:]:
		handler.close()
		logging.root.removeHandler(handler)

	# Star the logger
	if log_filename: logging.basicConfig(filename = log_filename, filemode = filemode, level = 'INFO', format = '%(asctime)s - %(funcName)s - %(levelname)s: %(message)s')
	else: logging.basicConfig(stream = sys.stdout, level = 'INFO', format = '%(asctime)s - %(funcName)s - %(levelname)s: %(message)s')

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

	# Update the exception handler to log the exception
	def expHandler(etype,val,tb):

		# Log the error
		logging.error("%s" % (val), exc_info=(etype,val,tb))

	# Update the exception hook
	sys.excepthook = expHandler

def logArgs (args, print_undefined = False):

	# Loop the arguments
	for arg in vars(args):

		# Get the value associated with the argument
		value = vars(args)[arg]

		# Report only defined arguments, unless print_undefined is True
		if value is not None or print_undefined:

			# Log the argument
			logging.info('Argument %s: %s' % (arg, value))