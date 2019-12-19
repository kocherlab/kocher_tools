import os
import sys
import csv

def readCommonFile (common_filename, sep = '\t'):

	with open(common_filename) as common_file:

		# Read the data file
		common_reader = csv.reader(common_file, delimiter = sep)

		if sys.version_info[0] == 3:

			# Save the header
			header = next(common_reader)

		else:
		
			# Save the header
			header = common_reader.next()

		# Loop the rows of data
		for row in common_reader:

			# Return the header and row
			yield header, row