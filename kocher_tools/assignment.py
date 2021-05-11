import logging

import pandas as pd
#from collections import defaultdict

def readInputFile (input_file):

	# Create an empty dataframe
	input_dataframe = pd.DataFrame()

	# Try to read an excel input file
	try: input_dataframe = pd.read_excel(input_file, dtype = str, engine = 'openpyxl')
	except:

		# Try to read a tsv input file
		try: input_dataframe = pd.read_csv(input_file, dtype = str, sep = '\t')
		except:

			# Try to read a csv input file
			try: input_dataframe = pd.read_csv(input_file, dtype = str)
			except: pass

	# Return an exception if the input is empty
	if input_dataframe.empty: raise Exception(f'Unable to parse input file: {input_file}')

	# Remove any empty rows
	input_dataframe = input_dataframe.dropna(axis = 0, how = 'all')

	# Return the dataframe if not empty
	logging.info(f'Successfully assigned input file: {input_file}')
	return input_dataframe


'''
def assignTables (config_data, include = None, exclude = None, include_ID = None, exclude_ID = None, include_species = None, exclude_species = None, include_genus = None, exclude_genus = None, include_nests = None, exclude_nests = None, **kwargs):

	# Create a list to hold the columns
	columns = []
	
	# Check if basic include is defined
	if include:

		# Update the column list
		columns.extend(list(include.keys()))

	# Check if basic exclude is defined
	if exclude:

		# Update the column list
		columns.extend(list(exclude.keys()))

	# Check if ID include is defined
	if include_ID:

		# Update the column list
		columns.append('Unique ID')

	# Check if ID exclude is defined
	if exclude_ID:

		# Update the column list
		columns.append('Unique ID')

	# Check if species include is defined
	if include_species:

		# Update the column list
		columns.append('Species')

	# Check if species exclude is defined
	if exclude_species:

		# Update the column list
		columns.append('Species')

	# Check if genus include is defined
	if include_genus:

		# Update the column list
		columns.append('Species')

	# Check if genus exclude is defined
	if exclude_genus:

		# Update the column list
		columns.append('Species')

	# Check if nests include is defined
	if include_nests:

		# Update the column list
		columns.append('From Nest?')

	# Check if genus exclude is defined
	if exclude_nests:

		# Update the column list
		columns.append('From Nest?')


	# Assign the tables
	assigned_tables = config_data.returnTables(columns)

	# Update log
	logging.info('Successfully assigned selection statement table(s)')

	# Return the tables
	return assigned_tables
	
def assignSelectionDict (config_data, include = None, exclude = None, include_ID = None, exclude_ID = None, include_species = None, exclude_species = None, include_genus = None, exclude_genus = None,include_nests = None, exclude_nests = None,  **kwargs):

	# Create a defaultdict to hold all the selection information
	selection_dict = defaultdict(lambda: defaultdict(list))

	# Check if basic include is defined
	if include:

		# Update the selection dict
		selection_dict['IN'] = config_data.returnColumnPathDict(include)

	# Check if basic exclude is defined
	if exclude:

		# Update the selection dict
		selection_dict['NOT IN'] = config_data.returnColumnPathDict(exclude)

	# Check if ID include is defined
	if include_ID:

		# Update the selection dict
		selection_dict['IN'][config_data.returnColumnPath('Unique ID')] = include_ID

	# Check if ID exclude is defined
	if exclude_ID:

		# Update the selection dict
		selection_dict['NOT IN'][config_data.returnColumnPath('Unique ID')] = exclude_ID

	# Check if species include is defined
	if include_species:

		# Update the selection dict
		selection_dict['IN'][config_data.returnColumnPath('Species')] = include_species

	# Check if species exclude is defined
	if exclude_species:

		# Update the selection dict
		selection_dict['NOT IN'][config_data.returnColumnPath('Species')] = exclude_species

	# Check if genus include is defined
	if include_genus:

		# Update the selection dict
		selection_dict['LIKE'][config_data.returnColumnPath('Species')] = include_genus

	# Check if genus exclude is defined
	if exclude_genus:

		# Update the selection dict
		selection_dict['NOT LIKE'][config_data.returnColumnPath('Species')] = exclude_genus

	# Check if genus include is defined
	if include_nests:

		# Update the selection dict
		selection_dict['IN'][config_data.returnColumnPath('From Nest?')] = ['Yes']

	# Check if genus exclude is defined
	if exclude_nests:

		# Update the selection dict
		selection_dict['NOT IN'][config_data.returnColumnPath('From Nest?')] = ['Yes']

	# Update log
	logging.info('Successfully assigned selection statement dict(s)')

	return selection_dict

'''