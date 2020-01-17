import yaml
import logging

import networkx as nx

from collections import OrderedDict, defaultdict
from itertools import combinations

class ConfigFile (list):
	def __init__ (self, *arg, **kw):
		super(ConfigFile, self).__init__(*arg, **kw)
		self.sql_database = ''
		self.table_graph = None

	def __contains__ (self, table_str):
		if table_str in self.tables:
			return True
		else:
			return False

	def __getitem__(self, table_str):
		for table in self:
			if str(table) == table_str:
				return table
	
	@property
	def tables (self):

		return [str(table) for table in self]	 

	def hasColumn (self, column_str):
		for table in self:
			if column_str in table:
				return True

		return False

	def assignFromYaml (self, config_yaml):
        
		# Assign the database filename
		self.sql_database = config_yaml['sql']['file']

		# Create a list for the tables
		table_list = []

		# Loop the config yaml
		for table, table_yaml in config_yaml['database']['tables'].items():

			# Create a DBTable object
			db_table = DBTable()

			# Assign the name of the table
			db_table.name = table

			# Assign the table from the yaml config
			db_table.assignFromYaml(table_yaml)

			# Save the table
			self.append(db_table)

		# Create a graph to store table edges
		self.table_graph = nx.Graph()

		# Loop all unique table combinations
		for db_table_source, db_table_dest in combinations(self, 2):

			# Check that the tables are compatible
			if db_table_source.join_by_key in db_table_dest or db_table_dest.join_by_key in db_table_source:

				# Add an edge between the two tables
				self.table_graph.add_edge(str(db_table_source), str(db_table_dest))

	def returnColumnPath (self, column):

		# Loop the tables in the config data
		for db_table in self:

			# Check if the table has an ID column assigned
			if column in db_table:

				# Return the path
				return db_table[column].path

	def returnJoinLists (self, tables_to_join):

		# Create list to store table paths
		table_paths = []

		# Loop all unique table combinations
		for db_table_source, db_table_dest in combinations(tables_to_join, 2):

			# Save the simple paths
			simple_paths = nx.all_simple_paths(self.table_graph, db_table_source, db_table_dest)

			# Loop the paths
			for simple_path in simple_paths:

				# Check if the path was already found
				if simple_path not in table_paths and simple_path[::-1] not in table_paths:

					# Get the simple paths for the tables
					table_paths.append(simple_path)

		# Create a list to store the path used to join the tables
		join_path = []

		# Loop each table path
		for first_table_pos in range(0, len(table_paths)):

			# Create bool to check if this path has all tables needed
			has_all_tables = True

			# Loop the other table paths
			for second_table_pos in range(first_table_pos + 1, len(table_paths)):

				#print(table_paths[first_table_pos], table_paths[second_table_pos])

				# Check if the table has fewer columns
				if len(table_paths[first_table_pos]) < len(table_paths[second_table_pos]):

					# Update the bool
					has_all_tables = False

					break

				# Check if the two tables have the sample columns
				has_all_tables = all(col in table_paths[first_table_pos] for col in table_paths[second_table_pos])

				
			# Check if the path has all tables needed
			if has_all_tables:

				# Check if a join path has been assigned
				if join_path:

					if len(table_paths[first_table_pos]) > len(join_path):

						# Update the join path
						join_path = table_paths[first_table_pos] 

				else:

					# Update the join path
					join_path = table_paths[first_table_pos]

		# Check if no join path was assigned
		if not join_path:

			# Return the error message
			raise Exception('Unable to join tables')

		# Create a dict to store the possible merges
		join_dict = defaultdict(list)

		# Loop each join path
		for first_path_pos in range(len(join_path)):

			# Create a list to store the tables that can be joined with the current table
			join_list = []

			# Loop the other join paths
			for second_path_pos in range(len(join_path)):

				# Make sure these arent the same tables, and check if they be can be joined
				if first_path_pos != second_path_pos and self[join_path[second_path_pos]].join_by_key in self[join_path[first_path_pos]]:

					# Add the table to the join list
					join_list.append(join_path[second_path_pos])

			# Check that the table joined other tables
			if join_list:

				# Assign the join data from the current table to the join dict
				join_dict[join_path[first_path_pos]] = join_list

		# Save list of the joinable table
		joinable_tables = list(join_dict.keys())

		# Create a dict to store the filtered merges
		filtered_join_dict = defaultdict(list)

		# loop the join dict by the table joining them
		for joinable_table in joinable_tables:

			# Create bool to decided if the current data should be removed
			keep_joinable_table = True

			# Loop the joined tables in the join dict
			for joined_tables in join_dict.values():

				# Create a list of the tables that may be joined to the joinable table
				joinable_tables = join_dict[joinable_table]

				# Skip if they joined tables with the same data
				if joinable_tables == joined_tables:

					continue

				# Check if another table has the same values
				if all(col in joined_tables for col in joinable_tables) and len(joined_tables) > len(joinable_tables):

					# Update the bool
					keep_joinable_table = False

			# Check if the current joined data should be removed
			if keep_joinable_table:

				# Add the table to the filtered dict
				filtered_join_dict[joinable_table] = join_dict[joinable_table]

		# Assign the a string for the primary table to join using
		join_using = ''

		# Save list of the joinable table
		possible_primary_join_tables = list(filtered_join_dict.keys())

		# Save string to store the primary table
		primary_join_table = ''

		# Loop the primary tables
		for possible_primary_join_table in possible_primary_join_tables:

			# Create int to store the number of other tables linked to the primary table
			primary_join_count = 0
			
			# Loop the tables joined to the primary tables
			for joined_to_primary in filtered_join_dict.values():

				# Check if the primary table is linkable
				if possible_primary_join_table in joined_to_primary:

					# Add to the count
					primary_join_count += 1

			# Check if the current primary table can link the other tables
			if primary_join_count == (len(possible_primary_join_tables) - 1):

				# Assign the primary table
				primary_join_table = possible_primary_join_table

				break

		# Create the lists to store the join data
		join_table_list = [primary_join_table]
		join_by_columns = []

		# Loop the tables of the primary join_table
		for primary_table in filtered_join_dict[primary_join_table]:

			# Confirm the table isn't a secondary join table
			if primary_table not in filtered_join_dict:

				# Populate the lists
				join_table_list.append(primary_table) 
				join_by_columns.append(self[primary_table].join_by_key)

			# Assign the secondary join table
			else:

				# Create the secondary lists to store the join data
				secondary_join_table_list = [primary_table]
				secondary_join_by_columns = []

				# Loop the secondary tables
				for secondary_table in filtered_join_dict[primary_table]:

					# Confirm the table isnt the primary join table
					if secondary_table != primary_join_table:

						# Populate the secondary lists
						secondary_join_table_list.append(secondary_table) 
						secondary_join_by_columns.append(self[secondary_table].join_by_key)

				# Confirm the secondary tables are required
				if secondary_join_by_columns:

					# Populate the primary lists
					join_table_list.append({primary_table:secondary_join_table_list}) 
					join_by_columns.append({self[primary_table].join_by_key:secondary_join_by_columns})

				else:

					# Populate the primary lists
					join_table_list.append(primary_table) 
					join_by_columns.append(self[primary_table].join_by_key)

		# Update log
		logging.info('Created table list and join-by column list from config file')

		return join_table_list, join_by_columns

	def returnColumnPathDict (self, column_dict):

		# Create a new assignment dict
		updated_dict = defaultdict(list)

		# Loop the keys of the dict
		for column_key, column_data in column_dict.items():

			# Check if the cuurent column is within the database
			if not self.hasColumn(column_key):

				# Print the error message
				raise Exception('Column (%s) not found' % column_key)

			# Assign the updated dict with the updated key
			updated_dict[self.returnColumnPath(column_key)] = column_data

		# Update log
		logging.info('Created column-path assignment dict')

		return updated_dict

	def returnColumnDict (self, column_dict, table):

		# Create a new assignment dict
		updated_dict = defaultdict(list)

		# Get database table
		db_table = self[table]

		# Loop the keys of the dict
		for column_key, column_data in column_dict.items():

			# Check if the table has the current column
			if column_key in db_table:

				# Assign the updated dict with the updated key
				updated_dict[column_key] = column_data

		# Update log
		logging.info('Created column assignment dict')

		return updated_dict

	def returnTables (self, columns):

		# Create a list to hold the tables
		tables = []

		# Loop the passed columns
		for column in columns:

			# Check if the cuurent column is within the database
			if not self.hasColumn(column):

				# Print the error message
				raise Exception('Column (%s) not found' % column_key)

			# Loop the tables in the config data
			for db_table in self:

				# Check if the table has the column
				if column in db_table:

					# Append the table
					tables.append(str(db_table))

					break

		# Remove duplicates
		tables = list(set(tables))

		# Update log
		logging.info('Created table list using column list')

		return tables

class DBTable (list):
	def __init__ (self, *arg, **kw):
		super(DBTable, self).__init__(*arg, **kw)
		self.name = ''
		self.primary_key_name = None
		self.join_by_key = None

	def __str__ (self):
		return self.name

	def __contains__ (self, column_str):
		if column_str in self.columns or column_str in self.unquoted_columns:
			return True
		else:
			return False

	def __getitem__(self, column_str):
		for column in self:
			if str(column) == column_str or column.unquoted_str == column_str:
				return column

	@property
	def primary_key_column (self):
		return self[self.primary_key_name]

	@property
	def columns (self):
		return [str(column) for column in self]

	@property
	def unquoted_columns (self):
		return [column.unquoted_str for column in self]

	@property
	def assignment_str (self):

		# Create the assignment str
		assign_str = ''

		# Loop the table by column
		for column in self:

			# Check if the assignment string is empty
			if assign_str:

				# Add a comma if not empty
				assign_str += ', '

			# Assign the column parameters and add to the tuple
			assign_str += column.assignment_str

		# Return the assignment str
		return assign_str

	def assignFromYaml (self, config_yaml):

		# Loop the config yaml
		for column, column_yaml in config_yaml.items():

			# Create a DBColumn object
			db_column = DBColumn()

			# Assign the name of the column
			db_column.name = column

			# Assign the path of the column
			db_column.path = '%s.%s' % (self.name, column)

			# Assign the column from the yaml
			db_column.assignFromYaml(column_yaml)

			# Check if the column is the primary key
			if db_column.primary_key == True:

				# Confirm there isn't already a primary key
				if self.primary_key_name:
					raise Exception('Only a single primary key may be assigned')

				# Assign the primary key column
				self.primary_key_name = str(db_column)

			# Check if the column is the primary key
			if db_column.join_by == True:

				# Confirm there isn't already a primary key
				if self.join_by_key:
					raise Exception('Only a join by column may be assigned')

				# Assign the primary key column
				self.join_by_key = str(db_column)

			# Save the column
			self.append(db_column)

	def retieveColumnPaths (self, keep_db_specific_columns = False):

		# Check if no columns are being removed
		if keep_db_specific_columns:

			# Return the symbol for all columns
			return ['%s.*' % self.name]

		# List of columns to be returned
		column_list = []

		# Loop the columns
		for column in self:

			# Check if: i) if the db column is kept, and if the column ia a db column
			if keep_db_specific_columns or not column.db_specific:
				
				# Add the columns to the list
				column_list.append(column.path)

		# Return the column list
		return column_list

class DBColumn ():
	def __init__ (self):
		self.name = ''
		self.path = ''
		self.type = None
		self.not_null = None
		self.primary_key = None
		self.db_specific = False
		self.join_by = False

	def __str__ (self):
		return self.name

	@property
	def unquoted_str (self):
		return self.name.replace('"','')

	@property
	def assignment_str (self):

		# Create string for column
		assign_str = '%s %s' % (self.name, self.type)

		# Check if the column is a primary key
		if self.primary_key == True:

			# Add the primary key statement to the string
			assign_str += ' PRIMARY KEY'

		# Check if the column is a primary key
		elif self.not_null == True:
			# Add the the not null statement to the string
			assign_str += ' NOT NULL'

		return assign_str

	def assignFromYaml (self, config_yaml):

		# Assign the data type
		self.type = config_yaml['type']

		# Loop the config yaml
		for column_arg, column_value in config_yaml.items():

			# Check for the primary key argument
			if column_arg == 'primary_key':

				# Assign the primary key argument
				self.primary_key = column_value

			# Check if the column should allow a null value
			if column_arg == 'not_null':

				# Assign the not null argument
				self.not_null = column_value

			# Check if the column is db specific
			if column_arg == 'db_specific':

				# Assing the db_specific value
				self.db_specific = True

			# Check if the column is db specific
			if column_arg == 'join_by':

				# Assing the db_specific value
				self.join_by = True

def orderedLoad (stream, Loader = yaml.Loader, object_pairs_hook = OrderedDict):
	class OrderedLoader (Loader):
		pass
	def construct_mapping (loader, node):
		loader.flatten_mapping(node)
		return object_pairs_hook(loader.construct_pairs(node))
	OrderedLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, construct_mapping)
	return yaml.load(stream, OrderedLoader)

def readConfig (filename):

	# Read in the YAML config file
	with open(filename, 'r') as yml_config_file:

		# Load the YAML config file in order
		config_yaml = orderedLoad(yml_config_file, yaml.SafeLoader)

		# Create a ConfigFile object
		config_file = ConfigFile()

		# Get filename and tables from the yaml file
		config_file.assignFromYaml(config_yaml)

		# Load the YAML config file in order
		return config_file

	# Update log
	logging.info('Config file (%s) loaded' % filename)
