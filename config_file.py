import yaml
from collections import OrderedDict, defaultdict

class ConfigFile (list):
	def __init__ (self, *arg, **kw):
		super(ConfigFile, self).__init__(*arg, **kw)
		self.sql_database = ''

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

	def returnJoinLists (self, tables_to_join):

		# Create a set of database tables that join the passed tables
		tables_joined_by = set()

		# Loop the tables that need to be joined
		for table_to_join in tables_to_join:

			# Create a list of database tables compatible with the current table
			table_compatible_with = []

			# Loop the tables in the database
			for table_in_db in self:

				# Check if the database table is compatible with the current table
				if self[table_to_join].join_by_key in table_in_db:

					# Add the database table
					table_compatible_with.append(str(table_in_db))

			# Check if the set is empty
			if not tables_joined_by:

				# Populate the empty set
				tables_joined_by = set(table_compatible_with)

			
			else:

				# Find the intersection if the set is already populated
				tables_joined_by = tables_joined_by & set(table_compatible_with)

		# Check if the passed tables join themselves
		if tables_joined_by & set(tables_to_join):

			# Find the intersection with the passed tables
			tables_joined_by = tables_joined_by & set(tables_to_join)

		# Assign the primary table to join using
		join_using = list(tables_joined_by)[0]

		# Create the lists to store the join data
		join_table_names = [join_using]
		join_table_keys = []

		# Loop the tables that join the passed tables
		for table_to_join in tables_to_join:

			# Don't repeat the primary table
			if table_to_join != join_using:

				# Populate the lists
				join_table_names.append(table_to_join) 
				join_table_keys.append(self[table_to_join].join_by_key)

		return join_table_names, join_table_keys

	def returnColumnPath (self, column):

		# Loop the tables in the config data
		for db_table in self:

			# Check if the table has an ID column assigned
			if column in db_table:

				# Return the species key
				return '%s.%s' % (str(db_table), column)

	def returnColumnPathDict (self, column_dict):

		# Create a new assignment dict
		updated_dict = defaultdict(list)

		# Loop the keys of the dict
		for column_key, column_data in column_dict.items():

			# Assign the updated dict with the updated key
			updated_dict[self.returnColumnPath(column_key)] = column_data

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

		return updated_dict

	def returnTables (self, columns):

		# Create a list to hold the tables
		tables = []

		# Loop the tables in the config data
		for db_table in self:

			# Loop the passed columns
			for column in columns:

				# Check if the table has the column
				if column in db_table:

					# Append the table
					tables.append(str(db_table))

					break

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
				column_list.append('%s.%s' % (self.name, column))

		# Return the column list
		return column_list

class DBColumn ():
	def __init__ (self):
		self.name = ''
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