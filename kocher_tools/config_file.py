import os
import yaml
import re

from collections import OrderedDict, defaultdict
from itertools import combinations

from sqlalchemy import Table, Column, ForeignKey, CheckConstraint, MetaData, Integer, Numeric, String, Text, LargeBinary, Date, DateTime, Boolean
from sqlalchemy.sql import func

class ConfigDB (list):
	def __init__ (self, yaml = ''):
		self.yaml = yaml
		self.yaml_dir = os.path.dirname(yaml)

		# Assign the DB properties
		self.type = None
		self.filename = None
		self.host = None
		self.user = None
		self.passwd = None
		self.database = None
		self.schema = None
		self.meta = None
		self._sql_tables = {}
		self._table_label_to_col = defaultdict(lambda: defaultdict(str))
		self._table_col_to_label = defaultdict(lambda: defaultdict(str))

		# Assign the Backup properties
		self.backup_dir = None
		self.log_file = None
		self.max_days_old = None
		self.max_files = None

		# Assign using the Yaml file
		self.assignFromYaml()

	@property
	def tables (self):
		return list(self._sql_tables)

	@property
	def sql_tables (self):
		return list(self._sql_tables.values())

	@property
	def sql_address (self):
		if not self.user or not self.passwd or not self.host or not self.database:
			sql_address_err_list = []
			if not self.user: sql_address_err_list.append('SQL Username')
			if not self.passwd: sql_address_err_list.append('SQL Password')
			if not self.host: sql_address_err_list.append('SQL Host')
			if not self.database: sql_address_err_list.append('SQL Database')
			raise Exception('Unable to assign SQL Address. Missing: %s.' % ', '.join(sql_address_err_list))
		return '%s:%s@%s/%s' % (self.user, self.passwd, self.host, self.database)

	@property
	def passwd_required (self):
		if self.type == 'sqlite': return False
		else: return True

	@property
	def can_backup (self):
		return self.backup_dir != None

	def __contains__ (self, table_str):
		if table_str in self._sql_tables:
			return True
		else:
			return False

	def __getitem__(self, table_str):
		return self._sql_tables[table_str]
	
	@classmethod
	def readConfig (cls, yaml_filename):

		# Create a ConfigDB object
		return cls(yaml = yaml_filename)

	def getSQLTables (self, table_strs):

		# Confirm the tables exists, and if so, return the SQL tables
		tables_to_return = []
		for table_str in table_strs:
			if table_str not in self: raise Exception('Table (%s) not found' % table_str)
			tables_to_return.append(self[table_str])
		return tables_to_return

	@staticmethod
	def getSQLColumn (table, column_str):

		try: sql_column = getattr(table, 'unique_id')
		except: raise Exception('Unable to assign column: %s' % column_str)

		return sql_column

	'''
	def getSQLColumns (self, column_strs):

		# Confirm the columns exists, and if so, return the SQL columns
		columns_to_return = []
		for column_str in column_strs:
			column_arg = None
			column_assigned = False
			for sql_table in self._sql_tables.values():
				try:
					test_arg = getattr(sql_table.columns, column_str)
					if len(test_arg.foreign_keys) >= 1:
						continue
					column_assigned = True
					column_arg = test_arg
				except: pass
			if not column_assigned: raise Exception('Unable to assign column: %s' % column_str)
			columns_to_return.append(column_arg)
		return columns_to_return
	'''

	def assignFromYaml (self):

		# Read in the YAML config file
		with open(self.yaml, 'r') as yml_config_file:

			# Load the YAML config file in order
			config_yaml = yaml.load(yml_config_file, Loader=yaml.FullLoader)

			# Assign the database information
			self.type = config_yaml['sql']['type']
			if self.type == 'sqlite':
				self.filename = config_yaml['sql']['filename']
			else:
				self.host = config_yaml['sql']['host']
				self.user = config_yaml['sql']['user']
				if 'database' in config_yaml['sql']: self.database = config_yaml['sql']['database']
				if 'schema' in config_yaml['sql']: self.schema = config_yaml['sql']['schema']
				if config_yaml['sql']['passwd']: self.passwd = config_yaml['sql']['passwd']

			if 'backup' in config_yaml:
				self.backup_dir = config_yaml['backup']['dir']
				if 'log' in config_yaml['backup']: self.log_file = config_yaml['backup']['log']
				if 'max_days_old' in config_yaml['backup']: self.max_days_old = int(config_yaml['backup']['max_days_old'])
				if 'max_files' in config_yaml['backup']: self.max_files = int(config_yaml['backup']['max_files'])

			# Create a metadata object
			if self.schema: meta = MetaData(schema = self.schema)
			else: meta = MetaData()

			# Loop the config yaml
			for table, column_yaml in config_yaml['database']['tables'].items():

				# Create the table attribute dict
				table_attr_list = []

				# Loop the config yaml
				for column, attribute_yaml in column_yaml.items():

					# Create a column arg list and kwarg dict
					col_arg_list = []
					col_kwarg_dict = {}

					# Assign a String-class arg, if found
					if 'String' in attribute_yaml['type']:
						if '(' in attribute_yaml['type'] and ')' in attribute_yaml['type']:
							type_value = int(attribute_yaml['type'].split('(')[1].split(')')[0])
							col_arg_list.append(String(type_value))
						else:
							col_arg_list.append(String)

					# Assign a Boolean-class arg, if found
					elif 'Boolean' in attribute_yaml['type']:
					
						col_arg_list.append(Boolean)
						
					# Assign a String-class arg, if found
					elif 'Text' in attribute_yaml['type']:
					
						col_arg_list.append(Text)

					# Assign a DateTime-class arg, if found
					elif 'DateTime' in attribute_yaml['type']:
						if 'default' in attribute_yaml or 'onupdate' in attribute_yaml: raise Exception('DateTime does not support default or onupdate options')
						if '(' in attribute_yaml['type'] and ')' in attribute_yaml['type']:
							if   '(record-created)': 
								col_arg_list.append(DateTime(timezone = True))
								col_kwarg_dict['default'] = func.now()
							elif '(record-updated)': 
								col_arg_list.append(DateTime(timezone = True))
								col_kwarg_dict['onupdate'] = func.now()
							elif    '(record-both)': 
								col_arg_list.append(DateTime(timezone = True))
								col_kwarg_dict['default'] = func.now()
								col_kwarg_dict['onupdate'] = func.now()
							else: raise Exception('Unable to assign DateTime subtype (%s)' % attribute_yaml['type'].split('(')[1].split(')')[0])
						else:
							col_arg_list.append(DateTime(timezone = True))

					# Assign a Date-class arg, if found
					elif 'Date' in attribute_yaml['type']:
						
						col_arg_list.append(Date)

					# Assign a Integer-class arg, if found
					elif 'Integer' in attribute_yaml['type']:
						
						col_arg_list.append(Integer)

					# Assign a Integer-class arg, if found
					elif 'Numeric' in attribute_yaml['type']:
						
						col_arg_list.append(Numeric)

					# Assign a Integer-class arg, if found
					elif 'Binary' in attribute_yaml['type']:
						
						col_arg_list.append(LargeBinary)

					else:
						if 'type' not in attribute_yaml: raise Exception('No type defined for column (%s)' % column)
						else: raise Exception('Unable to define type (%s) for column (%s)' % (attribute_yaml['type'], column))


					# Loop the optional column attributes
					for attribute_arg, attribute_value in attribute_yaml.items():

						# Assign as a primary_key, if found
						if attribute_arg == 'primary_key':
							col_kwarg_dict['primary_key'] = True

						# Assign as unique, if found
						if attribute_arg == 'unique':
							col_kwarg_dict['unique'] = True

						# Assign as not null, if found
						if attribute_arg == 'not_null':
							col_kwarg_dict['nullable'] = not attribute_value

						if attribute_arg == 'default':
							col_kwarg_dict['default'] = attribute_value

						if attribute_arg == 'onupdate':
							col_kwarg_dict['onupdate'] = attribute_value

						# Assign a ForeignKey arg, if found
						if attribute_arg == 'foreign_key':

							# Create foreign_key args
							fk_arg_list = []
							fk_kwarg_dict = {}

							for fk_arg, fk_value in attribute_value.items():
								if fk_arg == 'parent_key': fk_arg_list.append(fk_value)
								else: fk_kwarg_dict[fk_arg] = fk_value

							col_arg_list.append(ForeignKey(*fk_arg_list, **fk_kwarg_dict))

					# Add the column to the table attribute dict 
					sql_column = Column(column, *col_arg_list, **col_kwarg_dict)
					table_attr_list.append(sql_column)

					# Assign a CheckConstraint, if found, return error if constraint is unknown
					if 'constraint' in attribute_yaml:
						if attribute_yaml['constraint']['check'] == 'EQ':
							table_attr_list.append(CheckConstraint(sql_column == attribute_yaml['constraint']['value'], name = '%s_check' % column))
						elif attribute_yaml['constraint']['check'] == 'IN':
							table_attr_list.append(CheckConstraint(sql_column.in_(attribute_yaml['constraint']['value']), name = '%s_check' % column))
						elif attribute_yaml['constraint']['check'] == 'LT':
							table_attr_list.append(CheckConstraint(sql_column < attribute_yaml['constraint']['value'], name = '%s_check' % column))
						elif attribute_yaml['constraint']['check'] == 'GT':
							table_attr_list.append(CheckConstraint(sql_column > attribute_yaml['constraint']['value'], name = '%s_check' % column))
						elif attribute_yaml['constraint']['check'] == 'BOOL':
							table_attr_list.append(CheckConstraint(sql_column.in_([0, 1]), name = '%s_check' % column))
						elif attribute_yaml['constraint']['check'] == 'TF_1':
							table_attr_list.append(CheckConstraint(sql_column.in_(['T', 'F']), name = '%s_check' % column))
						elif attribute_yaml['constraint']['check'] == 'TF_Text':
							table_attr_list.append(CheckConstraint(sql_column.in_(['True', 'False']), name = '%s_check' % column))
						elif attribute_yaml['constraint']['check'] == 'YN':
							table_attr_list.append(CheckConstraint(sql_column.in_(['Y', 'N']), name = '%s_check' % column))
						elif attribute_yaml['constraint']['check'] == 'YN_NS':
							table_attr_list.append(CheckConstraint(sql_column.in_(['Y', 'N', 'NS']), name = '%s_check' % column))
						elif attribute_yaml['constraint']['check'] == 'YesNo':
							table_attr_list.append(CheckConstraint(sql_column.in_(['Yes', 'No']), name = '%s_check' % column))
						elif attribute_yaml['constraint']['check'] == 'YesNo_NS':
							table_attr_list.append(CheckConstraint(sql_column.in_(['Yes', 'No', 'NS']), name = '%s_check' % column))
						else: raise Exception('Unknown constraint (%s)' % attribute_yaml['constraint']['check'])

					# Assign a column label, if found
					if 'label' in attribute_yaml:
						self._table_label_to_col[table][attribute_yaml['label']] = column
						self._table_col_to_label[table][column] = attribute_yaml['label']

				# Assign the table class
				self._sql_tables[table] = Table(table, meta, *table_attr_list)

			self.meta = meta

	def webYaml (self):

		# Read in the YAML config file
		with open(self.yaml, 'r') as yml_config_file:

			# Load the YAML config file in order
			config_yaml = yaml.load(yml_config_file, Loader=yaml.FullLoader)

			# Create the yaml output dict and string
			yaml_output_dict = {}
			yaml_output_str = ''

			# Loop the config yaml
			for table, column_yaml in config_yaml['database']['tables'].items():

				# Populate the yaml output dict with the current table
				
				yaml_output_dict['form'] = f'{table}'
				yaml_output_dict['form_label'] = f'{table.capitalize()}'
				yaml_output_dict['db_schema'] = 'dash'
				yaml_output_dict['db_table'] = f'{table}'

				# Create the fields dict
				fields_dict = []

				# Loop the config yaml
				for column, attribute_yaml in column_yaml.items():

					# Create and populate the column field dict
					col_field_dict = {}

					col_field_dict['field'] = column
					col_field_dict['field_label'] =  attribute_yaml['label']

					# Assign a String-class arg, if found
					if 'String' in attribute_yaml['type']:

						col_field_dict['data_type'] = 'string'
						col_field_dict['data_class'] = 'text'
	
					# Assign a Boolean-class arg, if found
					elif 'Boolean' in attribute_yaml['type']:
						
						col_field_dict['data_type'] = 'boolean'
						col_field_dict['data_class'] = 'select'

					# Assign a String-class arg, if found
					elif 'Text' in attribute_yaml['type']:
					
						col_field_dict['data_type'] = 'string'
						col_field_dict['data_class'] = 'textarea'

					# Assign a Date-class arg or DateTime-class arg, if found
					elif 'DateTime' in attribute_yaml['type'] or 'Date' in attribute_yaml['type']:

						col_field_dict['data_type'] = 'string'
						col_field_dict['data_class'] = 'date'

					# Assign a Integer-class arg, if found
					elif 'Integer' in attribute_yaml['type']:
						
						col_field_dict['data_type'] = 'integer'
						col_field_dict['data_class'] = 'integer'

					# Assign a Integer-class arg, if found
					elif 'Numeric' in attribute_yaml['type']:
						
						col_field_dict['data_type'] = 'float'
						col_field_dict['data_class'] = 'real'

					else:
						if 'type' not in attribute_yaml: raise Exception('No type defined for column (%s)' % column)
						else: raise Exception('Unable to define type (%s) for column (%s)' % (attribute_yaml['type'], column))

					# Check if the column is the primary key
					if 'primary_key' in attribute_yaml:
						col_field_dict['primary_key'] = True

					# Add the column fields
					fields_dict.append(col_field_dict)

				# Add the fields to the yaml table dict
				yaml_output_dict['fields'] = fields_dict

				# Add to the yaml output string
				if yaml_output_str: yaml_output_str += '\n'
				yaml_output_str += yaml.dump(yaml_output_dict, explicit_start = True, sort_keys = False, indent = 4)

			# Modify the yaml output string
			yaml_output_str = re.sub(r'([a-z])\n\-  ', r'\1\n\n-  ', yaml_output_str)
			yaml_output_str = yaml_output_str.replace('-   ', '  - ')
			return yaml_output_str

	def webYamlFile (self, yaml_filename):
		if not yaml_filename: raise Exception('No filename specified')

		web_yaml_file = open(yaml_filename, 'w')
		web_yaml_file.write(self.webYaml())
		web_yaml_file.close()

	def webValueYaml (self):

		# Read in the YAML config file
		with open(self.yaml, 'r') as yml_config_file:

			# Load the YAML config file in order
			config_yaml = yaml.load(yml_config_file, Loader=yaml.FullLoader)

			# Create list to store previous value lists, to avoid repeats
			valuelist_list = []

			# Create a valuelist counter
			valuelist_counter = 1

			# Create the yaml out string
			yaml_output_str = ''

			# Loop the config yaml
			for table, column_yaml in config_yaml['database']['tables'].items():
				
				# Loop the config yaml
				for column, attribute_yaml in column_yaml.items():

					# Create the yaml output dict
					yaml_output_dict = {}
					
					# Check if the column has a constraint
					if 'constraint' in attribute_yaml:
						if 'value' in attribute_yaml['constraint']:

							constraint_type = attribute_yaml['constraint']['check']
							value_list = attribute_yaml['constraint']['value']

							# Create and populate the constraint options dict
							constraint_options_dict = {}
							constraint_options_dict['opt_type'] = 'array'
							if constraint_type == 'IN': pass
							else: raise Exception(attribute_yaml['constraint'])

							constraint_options_dict['opt_values'] = value_list
							yaml_output_dict['valuelist'] = f'{constraint_type}_{valuelist_counter}'
							yaml_output_dict['options'] = constraint_options_dict

							yaml_output_str +=  yaml.dump(yaml_output_dict, explicit_start = True, sort_keys = False, indent = 4, )
							yaml_output_str = yaml_output_str.replace("'", '')
							valuelist_list.append(value_list)
							valuelist_counter += 1

						else:
							constraint_type = attribute_yaml['constraint']['check']
							
							# Create and populate the constraint options dict
							constraint_options_dict = {}
							constraint_options_dict['opt_type'] = 'array'
							if constraint_type == 'YesNo_NS': value_list = ['Yes', 'No', 'NS']
							elif constraint_type == 'YesNo': value_list = ['Yes', 'No']
							else: raise Exception(constraint_type)
							constraint_options_dict['opt_values'] = value_list
							yaml_output_dict['valuelist'] = f'{constraint_type}'
							yaml_output_dict['options'] = constraint_options_dict

							yaml_output_str +=  yaml.dump(yaml_output_dict, explicit_start = True, sort_keys = False, indent = 4, )
							yaml_output_str = yaml_output_str.replace("'", '')
							valuelist_list.append(constraint_type)

			return yaml_output_str
