import os
import sys
#import sqlite3
#import datetime
#import pytz
#import copy
import itertools
import operator
import csv
import zipfile
import tempfile
import logging

import pandas as pd

from sqlalchemy import inspect
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select, insert
from sqlalchemy.schema import CreateTable

from sqlalchemy.dialects.postgresql import insert as postgresql_insert

def createEngineFromFilename (sql_filename, echo = False):

	return create_engine("sqlite:///%s" % sql_filename, echo = echo)

def createEngineFromAddress (sql_address, echo = False):

	return create_engine("postgresql://%s" % sql_address, echo = echo)

def createEngineFromConfig (config_data, echo = False):

	if config_data.type == 'sqlite': return create_engine("sqlite:///%s" % config_data.filename, echo = echo)
	else: return create_engine("postgresql://%s" % config_data.sql_address, echo = echo)

def startSessionFromConfig (config_data, echo = False):

	sql_engine = createEngineFromConfig(config_data, echo)
	Session = sessionmaker(bind=sql_engine)
	return Session()
	
def startSessionFromEngine (engine):
	
	# Start the session
	Session = sessionmaker(bind=engine)
	return Session()

def createAllFromConfig (config_data, engine):

	# Create the tables
	config_data.meta.create_all(engine)

def printCreateSQL (sql_tables):

	# Loop tables and print SQL statments
	for sql_table in sql_tables:
		print(CreateTable(sql_table))

def foreignKeyPairs (sql_tables):

	# Assign the pairs of foreign and parent keys
	for fk_sql_table in sql_tables:
		for column in fk_sql_table.columns:
			if column.foreign_keys:
				for foreign_key in column.foreign_keys:
					yield foreign_key.column, column

def prepDataFrameUsingConfig (config_data, table, dataframe):

	# Confirm the table is within the config file
	if not table in config_data: raise Exception(f'{table} not found in database')

	col_header_dict = config_data._table_col_to_label[table]
	col_label_dict = config_data._table_label_to_col[table]

	# Check which header values were found
	col_header_count = len(set(dataframe.columns) & set(list(col_header_dict)))
	col_label_count = len(set(dataframe.columns) & set(list(col_label_dict)))

	# Fix the headers, if necessary 
	header_method = 'header' if col_header_count > col_label_count else 'label'
	if header_method == 'label': dataframe = dataframe.rename(columns = col_label_dict)

	# Define the standard columns
	std_cols = list(col_header_dict)

	# Check if the file has non-standard columns
	non_std_cols = list(set(dataframe.columns) - set(std_cols))
	dataframe = dataframe.drop(columns = non_std_cols)

	# Return the dataframe
	return dataframe

class SQLSelect ():
	def __init__ (self, sql_select_tables = [], sql_select_columns = [], sql_where = [], sql_tables = [], tables_in_select = [], sql_connection = None):
		self.select_results = None
		self._sql_select_tables = sql_select_tables
		self._sql_select_columns = sql_select_columns
		self._sql_where = sql_where
		self._sql_tables = sql_tables
		self._tables_in_select = tables_in_select 
		self._sql_connection = sql_connection

	@property
	def _total_tables_in_select (self):

		return sum(self._tables_in_select)

	@property
	def _sql_tables_in_select (self):

		return [sql_table for table_bool, sql_table in zip(self._tables_in_select, self._sql_tables) if table_bool]

	def select (self):
		if self._total_tables_in_select > 1:
			for parent_key, foreign_key in foreignKeyPairs(self._sql_tables_in_select):
				if not self._sql_where:
					self._sql_where = SQLWhere.fromQuery(self._sql_tables)
				self._sql_where.addEQColWhere(parent_key, foreign_key)
		self.select_results = list(self._sql_connection.execute(select(self.removeRepeatsInSelect(self._sql_select_tables, self._sql_select_columns)).where(self._sql_where.where_statement)))

	def toDataFrame (self):

		# Return as dataframe
		return(pd.DataFrame([dict(_r) for _r in self.select_results]))

	def toFile (self, out_filename, sep, warn_if_nothing = True):

		# Check if any data was returned, if not return nothing
		if len(self.select_results) == 0:
			if warn_if_nothing: logging.warning(f'Nothing to return. File ({out_filename}) not created')
			else: logging.info(f'Nothing to return.')
			return

		# Create the output file, using DictWriter
		header = self.select_results[0].keys()
		with open(out_filename, 'w') as entries_file:
			entries_writer = csv.DictWriter(entries_file, fieldnames = header, delimiter = sep)
			entries_writer.writeheader()
			for results_row in self.select_results:
				entries_writer.writerow(dict(results_row))

		# Update log
		logging.info('Retrieved results written to file (%s)' % out_filename)

	def toScreen (self, sep, warn_if_nothing = True):

		# Check if any data was returned, if not return nothing
		if len(self.select_results) == 0:
			if warn_if_nothing: logging.warning(f'Nothing to return.')
			else: logging.info(f'Nothing to return.')
			return

		# Print the header using sep
		header = self.select_results[0].keys()
		print(sep.join(header))

		# Print the values to stdout, seperated by sep
		for results_row in self.select_results:
			print(sep.join(map(str, results_row.values())))

		# Update log
		logging.info('Retrieved results sent to stdout')

	@classmethod
	def fromConfig (cls, config_data, sql_connection):

		# Create a SQLQuery object from the config file and sql_connection
		return cls(sql_select_tables = [], 
				   sql_select_columns = [], 
				   sql_where = [], 
				   sql_tables = config_data.sql_tables, 
				   tables_in_select = [False] * len(config_data.sql_tables), 
				   sql_connection = sql_connection)

	def addTableToSelect (self, table):

		# Add the table, update the tables in use
		for table_pos, sql_table in enumerate(self._sql_tables):
			if table == sql_table:
				self._tables_in_select[table_pos] = True
		if table in self._sql_select_tables: raise Exception('Table (%s) already assigned' % table)
		self._sql_select_tables.append(table)

	def addTablesToSelect (self, tables):

		# Add the tables, update the tables in use
		for table in tables:
			for table_pos, sql_table in enumerate(self._sql_tables):
				if table == sql_table:
					self._tables_in_select[table_pos] = True
			if table in self._sql_select_tables: raise Exception('Table (%s) already assigned' % table)
			self._sql_select_tables.append(table)

	def addColumnToSelect (self, column):

		# Add the column, update the tables in use
		for table_pos, sql_table in enumerate(self._sql_tables):
			for sql_column in sql_table.columns:
				if column == sql_column:
					self._tables_in_select[table_pos] = True
		if column in self._sql_select_columns: raise Exception('Column (%s) already assigned' % column)
		self._sql_select_columns.append(column)

	def addColumnsToSelect (self, columns):

		# Add the columns, update the tables in use
		for column in columns:
			for table_pos, sql_table in enumerate(self._sql_tables):
				for sql_column in sql_table.columns:
					if column == sql_column:
						self._tables_in_select[table_pos] = True
			if column in self._sql_select_columns: raise Exception('Column (%s) already assigned' % column)
			self._sql_select_columns.append(column)

	def addDictWhere (self, where_dict, include = None, cmp_type = None, dict_type = None):

		if not include: raise Exception('Must indicate include/exclude type for where statement')
		if not cmp_type: raise Exception('Must indicate comparison type for where statement')
		if not dict_type: raise Exception('Must indicate dict type for where statement')

		# Add the where statement from a dict, update the tables in use
		if not self._sql_where:
			self._sql_where = SQLWhere.fromQuery(self._sql_tables)
		if dict_type == 'Column': self._sql_where.addColDictWhere(where_dict, include = include, cmp_type = cmp_type)
		elif dict_type == 'Table': self._sql_where.addTableDictWhere(where_dict, include = include, cmp_type = cmp_type)
		for table_pos in self._sql_where._tables_used:
			if not self._tables_in_select[table_pos]:
				self._tables_in_select[table_pos] = True

	@staticmethod
	def removeRepeatsInSelect (sql_tables, sql_columns):

		remove_dict = {}
		for sql_table in sql_tables:
			for column in sql_table.columns:
				if column.foreign_keys:
					for foreign_key in column.foreign_keys:
						if foreign_key.column.table in sql_tables:
							remove_dict[sql_table] = column

		updated_tables = []
		updated_columns = []
		for sql_table in sql_tables:
			if sql_table not in remove_dict:
				updated_tables.append(sql_table)
			else:
				for column in sql_table.columns:
					if column != remove_dict[sql_table]:
						updated_columns.append(column)

		all_columns = list(itertools.chain.from_iterable([list(updated_table.columns) for updated_table in updated_tables])) + updated_columns
		for sql_column in sql_columns:
			if sql_column not in all_columns:
				updated_columns.append(sql_column)

		return updated_tables + updated_columns

class SQLUpdate ():
	def __init__ (self, sql_update = None, sql_values = {}, sql_where = [], sql_tables = [], tables_in_update = [], sql_connection = None):
		self._sql_update = sql_update
		self._sql_values = sql_values
		self._sql_where = sql_where
		self._sql_tables = sql_tables
		self._tables_in_update = tables_in_update 
		self._sql_connection = sql_connection

	@property
	def _total_tables_in_update (self):
		return sum(self._tables_in_update)

	@property
	def _sql_tables_in_update (self):
		return [sql_table for table_bool, sql_table in zip(self._tables_in_update, self._sql_tables) if table_bool]

	def update (self):

		# Update the values
		if self._sql_where:
			if self._total_tables_in_update > 1:
				for parent_key, foreign_key in foreignKeyPairs(self._sql_tables_in_update):
					if not self._sql_where:
						self._sql_where = SQLWhere.fromQuery(self._sql_tables)
					self._sql_where.addEQColWhere(parent_key, foreign_key)
			self._sql_connection.execute(self._sql_update.update().values(**self._sql_values).where(self._sql_where.where_statement))
		else:
			self._sql_connection.execute(self._sql_update.update().values(**self._sql_values))

	@classmethod
	def fromConfig (cls, config_data, sql_connection):

		# Create a SQLQuery object from the config file and sql_connection
		return cls(sql_update = None, 
				   sql_values = {}, 
				   sql_where = [], 
				   sql_tables = config_data.sql_tables, 
				   tables_in_update = [False] * len(config_data.sql_tables), 
				   sql_connection = sql_connection)

	def addTableToUpdate(self, table):

		# Add the table, update the tables in use
		for table_pos, sql_table in enumerate(self._sql_tables):
			if table == sql_table:
				self._tables_in_update[table_pos] = True
		if self._sql_update: raise Exception('Table (%s) already assigned' % self._sql_update)
		self._sql_update = table

	def addDictValues(self, col_dict):

		# Add the update from a dict, update the tables in use
		sql_values = SQLValues.fromColDictValues(self._sql_update, col_dict)
		self._sql_values = sql_values

	'''
	def addDictWhere(self, where_dict, include = None, type = None, table_dict = False):

		# Add the where statement from a dict, update the tables in use
		if not self._sql_where:
			self._sql_where = SQLWhere.fromQuery(self._sql_tables)
		self._sql_where.addEQColDictWhere(where_dict)
		self._sql_where.addTableDictWhere(where_dict)
		for table_pos in self._sql_where._tables_used:
			if not self._tables_in_update[table_pos]:
				self._tables_in_update[table_pos] = True
	'''

	def addDictWhere (self, where_dict, include = None, cmp_type = None, dict_type = None):

		if not include: raise Exception('Must indicate include/exclude type for where statement')
		if not cmp_type: raise Exception('Must indicate comparison type for where statement')
		if not dict_type: raise Exception('Must indicate dict type for where statement')

		# Add the where statement from a dict, update the tables in use
		if not self._sql_where:
			self._sql_where = SQLWhere.fromQuery(self._sql_tables)
		if dict_type == 'Column': self._sql_where.addColDictWhere(where_dict, include = include, cmp_type = cmp_type)
		elif dict_type == 'Table': self._sql_where.addTableDictWhere(where_dict, include = include, cmp_type = cmp_type)
		for table_pos in self._sql_where._tables_used:
			if not self._tables_in_update[table_pos]:
				self._tables_in_update[table_pos] = True

class SQLInsert ():
	def __init__ (self, sql_insert = None, sql_values = [], sql_tables = [], tables_in_insert = [], sql_ignore_constraint = '', sql_update_constraint = '', sql_connection = None, sql_type = ''):
		self._sql_insert = sql_insert
		self._sql_values = sql_values
		self._sql_tables = sql_tables
		self._tables_in_insert = tables_in_insert
		self._sql_ignore_constraint = sql_ignore_constraint
		self._sql_connection = sql_connection
		self._sql_type = sql_type

	def insert (self):

		self._sql_connection.execute(insert(self._sql_insert).values(self._sql_values))

	def insertIgnore (self):

		if self._sql_type == 'postgresql': 
			self._sql_connection.execute(postgresql_insert(self._sql_insert).values(self._sql_values).on_conflict_do_nothing(constraint = self._sql_ignore_constraint))
		else: 
			self._sql_connection.execute(insert(self._sql_insert).values(self._sql_values).prefix_with('OR IGNORE'))

	@classmethod
	def fromConfig (cls, config_data, sql_connection):

		# Create a SQLQuery object from the config file and sql_connection
		return cls(sql_insert = None, 
				   sql_values = [], 
				   sql_tables = config_data.sql_tables, 
				   tables_in_insert = [False] * len(config_data.sql_tables),
				   sql_connection = sql_connection,
				   sql_type = config_data.type)

	def addIgnore (self, col_name):

		try: getattr(self._sql_insert.columns, col_name)
		except: raise Exception(f'Unable to assign column ({col_name})')
		if self._sql_ignore_constraint: raise Exception('Append string already defined')

		# Assign the schema basename (no reference to database)
		schema_basename = str(self._sql_insert)
		schema_basename = schema_basename if '.' not in schema_basename else schema_basename.split('.')[-1]
		self._sql_ignore_constraint = f'{schema_basename}_{col_name}_key'

	def addTableToInsert (self, table):

		# Add the table, update the tables in use
		for table_pos, sql_table in enumerate(self._sql_tables):
			if table == sql_table:
				self._tables_in_insert[table_pos] = True
		if self._sql_insert: raise Exception('Table (%s) already assigned' % self._sql_insert)

		self._sql_insert = table

	def addDictValues (self, col_dict):
		
		# Confirm the data is within a list
		if not isinstance(col_dict, dict): raise Exception('Values must be stored within a dict')

		# Add the insert from a dict
		sql_values = SQLValues.fromColDictValues(self._sql_insert, col_dict)
		self._sql_values.append(sql_values)

	def addListValues (self, col_dict_list):
		
		# Confirm the data is within a list
		if not isinstance(col_dict_list, list): raise Exception('Values must be stored within a List')

		# Add the insert from a list of dict
		for col_dict in col_dict_list:
			self.addDictValues(col_dict)

	def addDataFrameValues (self, dataframe):

		# Convert the dataframe
		col_dict_list = dataframe.to_dict('records')

		# Add the values
		self.addListValues(col_dict_list)
		
class SQLValues (dict):
	def __init__(self, *arg, **kw):
		super(SQLValues, self).__init__(*arg, **kw)

	@classmethod
	def fromColDictValues (cls, sql_table, col_dict):

		# Add the column-based dict values
		for col_name, col_value in col_dict.items():
			try: 
				getattr(sql_table.columns, col_name)
			except: raise Exception('Unable to assign value: %s: %s' % (col_name, col_value))
		# Create a SQLValues object
		return cls(col_dict)

class SQLWhere ():
	def __init__ (self, sql_tables = [], tables_in_where = []):
		self._sql_where = []
		self._tables_in_where = tables_in_where
		self._sql_tables = sql_tables

	@property
	def where_statement(self):
		if len(self._sql_where) == 1: return self._sql_where[0]
		else: return operator.and_(*self._sql_where)

	@property
	def _tables_used (self):
		return [table_pos for table_pos, table_bool in enumerate(self._tables_in_where) if table_bool]

	def __contains__ (self, new_where):

		# Check if the object contains the where statement
		for sql_where in self._sql_where:
			if new_where.compare(sql_where): return True
		else: return False

	def addTableDictWhere (self, table_dict, include, cmp_type):

		table_dict_pos, table_dict_arg = None, None
		for table_name, col_dict in table_dict.items():
			for table_pos, sql_table in enumerate(self._sql_tables):
				if str(sql_table) == table_name: 
					table_dict_arg = sql_table
					table_dict_pos = table_pos
			if table_dict_pos == None: raise Exception('Unable to assign table: %s' % table_name)
			if not self._tables_in_where[table_dict_pos]: self._tables_in_where[table_dict_pos] = True
			if cmp_type == 'EQ': self.addEQTableDictWhere(table_dict_arg, col_dict, include)
			elif cmp_type == 'IN': self.addINTableDictWhere(table_dict_arg, col_dict, include)
			elif cmp_type == 'LIKE': self.addLIKETableDictWhere(table_dict_arg, col_dict, include)

	def addEQTableDictWhere (self, sql_table, col_dict, include):

		# Add the table-based dict where
		for col_name, col_value in col_dict.items():
			where_arg = None
			where_passed = False
			try:
				test_arg = getattr(sql_table.columns, col_name)
				if len(test_arg.foreign_keys) >= 1:
					continue
				where_arg = test_arg
				where_passed = True
			except: pass
			if not where_passed: raise Exception('Unable to assign where: %s: %s' % (col_name, col_value))
			if include: self._sql_where.append(where_arg == col_value)
			else: self._sql_where.append(where_arg != col_value)

	def addLIKETableDictWhere (self, sql_table, col_dict, include):

		# Add the table-based dict where
		for col_name, col_value in col_dict.items():
			where_arg = None
			where_passed = False
			try:
				test_arg = getattr(sql_table.columns, col_name)
				if len(test_arg.foreign_keys) >= 1:
					continue
				where_arg = test_arg
				where_passed = True
			except: pass
			if not where_passed: raise Exception('Unable to assign where: %s: %s' % (col_name, col_value))
			if include: self._sql_where.append(where_arg.like(col_value))
			else: self._sql_where.append(where_arg.notilike(col_value))

	def addINTableDictWhere (self, sql_table, col_dict, include):

		# Add the table-based dict where
		for col_name, col_values in col_dict.items():
			if not isinstance(col_values, list): Exception('Column values are not list')
			where_arg = None
			where_passed = False
			try:
				test_arg = getattr(sql_table.columns, col_name)
				if len(test_arg.foreign_keys) >= 1:
					continue
				where_arg = test_arg
				where_passed = True
			except: pass
			if not where_passed: raise Exception('Unable to assign where: %s: %s' % (col_name, col_values))
			if include: self._sql_where.append(where_arg.in_(col_values))
			else: self._sql_where.append(where_arg.notin_(col_values))

	def addColDictWhere (self, col_dict, include, cmp_type):

		if cmp_type == 'EQ': self.addEQColDictWhere(col_dict, include)
		elif cmp_type == 'IN': self.addINColDictWhere(col_dict, include)
		elif cmp_type == 'LIKE': self.addLIKEColDictWhere(col_dict, include)


	def addEQColDictWhere (self, col_dict, include):

		# Add the column-based dict where
		for col_name, col_value in col_dict.items():
			where_pos, where_arg = None, None
			for table_pos, sql_table in enumerate(self._sql_tables):
				try:
					test_arg = getattr(sql_table.columns, col_name)
					if len(test_arg.foreign_keys) >= 1:
						continue
					where_arg = test_arg
					where_pos = table_pos
				except: pass
			if where_pos == None: raise Exception('Unable to assign where: %s: %s' % (col_name, col_value))
			if not self._tables_in_where[where_pos]: self._tables_in_where[where_pos] = True
			if include: self._sql_where.append(where_arg == col_value)
			else: self._sql_where.append(where_arg != col_value)

	def addLIKEColDictWhere (self, col_dict, include):

		# Add the column-based dict where
		for col_name, col_value in col_dict.items():
			where_pos, where_arg = None, None
			for table_pos, sql_table in enumerate(self._sql_tables):
				try:
					test_arg = getattr(sql_table.columns, col_name)
					if len(test_arg.foreign_keys) >= 1:
						continue
					where_arg = test_arg
					where_pos = table_pos
				except: pass
			if where_pos == None: raise Exception('Unable to assign where: %s: %s' % (col_name, col_value))
			if not self._tables_in_where[where_pos]: self._tables_in_where[where_pos] = True
			if include: self._sql_where.append(where_arg.like(col_value))
			else: self._sql_where.append(where_arg.notilike(col_value))

	def addINColDictWhere (self, col_dict, include):

		# Add the column-based dict where
		for col_name, col_values in col_dict.items():
			if not isinstance(col_values, list): Exception('Column values are not list')
			where_pos, where_arg = None, None
			for table_pos, sql_table in enumerate(self._sql_tables):
				try:
					test_arg = getattr(sql_table.columns, col_name)
					if len(test_arg.foreign_keys) >= 1:
						continue
					where_arg = test_arg
					where_pos = table_pos
				except: pass

			if where_pos == None: raise Exception('Unable to assign where: %s: %s' % (col_name, col_values))
			if not self._tables_in_where[where_pos]: self._tables_in_where[where_pos] = True
			if include: self._sql_where.append(where_arg.in_(col_values))
			else: self._sql_where.append(where_arg.notin_(col_values))
			
	def addEQColWhere (self, col1, col2):

		# Add the column eq were (i.e. col1 == col2)
		new_where = (col1 == col2)
		if new_where not in self._sql_where:
			self._sql_where.append(new_where)
		else:
			logging.warning('Where (%s) already assigned' % str(new_where))

	@classmethod
	def fromConfig (cls, config_data):

		# Create a SQLWhere object
		return cls(sql_tables = config_data.sql_tables, tables_in_where = [False] * len(config_data.sql_tables))

	@classmethod
	def fromQuery (cls, sql_tables):

		# Create a SQLWhere object
		return cls(sql_tables = sql_tables, 
				   tables_in_where = [False] * len(sql_tables))

'''
def currentTime(timezone = 'US/Eastern'):

	# Set the time format for the logging system
	time_format = '%Y-%m-%d %H:%M:%S %Z'

	# Set the timezone
	specified_timezone = pytz.timezone(timezone)

	# Set the current time
	current_time = datetime.datetime.now(specified_timezone)
		
	# Return string formatted time
	return current_time.strftime(time_format)

def valueMarksStr (values):

	# Determine the number of values
	value_count = len(values)

	# Return the question mark string
	return ', '.join(['?'] * value_count)

def quoteStr (str_to_quote):

	# Create list of characters that require quotes
	quote_chars = [' ', '-']

	try:
		# Check if any of the quote characters are within the string
		if any(quote_char in str_to_quote for quote_char in quote_chars):

			# Confirm the string isn't already quoted
			if str_to_quote[0] != '"' and str_to_quote[-1] != '"': 

				# Update the string with quotes
				str_to_quote = '"%s"' % str_to_quote

	except:

		pass
		
	# Return the string
	return str_to_quote

def quoteField (field_str, split_by_dot = True):

	# Check if the field should be split by a dot '.' symbol
	if split_by_dot:

		# Create a string to rebuild the split items
		joined_str = ''

		# Split the string by a dot '.' symbol
		for field_sub_str in field_str.split('.', 1):

			# Check if the join string is empty
			if joined_str:

				# Add a dot '.' symbol
				joined_str += '.'

			# Quote the entire string
			joined_str += quoteStr(field_sub_str)

		# Return the joined string
		return joined_str

	else:

		# Quote the entire string
		field_str = quoteStr(field_str)

		# Return the field string
		return field_str

def quoteFields (field_list, split_by_dot = True):

	# Loop the list by index
	for pos in range(len(field_list)):

		# Quote the current item in the list
		field_list[pos] = quoteField(field_list[pos], split_by_dot = split_by_dot)

	# Return the quoted list
	return field_list

def returnSetExpression (set_dict):

	# Create an empty string to hold the set expression
	set_expression_str = ''

	# Loop the set dict
	for column in set_dict.keys():

		# Check if the set expression is not empty
		if set_expression_str:

			# Add a comma
			set_expression_str += ', '

		set_expression_str += '%s = ?' % quoteField(column)

	# Return the set expression
	return set_expression_str

def returnSelectionDict (selection_dict):

	# Create the selection string
	selection_str = ''

	# Loop the dict by operator
	for selection_operator, selection_data in selection_dict.items():

		# Loop the selection dict
		for column, value_list in selection_data.items():

			# Check that the selection string isn't empty
			if selection_str:

				# Add the AND operator between each statement
				selection_str += ' AND '

			# Check if the selection operator is either IN or NOT IN
			if 'IN' in selection_operator:
					
				# Add the quoted column, the selection operator, and the question marks to the string
				selection_str += '%s %s (%s)' % (quoteField(column), selection_operator, valueMarksStr(value_list))

			# Check if the selection operator is either LIKE or NOT LIKE
			elif 'LIKE' in selection_operator:

				# Create a selection substring
				selection_sub_str = ''

				# Loop the selecion data
				for value_item in value_list:

					# Check that the selection string isn't empty
					if selection_sub_str:

						# Add the AND operator between each statement
						selection_sub_str += ' OR '

					# Add the quoted column and the selection operator
					selection_sub_str += '%s %s ?' % (quoteField(column), selection_operator)

				# Check if the sub string should be within parentheses
				if len(value_list) > 1:

					# Enclose the LIKE statements
					selection_sub_str = '(%s)' % selection_sub_str

				# Add the substring to the string
				selection_str += selection_sub_str

	# Update log
	logging.info('Created selection statement for database call')

	return selection_str

def returnSelectionValues (selection_dict):

	# Create an empty list to hold the values for the selection expression
	expression_statement_values = []

	# Loop the dict by operator
	for selection_operator, selection_data in selection_dict.items():

		# Check if the selection operator is either IN or NOT IN
		if 'IN' in selection_operator:

			# Add the include expression values to the list
			expression_statement_values.extend(itertools.chain.from_iterable(list(selection_data.values())))

		# Check if the selection operator is either LIKE or NOT LIKE
		elif 'LIKE' in selection_operator:

			# Loop each value in the selection dict
			for selection_value in itertools.chain.from_iterable(list(selection_data.values())):

				# Add the include expression value with wildcards to the list
				expression_statement_values.append('%' + selection_value + '%')

	# Quote the values
	expression_statement_values = quoteFields(expression_statement_values)

	# Update log
	logging.info('Successfully created value list for selection statement')

	# Return the list
	return expression_statement_values

def innerJoinTables (table_list, join_column_list):

	# Add the first table to the string
	inner_join_str = table_list[0]

	# Loop the tables, skipping the first entry
	for table_name, table_join_column in zip(table_list[1:], join_column_list):

		# Confirm the data is a linking table
		if isinstance(table_name, dict):

			# Assign the linking table and join column
			for link_table_name, link_table_join_column in zip(table_name.keys(), table_join_column.keys()):

				# Assign the sub table data
				sub_table_list = table_name[link_table_name]
				sub_join_column_list = table_join_column[link_table_join_column]

				# Add the first table to the string
				sub_inner_join_str = sub_table_list[0]

				# Loop the tables, skipping the first entry
				for sub_table_name, sub_table_join_column in zip(sub_table_list[1:], sub_join_column_list):

					# Add the sub inner join string
					sub_inner_join_str += ' INNER JOIN {0} ON {1}.{2} = {0}.{2}'.format(sub_table_name, sub_table_list[0], quoteField(sub_table_join_column))

				# Add the inner join string
				inner_join_str +=  ' INNER JOIN ({0}) {1} ON {2}.{3} = {1}.{3}'.format(sub_inner_join_str, link_table_name, table_list[0], quoteField(link_table_join_column))
		else:

			# Add the inner join string
			inner_join_str += ' INNER JOIN {0} ON {1}.{2} = {0}.{2}'.format(table_name, table_list[0], quoteField(table_join_column))

	# Update log
	logging.info('Created join tables statement for database call')

	# Return the inner join string
	return inner_join_str

def reportSqlError (sql_error, column_list = None, value_list = None):

	# Check if the unique constraint failed
	if 'UNIQUE constraint failed' in str(sql_error):

		# Get the column path that failed
		sql_column_path = str(sql_error).split(': ')[1]

		# Assign the table
		sql_table = sql_column_path.split('.')[0]

		# Assign the column
		sql_column = sql_column_path.split('.')[1]

		# Create an empty warning string to add to
		sql_error_message = ''

		# Check if column and values were given, and that the sql column can be found
		if column_list and value_list and (sql_column_path in column_list or sql_column in column_list):

			# Check if the column path is within the value list
			if sql_column_path in column_list:

				# Get the column index
				sql_column_index = column_list.index(sql_column_path)

			# Check if the column is within the value list
			elif sql_column in column_list:

				# Get the column index
				sql_column_index = column_list.index(sql_column)

			# Update the message
			sql_error_message += '%s already exists' % value_list[sql_column_index]
		
		# Check if the error message has been populated
		if sql_error_message:

			# Add punctuation
			sql_error_message += '. '

		# Quote the column path, if needed
		sql_quoted_column_path = quoteField(sql_column_path)

		# Display warning for duplicates, as raise would only report the first entry
		logging.warning('%s%s does not support duplicate entries' % (sql_error_message, sql_quoted_column_path))

	# If not a known error, report the standard message
	else:

		raise sql_error

def createTable (cursor, table, column_assignment_str):

	try:
		
		# Createt the table assignment string
		sqlite_create_table = 'CREATE TABLE IF NOT EXISTS %s (%s)' % (table, column_assignment_str)

		# Execute the create table command
		cursor.execute(sqlite_create_table)

		# Update log
		logging.info('Successfully created table (%s) in database' % table)

	except sqlite3.Error as error:
		raise Exception(error)

def insertValues (cursor, table, column_list, value_list):

	try:

		# Create list with column_list and the date columns
		column_list_w_dates = column_list + ['Last Modified (%s)' % table, 'Entry Created (%s)' % table]

		# Quote the columns as needed
		column_list_w_dates = quoteFields(column_list_w_dates)

		# Record the current time
		insert_time = currentTime()

		# Create list with value_list and the date values
		value_list_w_dates = value_list + [insert_time, insert_time]

		# Quote the values as needed
		value_list_w_dates = quoteFields(value_list_w_dates, split_by_dot = False)
	
		# Create the insert string
		sqlite_insert_values = 'INSERT INTO %s (%s) VALUES (%s)' % (table, ', '.join(column_list_w_dates), valueMarksStr(column_list_w_dates))

		# Execute the insert values command
		cursor.execute(sqlite_insert_values, value_list_w_dates)

		# Update log
		logging.info('Successfully inserted data into the database')

	except sqlite3.Error as sql_error:

		# Report the error
		reportSqlError(sql_error, column_list = column_list, value_list = value_list)

def updateValues (cursor, table, selection_dict, update_dict, update_table_column = None, tables_to_join = None, join_table_columns = None):

	try:

		# Record the current time
		insert_time = currentTime()

		# Create the select expression string for the SQL call
		select_expression = returnSelectionDict(selection_dict)

		# Create a list of the values associated with the select expression string
		select_value_list = returnSelectionValues(selection_dict)

		# Create an empty from expression for single table updates
		from_expression = ''

		# Check for multiple tables to select from
		if update_table_column and tables_to_join and join_table_columns:

			# Create an inner join statement
			table_str = innerJoinTables(tables_to_join, join_table_columns)

			# Update the selection expression with a select clause
			select_expression = '{1} IN (SELECT {0}.{1} FROM {2} WHERE {3})'.format(table, quoteField(update_table_column), table_str, select_expression)

		# Check if the multiple tables were incorrectly assigned
		elif update_table_column or tables_to_join or join_table_columns:

			raise Exception('Error updating database, unable to create FROM statement')

		# Copy the update dict
		update_dict_w_date = copy.copy(update_dict)

		# Update the update dict to include the date modified column
		update_dict_w_date['Last Modified ({0})'.format(table)] = insert_time

		# Create the update expression string for the SQL call
		update_expression = returnSetExpression(update_dict_w_date)

		# Create a quoated list of the values associated with the update expression string
		update_value_list = quoteFields(list(update_dict_w_date.values()), split_by_dot = False)

		# Combine the value lists and save as a tuple
		sqlite_value_list = tuple(update_value_list + select_value_list)

		# Create the update string
		sqlite_update_values = 'UPDATE %s SET %s WHERE %s' % (table, update_expression, select_expression)

		# Execute the insert values command
		cursor.execute(sqlite_update_values, sqlite_value_list)

		# Update log
		logging.info('Successfully updated the database')

	except sqlite3.Error as error:

		raise Exception(error)

def retrieveValues (cursor, tables, selection_dict, column_list, join_table_columns = None):

	try:

		# Create the select expression string for the SQL call
		select_expression = returnSelectionDict(selection_dict)

		# Create a list of the values associated with the select expression string
		select_value_list = returnSelectionValues(selection_dict)

		# Create a quoated list of the values
		select_value_list = quoteFields(select_value_list, split_by_dot = False)

		# Quote the columns as needed
		column_list = quoteFields(column_list)

		#  a string of the column list
		column_str = ', '.join(column_list)

		# Check if there is more than a single table
		if len(tables) > 1:

			# Create an inner join statement
			table_str = innerJoinTables(tables, join_table_columns)

		else:

			# Define the table string
			table_str = tables[0]
	
		# Create the basic select string
		sqlite_select_values = 'SELECT %s FROM %s' % (column_str, table_str)

		# Check if a selection expression was given
		if select_expression:

			# Update the sqlite string if an expression was given
			sqlite_select_values += ' WHERE %s' % select_expression

		# Check if selection expression values were created
		if select_value_list:

			# Execute the select values command
			cursor.execute(sqlite_select_values, select_value_list)

		else:

			# Execute the select values command
			cursor.execute(sqlite_select_values)

		# Save the selected results
		selection_results = cursor.fetchall()

		# Check if anything was returned
		if len(selection_results) == 0:

			# Print error message
			raise Exception('No output produced. Please check your input')

		# Update log
		logging.info('Successfully retrieved results from database')

		# Return the retrieved data as a dict
		return selection_results

	except sqlite3.Error as error:
		raise Exception(error)

def confirmValue (cursor, table, column, value):
	
	try:

		# Create a list to store the check results
		check_results = []

		# Check if the value is None
		if value == None:
			
			# Create the value check string
			sqlite_check_value = 'SELECT COUNT(*) FROM %s WHERE %s is NULL' % (table, quoteField(column))

			# Execute the create table command
			cursor.execute(sqlite_check_value)

		else:
		
			# Create the value check string
			sqlite_check_value = 'SELECT COUNT(*) FROM %s WHERE %s = ?' % (table, quoteField(column))

			# Execute the create table command
			cursor.execute(sqlite_check_value, [quoteField(value)])

		# Save the selected results
		selection_results = cursor.fetchall()

		# Check if there was an assignment error
		if not selection_results:
			raise Exception('Assignment error when checking for presence of %s in %s:%s)' % (value, table, column))

		# Check if the table was found
		if selection_results[0][0] >= 1:

			# Return True
			return True

		# Check if the table was not found
		elif selection_results[0][0] == 0:

			# Return False
			return False

		# Check if there was an unknown error
		else:

			raise Exception('Unknown error checking for presence of %s in %s:%s)' % (value, table, column))

	except sqlite3.Error as error:
		raise Exception(error)

def backupDatabase (database, backup_database):

	# Connect to the databse 
	sqlite_database_connection = sqlite3.connect(database)

	# Connect to the backup
	sqlite_backup_connection = sqlite3.connect(backup_database)

	# Backup the database
	with sqlite_backup_connection:
		sqlite_database_connection.backup(sqlite_backup_connection, pages=1)
	
	# Close the connections 	
	sqlite_backup_connection.close()
	sqlite_database_connection.close()
'''

'''
class SQLQuery ():
	def __init__ (self, sql_query = [], sql_filters = None, sql_updates = {}, sql_tables = [], tables_in_query = [], sql_session = None):
		self.query_results = None
		self._sql_query = sql_query
		self._sql_filters = sql_filters
		self._sql_updates = sql_updates
		self._sql_tables = sql_tables
		self._tables_in_query = tables_in_query
		self._sql_session = sql_session

	@property
	def _total_tables_in_query (self):
		return sum(self._tables_in_query)

	@property
	def _total_tables_in_results (self):
		return len(self._sql_query)

	def query_all (self):

		if self._total_tables_in_query > 1:
			for parent_key, foreign_key in foreignKeyPairs(self._sql_tables):
				if not self._sql_filters:
					self._sql_filters = SQLFilter.fromQuery(self._sql_tables)
				self._sql_filters.addColEQFilter(parent_key, foreign_key)
		self.query_results = self._sql_session.query(*self._sql_query).filter(*self._sql_filters.filters).all()

	def query_update (self):

		if self._total_tables_in_query > 1:
			for parent_key, foreign_key in foreignKeyPairs(self._sql_tables):
				if not self._sql_filters:
					self._sql_filters = SQLFilter.fromQuery(self._sql_tables)
				self._sql_filters.addColEQFilter(parent_key, foreign_key)
		self.query_results = self._sql_session.query(*self._sql_query).filter(*self._sql_filters.filters).all()

		if self._total_tables_in_results > 1:

			for row in self.query_results:
				#session.add(prop)
				#row_dict = {}
				for result_table in row:
					for update_table in self._sql_updates.updates:
						if result_table.__tablename__ != update_table.__tablename__:
							continue
							for key, value in self._sql_updates.updates[update_table].items():
								key = value
								self._sql_session.add(key)


								#print (key == result_table)


					#session.add(prop)
					#for result_key, result_value in self.object_as_dict(result_table).items():
					#	row_dict[result_key] = result_value
				#print(row_dict)
		else:

			for row in self.query_results:
				print('Here')
				#print(self.object_as_dict(row))

	def toCSV (self):

		if self._total_tables_in_results > 1:

			for row in self.query_results:
				row_dict = {}
				for result_table in row:
					for result_key, result_value in self.object_as_dict(result_table).items():
						row_dict[result_key] = result_value
				print(row_dict)
		else:

			for row in self.query_results:
				print(self.object_as_dict(row))

	@classmethod
	def fromConfig (cls, config_data, sql_session):

		# Create a SQLQuery object from the config file and sql_session
		return cls(sql_tables = config_data.sql_tables, tables_in_query = [False] * len(config_data.sql_tables), sql_session = sql_session)

	def addQuery(self, query, type = None):

		# Add the query, update the tables in use
		for table_pos, sql_table in enumerate(self._sql_tables):
			if query == sql_table:
				self._tables_in_query[table_pos] = True
		self._sql_query.append(query)

	def addDictFilter(self, col_dict, include = None, type = None, table_dict = False):

		# Add the filter from a dict, update the tables in use
		sql_filter = SQLFilter.fromQuery(self._sql_tables)
		sql_filter.addColDictFilter(col_dict)
		for table_pos in sql_filter._tables_used:
			if not self._tables_in_query[table_pos]:
				self._tables_in_query[table_pos] = True
		self._sql_filters = sql_filter

	def addDictUpdate(self, col_dict):

		# Add the update from a dict, update the tables in use
		sql_update = SQLUpdate.fromQuery(self._sql_tables)
		sql_update.addColDictUpdate(col_dict)
		for table_pos in sql_update._tables_used:
			if not self._tables_in_query[table_pos]:
				self._tables_in_query[table_pos] = True
		self._sql_updates = sql_update

	@staticmethod
	def object_as_dict(obj):
		return {c.key: getattr(obj, c.key) for c in inspect(obj).mapper.column_attrs}

class SQLFilter ():
	def __init__ (self, sql_tables = [], tables_in_filters = []):
		self._filters = []
		self._tables_in_filters = tables_in_filters
		self._sql_tables = sql_tables

	@property
	def filters (self):
		return self._filters

	@property
	def _tables_used (self):
		return [table_pos for table_pos, table_bool in enumerate(self._tables_in_filters) if table_bool]

	def __contains__ (self, new_filter):

		# Check if the object contains the filter
		for sql_filter in self._filters:
			if new_filter.compare(sql_filter): return True
		else: return False

	def addColDictFilter (self, col_dict):

		# Add the column-based dict filter
		for col_name, col_value in col_dict.items():
			filter_pos, filter_arg = None, None
			for table_pos, sql_table in enumerate(self._sql_tables):
				try:
					test_arg = getattr(sql_table, col_name)
					if len(test_arg.foreign_keys) >= 1:
						continue
					filter_arg = test_arg
					filter_pos = table_pos
				except: pass
			if not filter_arg: raise Exception('Unable to assign filter: %s: %s' % (col_name, col_value))
			if not self._tables_in_filters[filter_pos]: self._tables_in_filters[table_pos] = True
			self._filters.append(filter_arg == col_value)

	def addColEQFilter (self, col1, col2):

		# Add the column eq filter (i.e. col1 == col2)
		new_filter = (col1 == col2)
		if new_filter not in self:
			self._filters.append(new_filter)
		else:
			logging.warning('Filter (%s) already assigned' % str(new_filter))

	@classmethod
	def fromConfig (cls, config_data):

		# Create a SQLFilter object
		return cls(sql_tables = config_data.sql_tables, tables_in_filters = [False] * len(config_data.sql_tables))

	@classmethod
	def fromQuery (cls, sql_tables):

		# Create a SQLFilter object
		return cls(sql_tables = sql_tables, tables_in_filters = [False] * len(sql_tables))

class SQLUpdate ():
	def __init__ (self, sql_tables = [], tables_in_updates = []):
		self._updates = defaultdict(lambda: defaultdict(str))
		self._tables_in_updates = tables_in_updates
		self._sql_tables = sql_tables

	@property
	def updates (self):
		return self._updates

	@property
	def _tables_used (self):
		return [table_pos for table_pos, table_bool in enumerate(self._tables_in_updates) if table_bool]

	def __contains__ (self, new_update):

		# Check if the object contains the filter
		for update_column in list(new_update):
			if update_column in list(self._updates): return True
		else: return False

	def addColDictUpdate (self, col_dict):

		# Add the column-based dict update
		for col_name, col_value in col_dict.items():
			update_pos, update_arg, update_table = None, None, None
			for table_pos, sql_table in enumerate(self._sql_tables):
				try: 
					test_arg = getattr(sql_table, col_name)
					if len(test_arg.foreign_keys) >= 1:
						continue
					update_arg = test_arg
					update_pos = table_pos
					update_table = sql_table
				except: pass
			if not update_arg: raise Exception('Unable to assign update: %s: %s' % (col_name, col_value))

			if not self._tables_in_updates[update_pos]: self._tables_in_updates[update_pos] = True
			self._updates[update_table][update_arg] = col_value

	@classmethod
	def fromConfig (cls, config_data):

		# Create a SQLFilter object
		return cls(sql_tables = config_data.sql_tables, tables_in_updates = [False] * len(config_data.sql_tables))

	@classmethod
	def fromQuery (cls, sql_tables):

		# Create a SQLFilter object
		return cls(sql_tables = sql_tables, tables_in_updates = [False] * len(sql_tables))
'''