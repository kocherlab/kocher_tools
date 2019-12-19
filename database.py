import os
import sys
import sqlite3
import datetime
import pytz
import copy
import itertools

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
		for field_sub_str in field_str.split('.'):

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

				print()
				sys.exit()

	return selection_str

def returnSelectionValues (selection_dict):

	# Create an empty list to hold the values for the selection expression
	expression_statement_values = []

	# Loop the dict by operator
	for selection_operator, selection_data in selection_dict.items():

		# Add the include expression values to the list
		expression_statement_values.extend(itertools.chain.from_iterable(list(selection_data.values())))

	# Quote the values
	expression_statement_values = quoteFields(expression_statement_values)

	# Return the list
	return expression_statement_values

def innerJoinTables (table_list, join_key_list):

	# Create a string for the inner join command
	inner_join_str = ''

	# Add the first table to the string
	inner_join_str += table_list[0]

	# Loop the tables, skipping the first entry
	for table_name, table_join_key in zip(table_list[1:], join_key_list):

		# Add the inner join string
		inner_join_str += ' INNER JOIN {0} ON {1}.{2} = {0}.{2}'.format(table_name, table_list[0], quoteField(table_join_key))

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

		# Raise the updated exception
		raise Exception('%s%s does not support duplicate entries' % (sql_error_message, sql_column_path)) from sql_error

	# If not a known error, report the standard message
	else:

		raise sql_error

def createTable (database, table, column_assignment_str):

	try:
		
		# Createt the table assignment string
		sqlite_create_table = 'CREATE TABLE IF NOT EXISTS %s (%s)' % (table, column_assignment_str)

		# Connect to the sqlite database
		sqlite_connection = sqlite3.connect(database)

		# Create the cursor
		cursor = sqlite_connection.cursor()

		# Execute the create table command
		cursor.execute(sqlite_create_table)

		# Commit any changes
		sqlite_connection.commit()

		# Close the connection
		cursor.close()

	except sqlite3.Error as error:
		raise Exception(error)

def insertValues (database, table, column_list, value_list):

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

		# Connect to the sqlite database
		sqlite_connection = sqlite3.connect(database)

		# Create the cursor
		cursor = sqlite_connection.cursor()

		# Execute the insert values command
		cursor.execute(sqlite_insert_values, value_list_w_dates)

		# Commit any changes
		sqlite_connection.commit()

		# Close the connection
		cursor.close()

	except sqlite3.Error as sql_error:

		# Report the error
		reportSqlError(sql_error, column_list = column_list, value_list = value_list)

def updateValues (database, table, selection_dict, update_dict, update_table_key = None, tables_to_join = None, join_tables_keys = None):

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
		if update_table_key and tables_to_join and join_tables_keys:

			# Create an inner join statement
			table_str = innerJoinTables(tables_to_join, join_tables_keys)

			# Update the selection expression with a select clause
			select_expression = '{1} IN (SELECT {0}.{1} FROM {2} WHERE {3})'.format(table, quoteField(update_table_key), table_str, select_expression)

		# Check if the multiple tables were incorrectly assigned
		elif update_table_key or tables_to_join or join_tables_keys:

			print(update_table_key, tables_to_join, join_tables_keys)

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

		# Connect to the sqlite database
		sqlite_connection = sqlite3.connect(database)

		# Create the cursor
		cursor = sqlite_connection.cursor()

		# Execute the insert values command
		cursor.execute(sqlite_update_values, sqlite_value_list)

		# Commit any changes
		sqlite_connection.commit()

		# Close the connection
		cursor.close()

	except sqlite3.Error as error:
		raise Exception(error)

def retrieveValues (database, tables, selection_dict, column_list, join_tables_keys = None):

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
			table_str = innerJoinTables(tables, join_tables_keys)

		else:

			# Define the table string
			table_str = tables[0]
	
		# Create the select string
		sqlite_select_values = 'SELECT %s FROM %s WHERE %s' % (column_str, table_str, select_expression)

		# Connect to the sqlite database
		sqlite_connection = sqlite3.connect(database)

		# Setup SQLite to reture the rows as dict with columns
		sqlite_connection.row_factory = sqlite3.Row

		# Create the cursor
		cursor = sqlite_connection.cursor()

		# Execute the select values command
		cursor.execute(sqlite_select_values, select_value_list)

		# Save the selected results
		selection_results = cursor.fetchall()

		# Commit any changes
		sqlite_connection.commit()

		# Close the connection
		cursor.close()

		# Return the retrieved data as a dict
		return selection_results

	except sqlite3.Error as error:
		raise Exception(error)
