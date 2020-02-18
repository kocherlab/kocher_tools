import os
import sys
import datetime
import pytz
import logging
import random
import string

from kocher_tools.database import backupDatabase

# Create a module date object to store a single date for all tasks
module_date_object = None

def returnDate (timezone = 'US/Eastern'):

	# Assign the module date object as global
	global module_date_object

	# Check if the module date object is already assigned
	if not module_date_object:

		# Set the timezone
		specified_timezone = pytz.timezone(timezone)

		# Assign the current date
		module_date_object = datetime.datetime.now(specified_timezone)

	# Return the date object
	return module_date_object

def returnDateStr ():

	# Assign the module date object as global
	global module_date_object

	# Check if the module date object is already assigned
	if not module_date_object:

		# Assign the current date
		module_date_object = returnDate()

	# Set the date format for the logging system
	date_format = '%Y-%m-%d'

	# Convert the date into a string
	date_str = module_date_object.strftime(date_format)

	# Return the date string
	return date_str

def strToDate (date_str):

	# Convert the date string into a date object
	date_object = datetime.datetime.strptime(date_str, '%Y-%m-%d')

	# Return the date object
	return date_object

def backupNeeded (database, out_dir, update_freq, timezone = 'US/Eastern'):

	# Assign the database basename and file extension
	database_basename = os.path.basename(database)

	# Assign the current date
	current_date_object = returnDate()

	# Set the timezone
	specified_timezone = pytz.timezone(timezone)

	# Assign a bool to assign backup status
	create_backup = True

	# Loop the files within the backup dir
	for past_backup in os.listdir(out_dir):

		# Assign the past backup date str
		past_backup_date_str = past_backup.rsplit('.', 2)[-2]

		# Assign the backup date
		past_backup_date = strToDate(past_backup_date_str)

		# Update the past backup date with the timezone
		past_backup_date = past_backup_date.replace(tzinfo = specified_timezone)

		# Get the difference in time
		date_diff = current_date_object - past_backup_date

		# Check if the difference in greater than the update frequency
		if update_freq >= date_diff.days:

			# Update the bool
			create_backup = False

	# Return the result
	return create_backup

def createBackup (database, out_dir):

	# Assign the database basename and file extension
	database_basename = os.path.basename(database)
		
	# Convert the date into a string
	current_date_str = returnDateStr()

	# Create random string for database filename
	random_str = ''.join(random.choice(string.ascii_uppercase + string.digits) for i in range(6))

	# Assign the backup filename
	backup_file = os.path.join(out_dir, '%s.%s.%s.backup' % (database_basename, random_str, current_date_str))

	# Create the database
	backupDatabase(database, backup_file)

	# Update log
	logging.info('Backup created: %s' % backup_file)

def removeOldBackup (database, out_dir, file_limit):

	# Assign the database basename and file extension
	database_basename = os.path.basename(database)

	# Assign a dict to store the backups
	backups = {}

	# Loop the files within the backup dir
	for past_backup in os.listdir(out_dir):

		# Assign the past backup date str
		past_backup_date_str = past_backup.rsplit('.', 2)[-2]

		# Assign the backup date
		past_backup_date = strToDate(past_backup_date_str)

		# Update the dict
		backups[past_backup_date] = past_backup

	# Create a list of the dates
	backup_date_list = list(backups.keys())

	# Check if the number of backups is larger than the backup limit
	while len(backup_date_list) > file_limit:

		# Sort the dates
		backup_date_list.sort()

		# Assign the oldest backup date
		oldest_backup_date = backup_date_list[0]

		# Assign the oldest backup basename
		oldest_backup_basename = backups[oldest_backup_date]

		# Assign the oldest backup filepath
		oldest_backup_file = os.path.join(out_dir, oldest_backup_basename)

		# Confirm the file existsm if not, raise an exception
		if not os.path.exists(oldest_backup_file):
			raise Exception('Unable to delete backup %s. File not found' % oldest_backup_file)

		# Remove the oldest backup
		os.remove(oldest_backup_file)

		logging.info('Backup file (%s) deleted' % oldest_backup_basename)

		# Update the list by removing the deleted date
		backup_date_list.remove(oldest_backup_date)
