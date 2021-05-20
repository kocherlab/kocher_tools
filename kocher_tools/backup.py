import os
import sys
import datetime
import pytz
import logging
import random
import string
import subprocess

from kocher_tools.misc import confirmExecutable
from kocher_tools.logger import startLogger

class Backups (list):
	def __init__ (self, *arg, backup_dir = None, backup_subdir = '', log_file = '', file_limit = 10, day_cutoff = 120, **kw):
		super(Backups, self).__init__(*arg, **kw)
		self.backup_dir = os.path.join(backup_dir, backup_subdir)
		self.log_file = log_file
		self.file_limit = int(file_limit)
		self.str_format = '%Y-%m-%d'
		self.date = datetime.datetime.now()
		self.date_cutoff = self.date - datetime.timedelta(days = day_cutoff)
		self.oldest_backup = None
		self.most_recent_backup = None

		# Start the logger
		startLogger(self.log_file, filemode = 'a')

		# Check if data was not assigned
		if not self: self.assignBackups()

	def __str__ (self):
		return f'{[str(_b) for _b in self]}'

	@property
	def date_str (self):

		# Return the date as a string of Year-Month-Day-Seconds
		return self.date.strftime(self.str_format)

	def assignBackups (self):

		# Assign an empty object to store the oldest and newest backups
		oldest_backup = None
		most_recent_backup = None
        
		# Loop the files within the backup dir
		for backup_file in os.listdir(self.backup_dir):

			# Assign the backup file path
			backup_file_path = os.path.join(self.backup_dir, backup_file)

			# Assign the backup
			past_backup = Backup(backup_file_path)
			self.append(past_backup)

			# Check if the current backup is older than the stored oldest backup
			if not oldest_backup or past_backup < oldest_backup: 
				oldest_backup = past_backup

			# Check if the current backup is older than the stored oldest backup
			if not most_recent_backup or past_backup > most_recent_backup: 
				most_recent_backup = past_backup

		# Assing the oldest backup and the most recent date
		self.oldest_backup = oldest_backup
		self.most_recent_backup = most_recent_backup

		# Delete any old backups
		self.deleteOldBackups()

	def updateBackups (self):

		# Assign an empty object to store the oldest and newest backups
		oldest_backup = None
		most_recent_backup = None
       
		# Loop the backups, backup by backup
		for current_backup in self:

			# Assign/update the oldest backup
			if not oldest_backup or current_backup < oldest_backup:
				oldest_backup = current_backup

			# Assign/update the newest backup
			if not most_recent_backup or current_backup > most_recent_backup:
				most_recent_backup = current_backup

		# Assign the oldest backup and the most recent date
		self.oldest_backup = oldest_backup
		self.most_recent_backup = most_recent_backup

	def backupfromConfig (self, config_data):

		try:

			# Assign the arg list
			if config_data.type == 'postgresql':
				backup_filename = os.path.join(self.backup_dir, f'{config_data.schema}.{self.randStr()}.{self.date_str}.dmp')
				os.environ['PGPASSWORD'] = "pass01"
				backup_call_args = ['-U', config_data.user, '-d', config_data.database, '-n', config_data.schema, '-h', config_data.host, '-Fc']
				self.executeBackup('pg_dump', backup_call_args, backup_filename)
			if config_data.type == 'sqlite': raise Exception('In Development')

			# Assign the new backup
			new_backup = Backup(backup_filename)
			self.append(new_backup)
			self.most_recent_backup = new_backup

			logging.info(f'Backup Created Successfully: {backup_filename}')

			# Delete any old backups
			self.deleteOldBackups()

		except: 
			logging.info('Backup Failed')
			raise

	def deleteOldBackups (self):

		# Remove old backups
		old_backups = [_bf for _bf in self if self.date_cutoff > _bf]
		for old_backup in old_backups:
			try:
				self.remove(old_backup)
				old_backup.deleteFile()
				logging.info(f'Successfully deleted backup (date threshold): {old_backup}')
			except: 
				logging.info(f'Failed to delete backup (date threshold): {old_backup}')
				raise
		
		# Update the backups
		self.updateBackups()

	
		# Remove backups until they dont exceed the file limit
		while len(self) > self.file_limit:

			# Remove the oldest backup
			try:
				self.remove(self.oldest_backup)
				self.oldest_backup.deleteFile()
				logging.info(f'Successfully deleted backup (max file threshold): {self.oldest_backup}')
			except: 
				logging.info(f'Failed to delete backup(s) (max file threshold): {self.oldest_backup}')
				raise
			
			# Update the backups
			self.updateBackups()

		

	@classmethod
	def fromConfig (cls, config_data, backup_subdir = ''):
		return cls(backup_dir = config_data.backup_dir, backup_subdir = backup_subdir, log_file = config_data.log_file, file_limit = config_data.max_files, day_cutoff = config_data.max_days_old)

	@staticmethod
	def randStr (str_len = 6):
		return ''.join(random.choice(string.ascii_uppercase + string.digits) for i in range(str_len))

	@staticmethod
	def executeBackup (backup_executable_str, backup_call_args, backup_output):

		# Check if the executable is installed
		backup_executable = confirmExecutable(backup_executable_str)
		if not backup_executable:
			raise IOError(f'{backup_executable_str} not found. Please confirm the executable is installed')

		# Open the output file and make the subprocess call
		backup_output_file = open(backup_output, 'w')
		backup_call = subprocess.Popen([backup_executable] + backup_call_args, stderr = subprocess.PIPE, stdout = backup_output_file)

		# Get stdout and stderr from subprocess
		backup_stdout, backup_stderr = backup_call.communicate()
		if sys.version_info[0] == 3: backup_stderr = backup_stderr.decode()

		# Report any errors
		if backup_stderr: raise Exception(backup_stderr)

		# Close the file
		backup_output_file.close()

class Backup ():
	def __init__ (self, file_path):

		# Confirm the file exists
		if not os.path.isfile(file_path): raise Exception(f'Backup file ({file_path}) not found')

		# Assign the file modification date
		try: backup_date = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
		except: raise Exception(f'Unable to assign modification date for: {file_path}')

		self.file_path = file_path
		self.backup_date = backup_date

	def __str__ (self):

		# str() returns the file path
		return self.file_path

	def __gt__ (self, other_date_object):

		# Check if the backup date is more recent
		if isinstance(other_date_object, Backup): return self.backup_date > other_date_object.backup_date
		elif isinstance(other_date_object, datetime.datetime): return self.backup_date > other_date_object

	def __lt__ (self, other_date_object):

		# Check if the backup date is older
		if isinstance(other_date_object, Backup): return self.backup_date < other_date_object.backup_date
		elif isinstance(other_date_object, datetime.datetime): return self.backup_date < other_date_object

	def deleteFile (self):
		os.remove(self.file_path )



