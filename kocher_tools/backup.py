import os
import sys
import datetime
import pytz
import logging
import random
import string

from kocher_tools.database import backupDatabase

class Backups (list):
	def __init__ (self, *arg, out_dir = None, limit = None, update_freq = None, **kw):
		super(Backups, self).__init__(*arg, **kw)
		self.out_dir = out_dir
		self.limit = limit
		self.update_freq = update_freq
		self.timezone = pytz.timezone('US/Eastern')
		self.str_format = '%Y-%m-%d'
		self.date = datetime.datetime.now(self.timezone)
		self.oldest_backup = None
		self.most_recent_backup = None

		# Check if data was not assigned
		if not self:

			# Assign the backups
			self.assignBackups()

	@property
	def date_str (self):

		# Return the date as a string of Year-Month-Day
		return self.date.strftime(self.str_format)

	def assignBackups (self):

		# Assign an empty object to store the oldest backup
		oldest_backup = None

		# Assign an empty object to store the newest backup
		most_recent_backup = None
        
		# Loop the files within the backup dir
		for backup_file in os.listdir(self.out_dir):

			# Assign the backup file path
			backup_file_path = os.path.join(self.out_dir, backup_file)

			# Assign the past backup date str
			past_backup_date_str = backup_file.rsplit('.', 2)[-2]

			# Assign the backup date
			past_backup_date = datetime.datetime.strptime(past_backup_date_str, self.str_format)

			# Update the past backup date with the timezone
			past_backup_date = past_backup_date.replace(tzinfo = self.timezone)

			# Assign a backup
			past_backup = Backup(backup_file_path, past_backup_date)

			# Check if the oldest backup has been defined
			if not oldest_backup:

				# Assign the current backup
				oldest_backup = past_backup

			else:

				# Check if the current backup is older than the stored oldest backup
				if past_backup < oldest_backup:

					# Assign the current backup
					oldest_backup = past_backup

			# Check if the newest backup has been defined
			if not most_recent_backup:

				# Assign the current backup
				most_recent_backup = past_backup

			else:

				# Check if the current backup is older than the stored oldest backup
				if past_backup > most_recent_backup:

					# Assign the current backup
					most_recent_backup = past_backup

			# Save the table
			self.append(past_backup)

		# Assing the oldest backup and the most recent date
		self.oldest_backup = oldest_backup
		self.most_recent_backup = most_recent_backup

	def updateBackups (self):

		# Check if the number of backups exceeds the backup limit
		if len(self) > self.limit:

			# Delete the oldest backup
			self.deleteBackup(self.oldest_backup)

			# Assign an empty object to store the oldest backup
			oldest_backup = None

			# Assign an empty object to store the newest backup
			most_recent_backup = None
	        
			# Loop the backups, backup by backup
			for current_backup in self:

				# Check if the oldest backup has been defined
				if not oldest_backup:

					# Assign the current backup
					oldest_backup = current_backup

				else:

					# Check if the current backup is older than the stored oldest backup
					if current_backup < oldest_backup:

						# Assign the current backup
						oldest_backup = current_backup

				# Check if the newest backup has been defined
				if not most_recent_backup:

					# Assign the current backup
					most_recent_backup = current_backup

				else:

					# Check if the current backup is older than the stored oldest backup
					if current_backup > most_recent_backup:

						# Assign the current backup
						most_recent_backup = current_backup

			# Assing the oldest backup and the most recent date
			self.oldest_backup = oldest_backup
			self.most_recent_backup = most_recent_backup

	def backupNeeded (self):

		# Check if there are no backups
		if not self.most_recent_backup:

			# Return True if their is no backup
			return True

		# Get the difference in time from the most recent backup
		date_diff = self.date - self.most_recent_backup.backup_date

		# Check if the difference in greater than the update frequency
		if self.update_freq >= date_diff.days:

			# Return False if the limit to larger than the number of days
			return False

		# Return True if the limit is smaller
		return True

	def newBackup (self, database):

		# Assign the database basename and file extension
		database_basename = os.path.basename(database)
			
		# Create random string for database filename
		random_str = ''.join(random.choice(string.ascii_uppercase + string.digits) for i in range(6))

		# Assign the backup filename
		backup_file = os.path.join(self.out_dir, '%s.%s.%s.backup' % (database_basename, random_str, self.date_str))

		# Create the database
		backupDatabase(database, backup_file)

		# Assign the new backup
		new_backup = Backup(backup_file, self.date)

		# Append the new backup
		self.append(new_backup)

		# Update the most recent backup
		self.most_recent_backup = new_backup

		# Update the backups
		self.updateBackups()

	def deleteBackup (self, backup_to_delete):

		# Assign the file path of the backup file
		backup_file_path = str(backup_to_delete)

		# Confirm the file exists, otherwise raise an error
		if not os.path.isfile(backup_file_path):
			raise Exception('Backup file (%s) not found' % backup_file_path)

		# Remove the file
		os.remove(backup_file_path)

		# Loop the backups by index
		for backup_pos in range(len(self)):

			# Check if the current backup is the one to remove
			if self[backup_pos] == backup_to_delete:

				# Remove the backup object
				del self[backup_pos]
				break

class Backup ():
	def __init__ (self, file_path, backup_date):

		# Confirm the file exists
		if not os.path.isfile(file_path):
			raise Exception('Backup file (%s) not found' % file_path)

		self.file_path = file_path
		self.backup_date = backup_date

	def __str__ (self):

		# str() returns the file path
		return self.file_path

	def __gt__ (self, other_backup):

		# Check if the stored date is more recent
		return self.backup_date > other_backup.backup_date

	def __lt__ (self, other_backup):

		# Check if the stored date is older
		return self.backup_date < other_backup.backup_date


