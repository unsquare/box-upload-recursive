# This script relies on the Box Python SDK:
# pip install boxsdk[jwt]==2.0.0a11

import os
import time
import datetime

from requests import ReadTimeout, ConnectTimeout, HTTPError, Timeout, ConnectionError

from boxsdk import JWTAuth
from boxsdk import Client
from boxsdk.exception import BoxAPIException

app_users = {'1':'APP_USER_ID_HERE', '2':'APP_USER_ID_HERE', '3':'APP_USER_ID_HERE'} # Add as many app users as you want here
log_file_id = 'LOG_FOLDER_ID_HERE' # ID for the Box Log Files folder
box_size_limit = 16106127360 # 15gb in bytes
log_batch = 5000 # Upload log every X records
max_attempts = 10 # Maximum number of retries after a ConnectionError
time_to_wait = 30 # Number of seconds to wait

# Set the variables below if you want to hard-code your script
# uploader_id = '' # ID for the app user that will perform the upload
# home_folder_id = '' # ID for the overall parent folder where you are uploading
# s_input_folder = '' # path to the local folder where your files are stored
# top_level_name = '' # name of the folder to create on Box inside your home folder

d_invalid_char_replace= { "/" : "_", "?": "X", "<": "(", ">": ")", "\\": "#", ":":"_", "*": "-", "|": "!", "\"" :""}
ignored_files_and_folders = ['.DS_Store', '.Trash', '.Spotlight-V100', '_gsdata_'] # Mac hidden files

counts = {"files_uploaded": 0,
	"files_existing": 0,
	"folders_created": 0,
	"folders_existing":0,
	"skipped": 0,
	"errors": 0,
	"oversize": 0}
	
def box_auth(user_id):
	auth = JWTAuth(
		client_id='YOUR_CLIENT_ID',
		client_secret='YOUR_CLIENT_SECRET',
		enterprise_id='YOUR_ENTERPRISE_ID',
		jwt_key_id='YOUR_JWT_KEY_ID',
		rsa_private_key_passphrase='RSA_PRIVATE_KEY_PASSPHRASE',
		rsa_private_key_file_sys_path='PATH_TO_KEY_FILE'
	)

	access_token = auth.authenticate_instance()
	
	client = Client(auth)
	user = client.user(user_id)
	
	return (client, user)

def update_log(result):
	now = '['+ time.strftime('%Y-%m-%d %I:%M:%S') + '] '
	if upload_log:
		upload_log.write(now + result + '\n')
		
		total_counts = sum(counts.values())
		
		if (total_counts > 0) and (total_counts % log_batch == 0):
			# Send the current log file to Box so that we get an in-progress view
			log = send_log_to_box(log_path, log_name, 1)
			print("Sent log file to Box.")
		
	print(now + result)
	
def send_log_to_box(log_path, log_name, attempts):
	try:
		upload_the_log = log_folder.upload(log_path, log_name, upload_using_accelerator=True)
		result = "\nUploaded log file '%s' to Box." % log_name
	except BoxAPIException as error:
		if error.code == "item_name_in_use":
			# The file already exists, so let's upload a new version
			existing_id = error.context_info['conflicts']['id']
			existing_file = client.as_user(user).file(existing_id).get()
			update_file = existing_file.update_contents(log_path, upload_using_accelerator=True)
			result = "\nUpdated log file '%s' on Box." % log_name
		else:
			# Some other error thrown
			result =  "\nUnable to upload log: %s" % error.message
	except ConnectionError as error:
		if attempts < max_attempts:
			attempts += 1
			print("I'm having trouble connecting. Attempt #" + str(attempts) + " in " + str(time_to_wait) + " seconds...")
			time.sleep(time_to_wait)
			result = send_log_to_box(log_path, log_name, attempts)
		else:
			result = "Maximum number of retries reached while trying to upload: %s" % log_name

	return(result)
	
def sanitize(sanitized_name):
	for i, j in d_invalid_char_replace.iteritems():
		sanitized_name = sanitized_name.replace(i, j)
	sanitized_name = sanitized_name.rstrip('. ') # strip periods and spaces from the end

	return sanitized_name

def create_folder(current_path, folder_name, parent_id, attempts):
	try:
		new_folder = client.as_user(user).folder(parent_id).create_subfolder(folder_name)
		counts['folders_created'] += 1
		result = "Folder '%s' successfully created as '%s'" % (current_path, folder_name)
		return(result, new_folder['id'])
	except BoxAPIException as error:
		if error.code == "item_name_in_use":
			try:
				existing_id = error.context_info['conflicts'][0]['id']
				existing_folder = client.as_user(user).folder(existing_id).get()
				result = "Folder '%s' already exists as '%s'" % (current_path, folder_name)
				counts['folders_existing'] += 1
				return(result, existing_id)
			except Exception as error:
				counts['errors'] += 1
				result = "UNCAUGHT ERROR: %s" % str(error)
				return(result, None)
		elif error.code == "name_temporarily_reserved":
			if attempts < max_attempts:
				attempts += 1
				update_log("Name temporarily reserved. Attempt #%s in %s seconds..." % (str(attempts), str(time_to_wait)))
				time.sleep(time_to_wait)
				result, folder_id = create_folder(current_path, folder_name, parent_id, attempts)
			else:
				counts['errors'] += 1
				result = "ERROR: Maximum number of retries reached while trying to create: %s" % current_path
				folder_id = None
			return(result, folder_id)
			
		else:
			counts['errors'] += 1
			result = "ERROR: Unable to create '%s': %s"  % (current_path, error.message)
			return(result, None)
	except ConnectionError as error:			
		if attempts < max_attempts:
			attempts += 1
			update_log("I'm having trouble connecting. Attempt #%s in %s seconds..." % (str(attempts), str(time_to_wait))) 
			time.sleep(time_to_wait)
			result, folder_id = create_folder(current_path, folder_name, parent_id, attempts)
		else:
			counts['errors'] += 1
			result = "ERROR: Maximum number of retries reached while trying to create: %s" % current_path
			folder_id = None
		return(result, folder_id)
	except Exception as error:
		counts['errors'] += 1
		result = "UNCAUGHT ERROR: %s" % str(error)
		return(result, None)

def create_file(file_path, file_name, parent_id, attempts):
	root_folder = client.as_user(user).folder(parent_id)
	file_size_local = os.path.getsize(file_path)
	if file_size_local <= box_size_limit:
		try:
			a_file = root_folder.upload(file_path, file_name, upload_using_accelerator=True)
			counts['files_uploaded'] += 1
			result = "File '%s' successfully created as '%s'" % (file_path, file_name)
			return(result, a_file['id'])
		except BoxAPIException as error:
			if error.code == "item_name_in_use":
				existing_id = error.context_info['conflicts']['id']
				existing_file = client.as_user(user).file(existing_id).get()
				file_size_box = existing_file['size']
			
				if file_size_box == file_size_local:
					counts['files_existing'] += 1
					try:
						result = "File '%s' already exists as '%s'" % (file_path, file_name)
					except Exception as error:
						result = "UNCAUGHT ERROR: %s" % str(error)
				else:
					try:
						# There must be a partial upload, so let's upload a new version
						update_file = existing_file.update_contents(file_path, upload_using_accelerator=True)
						counts['files_existing'] += 1
						result = "File '%s' already existed as '%s' but the size didn't match, so I updated it." % (file_path, file_name)
					except BoxAPIException as error:
						counts['errors'] += 1
						result = "ERROR: Unable to upload '%s': %s"  %(file_name, error.message)
					except ConnectionError as error:
						if attempts < max_attempts:
							attempts += 1
							update_log("I'm having trouble connecting. Attempt #%s in %s seconds..." % (str(attempts), str(time_to_wait)))
							time.sleep(time_to_wait)
							result, file_id = create_file(file_path, file_name, parent_id, attempts)
						else:
							counts['errors'] += 1
							result = "ERROR: Maximum number of retries reached while trying to create: %s" % file_path
							file_id = None
						return(result, file_id)

				return(result, existing_id)
			elif error.code == "name_temporarily_reserved":
				if attempts < max_attempts:
					attempts += 1
					update_log("Name temporarily reserved. Attempt #%s in %s seconds..." % (str(attempts), str(time_to_wait)))
					time.sleep(time_to_wait)
					result, file_id = create_file(file_path, file_name, parent_id, attempts)
				else:
					counts['errors'] += 1
					result = "ERROR: Maximum number of retries reached while trying to create: %s" % file_path
					file_id = None
				return(result, folder_id)
			else:
				counts['errors'] += 1
				result = "ERROR: Unable to create '%s': %s" % (file_name, error.message)
				return(result, None)
		except ConnectionError as error:
			if attempts < max_attempts:
				attempts += 1
				update_log("I'm having trouble connecting. Attempt #%s in %s seconds..." % (str(attempts), str(time_to_wait)))
				time.sleep(time_to_wait)
				result, file_id = create_file(file_path, file_name, parent_id, attempts)
			else:
				counts['errors'] += 1
				result = "ERROR: Maximum number of retries reached while trying to create: %s" % file_path
				file_id = None
			return(result, file_id)
		except Exception as error:
			counts['errors'] += 1
			result = "UNCAUGHT ERROR: %s" % str(error)
			return(result, None)

	else:
		result = "File '%s' is larger than 15gb." % str(file_path)
		counts['oversize'] += 1
		return(result, None)
		
def upload_to_box(s_input_folder, parent_id):
	folder_list = {0: {s_input_folder: parent_id}} # we'll use this to track the IDs for folders on Box
		
	for root, directories, files in os.walk(s_input_folder,topdown=True):
		for name in directories:
			current_path = os.path.join(root,name)
			
			if name.startswith('._'):
				ignored_files_and_folders.append(name)
			
			if any(ignored in current_path for ignored in ignored_files_and_folders):
				result = "Folder '%s' skipped." % current_path
				counts['skipped'] += 1
				update_log(result)
			else:
				parent_level = root.count(os.sep) - s_input_folder.count(os.sep)
				current_level = parent_level + 1
				try:
					parent_id = folder_list[parent_level][root]
					result, folder_id = create_folder(current_path, sanitize(name), parent_id, 1)
				except KeyError:
					counts['errors'] += 1
					result = "ERROR: I don't have an ID for this folder: %s" % root
					folder_id = None
				
				if folder_id is not None:
					if current_level in folder_list.keys():
						folder_list[current_level].update({current_path: folder_id})
					else:
						folder_list[current_level] = {current_path: folder_id}
				
				update_log(result)
			
		for name in files:
			current_path = os.path.join(root,name)
			
			if name.startswith('._'):
				ignored_files_and_folders.append(name)
			
			if any(ignored in current_path for ignored in ignored_files_and_folders):
				result = "File '%s' skipped." % current_path
				counts['skipped'] += 1
				update_log(result)
			else:
				parent_level = root.count(os.sep) - s_input_folder.count(os.sep)
				current_level = parent_level + 1
				try:
					parent_id = folder_list[parent_level][root]
					result, file_id = create_file(current_path, sanitize(name), parent_id, 1)
				except KeyError:
					counts['errors'] += 1
					result = "ERROR: I don't have a Parent ID for this file: %s" % name
					file_id = None
				
				update_log(result)
		
		# after we go back up a level, forget the IDs for sub-folders						
		max_level = max(folder_list.keys())
		if max_level > current_level:
			folder_list.pop(max_level)
									
if __name__ == '__main__':
	if "uploader_id" not in globals():
		if "app_users" in globals():
			which_user = raw_input("\nUploader to use? (%s) " % ', '.join(sorted(app_users.keys())))
			try:
				uploader_id = app_users[which_user]
			except:
				raise SystemExit("\nERROR: Invalid uploader!")
		else:
			uploader_id = raw_input("\nUploader ID? ")

	try:
		client, user = box_auth(uploader_id)
		uploader_name = client.as_user(user).user().get()['name']
	except:
		raise SystemExit("\nERROR: Invalid uploader ID!")
	
	print("\nUploader set to: %s" % uploader_name)
	
	log_folder = client.as_user(user).folder(log_file_id)
	logdir = (os.sep).join((os.path.dirname(os.path.abspath(__file__)), "logs"))
	
	if not os.path.exists(logdir):
		try:
			os.makedirs(logdir)
		except:
			raise SystemExit("\nERROR: Unable to create log folder!")
			
	if "home_folder_id" not in globals():
		home_folder_id = raw_input("\nID for home folder on Box? ")
	
	try:
		home_folder_name = client.as_user(user).folder(home_folder_id).get()['name']
		print("\nHome folder set to: " + home_folder_name)
	except:
		raise SystemExit("\nERROR: Invalid home folder ID!\n")
	
	if "s_input_folder" not in globals():
		s_input_folder = raw_input("\nPath to local folder? ")

	if os.path.isdir(s_input_folder):		

		if "top_level_name" not in globals():
			top_level_name = raw_input("\nFolder to create on Box? ")
		
		top_level_name = sanitize(top_level_name)
					
		start = time.time()
			
		summary = ("Uploading from: " + s_input_folder + "\n"
			+ "To top-level folder: " + top_level_name + "\n"
			+ "Inside home folder: " + home_folder_name + "\n"
			+ "Uploading as user: " + uploader_name + "\n\n")
			
		time_stamp = time.strftime('%Y-%m-%d %H%M%S')
		log_name =  "%s - %s - %s.txt" % (uploader_name, top_level_name, time_stamp)
		log_path = (os.sep).join((logdir, log_name))
		
		try:
			upload_log = open(log_path, "a")
		except:
			raise SystemExit("ERROR: Unable to write log file!")
			
		print("\n" + summary)
		upload_log.write(summary)
		
		try:
			# Create the top-level folder
			top_level_result, top_level_id = create_folder(s_input_folder, top_level_name, home_folder_id, 1)
		except:
			raise SystemExit("\nERROR: Unable to create top-level folder!\n")
		
		update_log(top_level_result)

		# Perform the recursive upload
		upload_to_box(s_input_folder, top_level_id) 

		#Wrap everything up and finish off the log file
		all_counts = ("\nFiles uploaded: " + str(counts['files_uploaded'])
			+ "\nExisting files: " + str(counts['files_existing'])
			+ "\nFolders created: " + str(counts['folders_created'])
			+ "\nExisting folders: " + str(counts['folders_existing'])
			+ "\nSkipped: " + str(counts['skipped'])
			+ "\nFiles larger than 15gb: " + str(counts['oversize'])
			+ "\nErrors: " + str(counts['errors']))

		print(all_counts)
		upload_log.write(all_counts)

		elapsed = "\nTime elapsed: " + str(datetime.timedelta(seconds=(time.time() - start)))
		print(elapsed)
		upload_log.write(elapsed)
		
		upload_log.close()

		# Upload the log file to Box
		result = send_log_to_box(log_path, log_name, 1)

		print(result)
	else:
		raise SystemExit("\nERROR: Local folder does not exist!\n")