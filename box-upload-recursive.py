# This script relies on the Box Python SDK:
# pip install boxsdk[jwt]==2.0.0a11

import os
import time

from boxsdk import JWTAuth
from boxsdk import Client

uploader_id = 'APP_USER_ID_HERE' # ID for the app user account used for the upload
home_folder_id = 'HOME_FOLDER_ID_HERE' # ID for the overall parent folder where you're uploading
log_file_id = 'LOG_FOLDER_ID_HERE' # ID for the folder where you want to store log files
box_size_limit = 16106127360 # 15gb in bytes
log_batch = 100000 # We're going to upload the log every X records

d_invalid_char_replace= { "/" : "_", "?": "X", "<": "(", ">": ")", "\\": "#", ":":"_", "*": "-", "|": "!", "\"" :""}
ignored_files_and_folders = ['.DS_Store', '.Trash', '.Spotlight-V100', '._', '_gsdata_'] # Mac hidden files

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
	if upload_log:
		upload_log.write(time.strftime('%H:%M:%S') + ": " + result + '\n')
		
		total_counts = sum(counts.values())
		
		if total_counts % log_batch == 0:
			# Send the current log file to Box so that we get an in-progress view
			log = send_log_to_box(log_path, log_name)
			print("Sent log file to Box.")
		
	print(result)
	
def send_log_to_box(log_path, log_name):
	try:
		upload_the_log = log_folder.upload(log_path, log_name, upload_using_accelerator=True)
		result = "\nUploaded log file '" + log_name + "' to Box."
	except Exception as error:
		if error.code == "item_name_in_use":
			# The file already exists, so let's upload a new version
			existing_id = error.context_info['conflicts']['id']
			existing_file = client.as_user(user).file(existing_id).get()
			update_file = existing_file.update_contents(log_path, upload_using_accelerator=True)
			result = "\nUpdated log file '" + log_name + "' on Box."
		else:
			# Some other error thrown
			result =  "\nUnable to upload log: " + error.message

	return(result)
	
def sanitize(sanitized_name):
	for i, j in d_invalid_char_replace.iteritems():
		sanitized_name = sanitized_name.replace(i, j)
	sanitized_name = sanitized_name.rstrip('. ') # strip periods and spaces from the end

	return sanitized_name

def create_folder(current_path, folder_name, parent_id):
	try:
		new_folder = client.as_user(user).folder(parent_id).create_subfolder(folder_name)
		result = "Folder '" + current_path + "' successfully created as '" + new_folder['name'] + "'"
		counts['folders_created'] += 1
		return(result, new_folder['id'])
	except Exception as error:
		if error.code == "item_name_in_use":
			existing_id = error.context_info['conflicts'][0]['id']
			existing_folder = client.as_user(user).folder(existing_id).get()
			result = "Folder '" + current_path + "' already exists as '" + existing_folder['name'] + "'"
			counts['folders_existing'] += 1
			return(result, existing_id)
		else:
			counts['errors'] += 1
			return("Unable to create '" + folder_name + "': " + error.message, False)

def create_file(file_path, file_name, parent_id):
	root_folder = client.as_user(user).folder(parent_id)
	file_size_local = os.path.getsize(file_path)
	if file_size_local <= box_size_limit:
		try:
			a_file = root_folder.upload(file_path, file_name, upload_using_accelerator=True)
			result = "File '" + file_path + "' successfully created as '" + a_file['name'] + "'"
			counts['files_uploaded'] += 1
			return(result, a_file['id'])
		except Exception as error:
			if error.code == "item_name_in_use":
				existing_id = error.context_info['conflicts']['id']
				existing_file = client.as_user(user).file(existing_id).get()
				file_size_box = existing_file['size']
			
				if file_size_box == file_size_local:
					counts['files_existing'] += 1
					result = "File '" + file_path + "' already exists as '" + existing_file['name'] + "'"
				else:
					try:
						# There must be a partial upload, so let's upload a new version
						update_file = existing_file.update_contents(file_path, upload_using_accelerator=True)
						counts['files_existing'] += 1
						result = "File '" + file_path + "' already existed as '" + update_file['name'] + "' but the size didn't match, so I updated it."
					except Exception as error:
						counts['errors'] += 1
						result = "Unable to upload '"+ file_name + "': " + error.message
				return(result, existing_id)
			else:
				counts['errors'] += 1
				return("Unable to create '" + file_name + "': " + error.message, False)
	else:
		result = "File '" + file_path + "' is larger than 15gb."
		counts['oversize'] += 1
		return(result, False)
		
def upload_to_box(s_input_folder, parent_id):
	folder_list = {0: {s_input_folder: parent_id}} # we'll use this to track the IDs for folders on Box
		
	for root, directories, files in os.walk(s_input_folder,topdown=True):
		for name in directories:
			if any(ignored in name for ignored in ignored_files_and_folders):
				result = "Folder '" + name + "' skipped."
				counts['skipped'] += 1
				update_log(result)
			else:
				parent_level = root.count(os.sep) - s_input_folder.count(os.sep)					
				current_level = parent_level + 1
				current_path = os.path.join(root,name)
				parent_id = folder_list[parent_level][root]
				result, folder_id = create_folder(current_path, sanitize(name), parent_id)
				
				if current_level in folder_list.keys():
					folder_list[current_level].update({current_path: folder_id})
				else:
					folder_list[current_level] = {current_path: folder_id}
				
				update_log(result)
			
		for name in files:
			if any(ignored in name for ignored in ignored_files_and_folders):
				result = "File '" + name + "' skipped."
				counts['skipped'] += 1
				update_log(result)
			else:
				parent_level = root.count(os.sep) - s_input_folder.count(os.sep)
				current_level = parent_level + 1
				current_path = os.path.join(root,name)
				parent_id = folder_list[parent_level][root]
				result, file_id = create_file(current_path, sanitize(name), parent_id)
				
				update_log(result)
		
		# after we go back up a level, forget the IDs for sub-folders						
		max_level = max(folder_list.keys())
		if max_level > current_level:
			folder_list.pop(max_level)
									
if __name__ == '__main__':
	client, user = box_auth(uploader_id)
	
	ts = time.strftime('%m-%d-%Y-%I-%M-%S')
	log_name = "upload_log_" + ts + ".txt"
	log_folder = client.as_user(user).folder(log_file_id) 
	log_path = (os.sep).join((os.path.dirname(os.path.abspath(__file__)), "logs", log_name))
	
	try:
		upload_log = open(log_path, "a")
	except:
		raise SystemExit("\nUnable to write logfile. Make sure you have a folder called 'logs' in the same path as this script.\n")
	
	home_folder_id = raw_input("\nID for home folder on Box? ")
	
	try:
		home_folder_name = client.as_user(user).folder(home_folder_id).get()['name']
		print("\nHome folder set to: " + home_folder_name)
	except:
		raise SystemExit("\nInvalid home folder ID!\n")
	
	s_input_folder = raw_input("\nPath to local folder? ")

	if os.path.isdir(s_input_folder):		

		uploader_name = client.as_user(user).user().get()['name']
					
		top_level_name = raw_input("\nFolder to create on Box? ")
		
		try:
			# Create the top-level folder
			top_level_name = sanitize(top_level_name)
			top_level_result, top_level_id = create_folder(s_input_folder, top_level_name, home_folder_id)
		except:
			raise SystemExit("\nUnable to create top-level folder!\n")
			
		summary = ("Uploading from: " + s_input_folder + "\n"
			+ "To top-level folder: " + top_level_name + "\n"
			+ "Inside home folder: " + home_folder_name + "\n"
			+ "Uploading as user: " + uploader_name + "\n\n")
			
		print("\n" + summary)
		upload_log.write(summary)
		
		update_log(top_level_result)

		# Perform the recursive upload
		upload_to_box(s_input_folder, top_level_id) 

		all_counts = ("\nFiles uploaded: " + str(counts['files_uploaded'])
			+ "\nExisting files: " + str(counts['files_existing'])
			+ "\nFolders created: " + str(counts['folders_created'])
			+ "\nExisting folders: " + str(counts['folders_existing'])
			+ "\nSkipped: " + str(counts['skipped'])
			+ "\nFiles larger than 15gb: " + str(counts['oversize'])
			+ "\nErrors: " + str(counts['errors']))

		print(all_counts)

		upload_log.write(all_counts)

		upload_log.close()

		# Upload the log file to Box
		result = send_log_to_box(log_path, log_name)

		print(result)
	else:
		raise SystemExit("\nLocal folder does not exist!\n")
