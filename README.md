# Box Upload - Recursive

This is a Python script that uses os.walk and the [Box SDK for Python][1] to upload files to Box.

First, install the SDK with jwt authentication:

	pip install boxsdk[jwt] --pre

Before you can use this script, you'll need to create an app at the [Box Developer][2] website.

Select OAuth 2.0 with JWT and download the JSON file generated by the developer console. Then, authorize the app on your enterprise. The [JWT App Primer for Box][4] has detailed instructions.

Then, plug those values into the script:

	auth = JWTAuth(
		client_id='YOUR_CLIENT_ID',
		client_secret='YOUR_CLIENT_SECRET',
		enterprise_id='YOUR_ENTERPRISE_ID',
		jwt_key_id='YOUR_JWT_KEY_ID',
		rsa_private_key_passphrase='RSA_PRIVATE_KEY_PASSPHRASE',
		rsa_private_key_file_sys_path='PATH_TO_KEY_FILE'
	)

I created a separate RSA key file from the JSON, so I had to specify the key file path.

You'll also need to create one or more [App User(s)][3] and grant them access to the folder where you want the script to upload files.

	app_users = {'1':'APP_USER_ID_HERE', '2':'APP_USER_ID_HERE', '3':'APP_USER_ID_HERE'} # Add as many app users as you want here

The script also uploads a log file to Box at intervals, so you'll need to specify an ID for that folder and grant your App User access:

	log_file_id = 'LOG_FOLDER_ID_HERE' # ID for the folder where you want to store log files
	
The [Box CLI][5] was very helpful. I used it to generate my App Users.

[1]: https://github.com/box/box-python-sdk
[2]: https://developer.box.com
[3]: https://github.com/box/box-python-sdk#box-developer-edition
[4]: https://github.com/box-community/jwt-app-primer
[5]: https://developer.box.com/docs/box-cli
