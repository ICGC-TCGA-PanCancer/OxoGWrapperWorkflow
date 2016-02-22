#! /usr/bin/python

import os
import sys
import random
import time
import subprocess
import json
import datetime

#Args:
# 1. path to root-dir in repo
# 2. source directory to move from
# 3. destination directory to move to
# 4. name of file to move
# 5. test mode - True => actual move in git; False => filesystem move only.
# 6. ip address - the IP address of this machine. Will be injected into JSON as 'host_ip'.
#Example uage:
#python git_move.py /datastore/gitroot/oxog-opts/aws-jobs/ queued-jobs downloading-jobs SomeJobFile.json False  

args = sys.argv;
repo_location = args[1];
src_dir = args[2];
dest_dir = args[3];
file_name = args[4];
test_mode = args[5];
#if len(args) >= 7: 
#    ip_address = args[6];

# Need to inject the host IP address into the JSON. And the current timestamp.
#if ip_address is not None:
#    with open(os.path.join(repo_location, src_dir , file_name),'r+') as jsonFile:
#        data = json.load(jsonFile);
#        data['host_ip'] = ip_address;
#        data['transition_to_'+dest_dir+'_time'] = datetime.datetime.now().isoformat();
#        json.dump(data, jsonFile);

# TODO: There should be something in here to set the git config username and email. If a workflow is retried, the values set previously
# will have been lost since they were set in a different docker container.

full_path_to_src = os.path.join(repo_location, src_dir, file_name)
full_path_to_dest = os.path.join(repo_location, dest_dir, file_name)

print("Getting ready to move "+full_path_to_src+" to "+full_path_to_dest);

move_command = '';

if test_mode:
    print ("In test mode - file will only be moved locally.");

    move_command = 'mv {} {}'.format(full_path_to_src, full_path_to_dest);
else:
    print ("In \"live\" mode - files will be moved in git");
    move_command = 'git mv {} {} && '.format(full_path_to_src, full_path_to_dest) + \
                  'git commit -m \'{} to {}: {} \' && '.full_path_to_dest + \
                  'git push';
    
for i in range(10): # try 10 times. If there are MANY clients trying to check-in at once this might be necessary. 

    print ("git mv attempt #"+str(i));
    
    if os.path.isfile(full_path_to_src):

        if test_mode:
            command = 'cd {} ; '.format(repo_location) + \
                  move_command;
        else:
            command = 'cd {} ; '.format(repo_location) + \
                  'git checkout master ; ' + \
                  'git reset --hard origin/master ; ' + \
                  'git pull ; ' + \
                  move_command;
        
        
        print("Command to execute will be:\t\n"+command+"\n\n");
        process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        out, err = process.communicate()
    
        if process.returncode == 0 :
            if dest_dir == 'failed-jobs':
                print ("moved to failed, exiting script with error code 1 to interrupt workflow!");
                sys.exit(1);
            else:
                sys.exit(0);  # succeeded
        else:
            print('Error while moving the file: '+file_name+'.\nError message: {}\n\nRetrying...'.format(err))
            if not test_mode:
                # Only retry if we're not in test mode.
                time.sleep(randint(1,15))  # pause a few seconds before retry
            else:
                sys.exit(1);
    else:
        print ("File "+file_name+" was not in "+repo_location+"/"+src_dir+"/"+" but that might not be an error. Please check that another process hasn't already moved the file.");
        sys.exit(0);