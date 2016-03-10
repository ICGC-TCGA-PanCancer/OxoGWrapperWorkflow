#! /usr/bin/python

import os
import sys
import random
import time
import subprocess
import json
import datetime
import math

#Args:
# 1. path to root-dir in repo
# 2. source directory to move from
# 3. destination directory to move to
# 4. name of file to move
# 5. test mode - True => actual move in git; False => filesystem move only.
# 6. ip address - the IP address of this machine. Will be injected into JSON as 'host_ip'.
#Example uage:
#python git_move.py /datastore/gitroot/oxog-opts/aws-jobs/ queued-jobs downloading-jobs SomeJobFile.json False  

args = sys.argv
repo_location = args[1]
src_dir = args[2]
dest_dir = args[3]
file_name = args[4]
test_mode = (str(args[5])).lower()
#if len(args) >= 7: 
#    ip_address = args[6];

# Need to inject the host IP address into the JSON. And the current timestamp.
#if ip_address is not None:
#    with open(os.path.join(repo_location, src_dir , file_name),'r+') as jsonFile:
#        data = json.load(jsonFile);
#        data['host_ip'] = ip_address;
#        data['transition_to_'+dest_dir+'_time'] = datetime.datetime.now().isoformat();
#        json.dump(data, jsonFile);
# Get datetime with bash: `TZ=EST date +"%Y-%m-%d_%H:%M:%S_%Z"`
# Get IP address with bash: hostname -i or try:
#     ip addr show eth0 | grep "inet " | sed 's/.*inet \(.*\)\/.*/\1/g'

# TODO: There should be something in here to set the git config username and email. If a workflow is retried, the values set previously
# will have been lost since they were set in a different docker container.

full_path_to_src = os.path.join(repo_location, src_dir, file_name)
full_path_to_dest = os.path.join(repo_location, dest_dir, file_name)

print("Getting ready to move "+full_path_to_src+" to "+full_path_to_dest)

command = 'cd {} && '.format(repo_location)
if test_mode == 'true' :
    print ("In test mode - file will only be moved locally.")

    command = command + 'mv {} {}'.format(full_path_to_src, full_path_to_dest)
else:
    print ("In \"live\" mode - files will be moved in git")
    command = command + ' git checkout master && git reset --hard origin/master && git pull && git mv {} {} && '.format(full_path_to_src, full_path_to_dest) + \
                  ' git status && git commit -m \'{} to {}: {} \' && '.format(src_dir,dest_dir,file_name) + \
                  ' git push'
    
for i in range(60): # try up to 60 times. If there are MANY clients trying to check-in at once this might be necessary. 
    exit_code = 0
    sleepAmt = random.uniform(0,(2*i)+5)
    time.sleep(sleepAmt)
    print ("git mv attempt #"+str(i)+ ", after sleeping for "+str(sleepAmt)+" seconds.")
    print("Command to execute will be:\n"+command+"\n\n")
    if os.path.isfile(full_path_to_src):
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
        out, err = process.communicate()
    
        if process.returncode == 0 :
            if dest_dir == 'failed-jobs':
                print ("moved to failed, exiting script with error code 1 to interrupt workflow!")
                exit_code = 1
            else:
                # succeeded
                exit_code = 0
            sys.exit(exit_code)
        else:
            print('Error while moving the file: '+file_name+'.\nError message: {}\n\nRetrying...'.format(err))

            if test_mode == 'true' :
                print("In TEST mode, so file move will *not* be retried. Exiting without error, so workflow can continue.")
                exit_code = 0
                sys.exit(exit_code)
    else:
        print ("The check to see if "+full_path_to_src+" is a valid file failed, BUT that might not be an error: Please check that another process (or the same process, re-trying multiple times) hasn't already moved the file.")
        if dest_dir == 'failed-jobs':
            print ("Error: Can't move to failed-jobs because source file could not be found!")
            exit_code = 1
        else:
            exit_code = 0
        sys.exit(exit_code)