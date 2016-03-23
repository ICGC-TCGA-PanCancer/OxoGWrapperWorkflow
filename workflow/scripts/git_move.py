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


full_path_to_src = os.path.join(repo_location, src_dir, file_name)
full_path_to_dest = os.path.join(repo_location, dest_dir, file_name)


# TODO: There should be something in here to set the git config username and email. If a workflow is retried, the values set previously
# will have been lost since they were set in a different docker container.

print("Getting ready to move "+full_path_to_src+" to "+full_path_to_dest)

pre_command = 'cd {} && '.format(repo_location)
mv_command = ''
if test_mode == 'true' :
    print ("In test mode - file will only be moved locally.")
    mv_command = mv_command + ' mv {} {}'.format(full_path_to_src, full_path_to_dest)
else:
    print ("In \"live\" mode - files will be moved in git")
    pre_command = pre_command + ' git checkout master && git reset --hard origin/master && git pull '
    mv_command = ' git mv {} {} && '.format(full_path_to_src, full_path_to_dest) + \
                  ' git status && git commit -m \'{} to {}: {} \' && '.format(src_dir,dest_dir,file_name) + \
                  ' git push'
    
for i in range(60): # try up to 60 times. If there are MANY clients trying to check-in at once this might be necessary.
    sleepAmt = random.uniform(0,(2*i)+5) #Increase max possible sleep time with each retry, makes it less likely that multiple machines will choose the same sleep time in the event of a collision.
    time.sleep(sleepAmt)
    print ("git mv attempt #"+str(i)+ ", after sleeping for "+str(sleepAmt)+" seconds.")
    print("pre-Command to execute will be:\n"+pre_command+"\n\n")
    
    process = subprocess.Popen(pre_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    out, err = process.communicate()

    print("Return code: "+str(process.returncode)+"\nCommand result:\n"+out) 

    transition_key = 'transition_to_'+dest_dir+'_time'
    transition_value = datetime.datetime.now().isoformat()
    print ("Updating JSON file with transtion timetamp:\n\t"+transition_key+":"+transition_value)
    data = {}
    with open(full_path_to_src,'r') as jsonFile:
        data = json.load(jsonFile)

    data[transition_key] = transition_value
    
    with open(full_path_to_src,'w+') as jsonFile:
        json.dump(data, jsonFile)

    print("move command to execute will be:\n"+mv_command+"\n\n")
    
    process = subprocess.Popen(mv_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    out, err = process.communicate()

    print("Return code: "+str(process.returncode)+"\nCommand result:\n"+out)    

    if process.returncode == 0 :
        if 'failed-jobs' in full_path_to_dest:
            print ("moved to failed, exiting script with error code 1 to interrupt workflow!")
            sys.exit(1)
        else:
            sys.exit(0)
    else:
        print('Error while moving the file: '+file_name+'.\nError message: {}\n\nRetrying...'.format(err))
        if test_mode == 'true' :
            print("In TEST mode, so file move will *not* be retried. Exiting without error, so workflow can continue.")
            sys.exit(0)
    