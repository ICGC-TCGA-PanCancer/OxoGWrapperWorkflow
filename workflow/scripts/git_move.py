#! /usr/bin/python

import os
import sys
import random
import time
import subprocess

#Args:
# 1. path to root-dir in repo
# 2. source directory to move from
# 3. destination directory to move to
# 4. name of file to move
# 5. test mode - True => actual move in git; False => filesystem move only.
#Example uage:
#python git_move.py /datastore/gitroot/oxog-opts/aws-jobs/ queued-jobs downloading-jobs SomeJobFile.json False  

args = sys.argv;
repo_location = args[1];
src_dir = args[2];
dest_dir = args[3];
file_name = args[4];
test_mode = args[5];

move_command = '';
if test_mode == True:
    move_command = 'mv {} {}'.format(os.path.join(repo_location, src_dir , file_name),
                                            os.path.join(repo_location, dest_dir , file_name));
else:
    move_command = 'git mv {} {} && '.format(os.path.join(repo_location, src_dir , file_name),
                                            os.path.join(repo_location, dest_dir , file_name)) + \
                  'git commit -m \'{} to {}: {} \' && '.format(src_dir,
                        dest_dir, file_name) + \
                  'git push';
    
for i in range(10): # try 10 times. If there are MANY clients trying to check-in at once this might be necessary. 

    print ("git mv attempt #"+str(i));

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
        break  # succeeded
    else:
        print('Error while moving the file: '+file_name+'.\nError message: {}\n\nRetrying...'.format(err))
        if test_mode == False:
            # Only retry if we're not in test mode.
            time.sleep(randint(1,15))  # pause a few seconds before retry
        else:
            sys.exit(1);