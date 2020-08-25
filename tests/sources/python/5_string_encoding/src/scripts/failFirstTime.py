import os
import sys
import stat

received = str(sys.argv[-1])

print("This is a script that checks that the string received as a parameter")
print("and fails the first time. This forces the runtime to resubmit the task")
print("and the second time it will not fail.")
print("String received: " + received)

exit_code = 0
if received != "testing string":
    print("ERROR: The string received does NOT match the expected string.")
    exit_code = 1
else:
    print("Received the expeted string: OK.")
    exit_code = 0

check_file = "/tmp/has_failed.task"

if os.path.isfile(check_file):
  # If file exists == OK
  os.remove(check_file)   # Clean on Resubmission
  if exit_code == 0:
      exit_code = 0
  else:
      exit_code = 1
else:
  # If file does not exist == FAIL
  with open(check_file, 'w') as f:
      f.write(received)
  exit_code = 1

print("Executing python script.")
file = open('executor.sh','w')
file.write('#!/bin/sh\n')
file.write('echo "Executing binary for auto"\n')
file.write('exit ' + str(exit_code))
file.close()
os.chmod('executor.sh', stat.S_IRWXU)
