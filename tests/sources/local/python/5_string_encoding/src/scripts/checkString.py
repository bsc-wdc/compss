import os
import sys
import stat

received = str(sys.argv[-1])

print("This is a script that checks that the string received as a parameter:")
print("String received: " + received)

exit_code = 0
if received != "testing string":
    print("ERROR: The string received does NOT match the expected string.")
    exit_code = 1
else:
    print("Received the expeted string: OK.")
    exit_code = 0

print("Executing python script.")
file = open('executor.sh','w')
file.write('#!/bin/sh\n')
file.write('echo "Executing binary for auto"\n')
file.write('exit ' + str(exit_code))
file.close()
os.chmod('executor.sh', stat.S_IRWXU)
