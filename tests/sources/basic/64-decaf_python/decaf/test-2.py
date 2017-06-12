# a small 2-node example, just a producer and consumer

import os
import stat
import sys

print("Executing python script.")

file = open('test-2.sh','w')
file.write('#!/bin/sh\n') 
file.write('echo "Executing binary for 2" >'+sys.argv[-1]+'\n') 
file.close()
os.chmod('test-2.sh', stat.S_IRWXU)
