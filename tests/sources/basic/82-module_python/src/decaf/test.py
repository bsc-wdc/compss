# a small 2-node example, just a producer and consumer

import os
import stat
import sys 


print("Executing python script.")

file = open('test.sh','w')
file.write('#!/bin/sh\n') 
file.write('echo "Executing binary" >'+sys.argv[-1][8:-1]+'\n') 
file.close()
os.chmod('test.sh', stat.S_IRWXU)
