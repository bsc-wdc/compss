# a small 2-node example, just a producer and consumer

import os
import stat
import sys 

print("Executing python script.")

file = open('executor.sh','w')
file.write('#!/bin/sh\n')
print('echo "Executing binary for auto" > '+ sys.argv[-1][8:-1] +'\n')
file.write('echo "Executing binary for auto" > '+ sys.argv[-1][8:-1] +'\n') 
file.close()
os.chmod('executor.sh', stat.S_IRWXU)
