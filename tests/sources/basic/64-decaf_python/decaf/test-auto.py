# a small 2-node example, just a producer and consumer

import os
import stat

print("Executing python script.")

file = open('executor.sh','w')
file.write('#!/bin/sh\n') 
file.write('echo "Executing binary"\n') 
file.close()
os.chmod('test.sh', stat.S_IRWXU)
