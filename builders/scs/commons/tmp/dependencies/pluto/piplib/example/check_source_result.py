#!/usr/bin/python 


import sys
import os
import subprocess
import string


# Check arguments
if (len(sys.argv) < 3):
	print 'Usage:', sys.argv[0], 'file.dat file.ll [pip_path]'
	sys.exit(2)

# Get arguments
source_filename = sys.argv[1]
ll_filename = sys.argv[2]
	
# Display arguments
print sys.argv[0], source_filename, ll_filename

# Check if source_filename and ll_filename exist
if (os.path.exists(source_filename) == False):
	print 'Error:', source_filename, ' file does not exist'
	sys.exit(3)
if (os.path.exists(ll_filename) == False):
	print 'Error:', ll_filename, ' file does not exist'
	sys.exit(4)

# Custom pip
pip = ""
if (len(sys.argv) >= 4):# and os.path.exists(sys.argv[3]) and os.access(sys.argv[3], os.X_OK)):
	pip = sys.argv[3]
else:
	pip = "exemple32"
# Final pip
print "pip =", pip


# Get source
source_file = open(source_filename, 'r')
source = ""
for line in source_file:
	#sys.stdout.write(line)
	source += line
source_file.close()

# Get ll form pip
pip_output = subprocess.Popen([pip, source_filename], shell = True, stdout = subprocess.PIPE, stdin = subprocess.PIPE)
pip_output_string = pip_output.communicate(input=source)[0]
pip_output.stdin.close()
#print pip_output

# Get ll
ll_file = open(ll_filename, 'r')
ll = ""
for line in ll_file:
	#sys.stdout.write(line)
	ll += line
ll_file.close()


# Compare pip_output and ll
s0 = ""
s1 = ""
# Remove empty line
for line in string.split(pip_output_string, '\n'):
	line = line.replace(' ', '')
	if line != '': s0 += line + '\n'
for line in string.split(ll, '\n'):
	line = line.replace(' ', '')
	if line != '': s1 += line + '\n'
# Print
print s0
print s1
# Result
if (s0 != s1):
	print 'Result:', '"' + pip, '<', source_filename + '"', 'and', '"' + ll_filename + '"', 'are different'
	sys.exit(1)
else:
	print 'Result:', '"' + pip, '<', source_filename + '"', 'and', '"' + ll_filename + '"', 'have no difference'

# End
sys.exit(0)
