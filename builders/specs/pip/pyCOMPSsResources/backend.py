#!/usr/bin/python
import os
import sys
import wget
import site
import shutil
import tarfile
import subprocess

'''
  This is a function that downloads the COMPSs installable from the specified repository and version
  and installs it. It is intended to be called from the setup.py script when we are installing.  
  It also leaves a script on /etc/profile.d/compss.sh equivalent to the "compssenv" one on
  the supercomputers packages.

  Since this script calls another script which calls another script which calls... it may be
  possible that not all errors will be properly caught, leading to incomplete installations.
'''

def install(target_path):
	base_url = open('url','r').read().rstrip()
	version_name = open('VERSION.txt','r').read().rstrip()
	bulk_name = 'COMPSs_{0}.tar.gz'.format(version_name)

	full_url = os.path.join(base_url, bulk_name)

	file_name = wget.download(full_url)

	tar = tarfile.open(file_name, 'r:gz')
	tar.extractall('.')

	if subprocess.call(['./COMPSs/install', target_path]) != 0:
		raise
	
	pref = os.path.join(target_path, 'COMPSs')
	try:
		s="export IT_HOME=%s\n\
export PATH=$PATH:%s/Runtime/scripts/user\n\
export CLASSPATH=$CLASSPATH:%s/Runtime/compss-engine.jar\n\
export PATH=$PATH:%s/Bindings/c/bin\n\
export PYTHONPATH=$PYTHONPATH:%s/pycompss"%(pref, pref, pref, pref, site.getsitepackages()[0])
		open('/etc/profile.d/compss.sh', 'w').write(s)
	except:
		print ('Unable to copy compsenvv to /etc/profile.d/compss.sh. Please, do it manually.')

if __name__ == "__main__":
	install()
