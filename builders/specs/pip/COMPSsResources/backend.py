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
	# download and decompress the COMPSs_version.tar.gz file	
	base_url = open('url','r').read().rstrip()
	version_name = open('VERSION.txt','r').read().rstrip()
	bulk_name = 'COMPSs_{0}.tar.gz'.format(version_name)

	full_url = os.path.join(base_url, bulk_name)

	file_name = wget.download(full_url)


	tar = tarfile.open(file_name, 'r:gz')
	tar.extractall('.')

	os.remove(file_name)

	# call the SuperComputers install script
	if subprocess.call(['./COMPSs/install', target_path]) != 0:
		raise Exception('COMPSs install script ended with some error. Plase check stdout & stderr for more info.')
	
	pref = os.path.join(target_path, 'COMPSs')

	# create a script that defines some environment variables
	# if called as root and pip does not detect compss as an installed module
	# it will delete COMPSs
	substitution_map = {
		'##VERSION##': str(sys.version_info[0])+'.'+str(sys.version_info[1]),
		'##SITE_PACKAGES##': site.getsitepackages()[0],
		'##COMPSS_PATH##': pref
	}
	s = open('compssenv', 'r').read()
	for (key, value) in substitution_map.items():
		s = s.replace(key, value)
	open('/etc/profile.d/compss.sh', 'w').write(s)

	# create symbolic links to the python package and its C extension, respectively
	target_files = ['pycompss', 'compss.so']
	for target_file in target_files:
		try:
			os.remove(os.path.join(site.getsitepackages()[0], target_file))
		except:
			os.symlink(os.path.join(pref, "Bindings", "python", target_file), os.path.join(site.getsitepackages()[0], target_file))

