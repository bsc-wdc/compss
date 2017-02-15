import subprocess
import platform
import site
import sys
import os
from backend import install as backend_install
from distutils.core import setup, Extension
from distutils.command.install import install
from distutils import log

'''
	Distutils installer. This script will be called by pip when:
	- We want to create a distributable (sdist) tar.gz
	- We want to build the C extension (build and build_ext)
	- We want to install pyCOMPSs (install)


	It can be invoked to do these functionalities with
	python setup.py (install|sdist|build)
'''

bindings_location = os.path.join('COMPSs', 'Bindings')
target_path = os.path.join(site.getsitepackages()[0], 'pycompss')

def check_system():
        '''
                Check that we have a proper python version and a
                proper OS (i.e: not windows)
                Also, check that we have JAVA_HOME defined
        '''
        # check Python version
        assert sys.version_info[:2] == (2, 7), 'COMPSs does not support Python version %s, only Python 2.7.x is supported'%sys.version
        # check os version
        assert 'win' not in sys.platform, 'COMPSs does not support Windows'
        # check we have JAVA_HOME defined
        assert 'JAVA_HOME' in os.environ, 'JAVA_HOME is not defined'

'''
	Pre-install operation: download and install COMPSs
	This will try to stop the installation if some error is
	found during that part. However, some sub-scripts dont
	propagate the errors they find, so there is not absolute
	guarantee that this script will lead to a perfect, clean
	installation.
'''
if 'install' in sys.argv[1:]:
	check_system()
	backend_install(target_path)

'''
	Setup function.
'''
setup (name='pycompss',
	version=open('VERSION.txt').read().rstrip(),
	description='Python Binding for COMP Superscalar Runtime',
	long_description=open('README.txt').read(),
	author='The COMPSs team',
	author_email='support-compss@bsc.es',
	maintainer='The COMPSs team',
	maintainer_email='support-compss@bsc.es',
	url='http://compss.bsc.es',
	classifiers = [
        	'Development Status :: 5 - Production/Stable',
			'Environment :: Console',
        	'Intended Audience :: Developers',
        	'Intended Audience :: Science/Research',
        	'License :: OSI Approved :: Apache Software License',
			'Operating System :: POSIX :: Linux',
			'Operating System :: Unix',
        	'Programming Language :: C',
        	'Programming Language :: C++',
        	'Programming Language :: Java',
            'Programming Language :: Python :: 2.7',
        	'Topic :: Software Development',
			'Topic :: Scientific/Engineering',
			'Topic :: System :: Distributed Computing',
			'Topic :: Utilities'
	],
	license='Apache 2.0',
	platforms=['Linux']
	)
