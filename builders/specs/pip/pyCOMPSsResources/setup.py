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
target_path = "/opt"

def check_system():
	return # TEMPORARY!!! REMOVE LATER!!!
        '''
                Checks that we have a proper python version and a
                proper OS (i.e: not windows)
        '''
        if sys.version_info[:2] != (2, 7):
                raise Exception('COMPSs does not support Python version %s'%sys.version)
        if 'win' in sys.platform:
                raise Exception('COMPSs does not support Windows')

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
	C extension for pyCOMPSs. Declaring it that way allows us
	to compile and place it with setup.py build.
	This extension will generate a Python extension named compss
	and will place a compss.so file in our site-packages.
	This file will be properly deleted with pip uninstall.
'''
compssmodule = Extension('compss',
        include_dirs = [
            os.path.join(bindings_location, 'bindings-common', 'src'),
            os.path.join(bindings_location, 'bindings-common', 'include')
            ],
        library_dirs = [
            os.path.join(bindings_location, 'bindings-common', 'lib')
            ],
        libraries = ['bindings_common'],
        extra_compile_args = ['-fPIC'],
        sources = [os.path.join('src','ext','compssmodule.c')]
)

'''
	Setup function.
'''
setup (name='pycompss',
	version=open('VERSION.txt').read().rstrip(),
	description='Python Binding for COMP Superscalar Runtime',
	long_description=open('README.txt').read(),
	author='The COMPSs team',
	author_email='distributed_computing@bsc.es',
	maintainer='The COMPSs team',
	maintainer_email='distributed_computing@bsc.es',
	url='http://compss.bsc.es',
	classifiers = [
        	'Development Status :: 5 - Stable',
        	'Intended Audience :: Developers',
        	'License :: OSI Approved :: Apache 2.0',
        	'Programming Language :: Python :: 2.7'
	],
	install_requires=['wget'],
	license='Apache 2.0',
	platforms=['Linux', 'Mac OS-X']
	)

