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
                Checks that we have a proper python version and a
                proper OS (i.e: not windows)
        '''
        if sys.version_info[:2] != (2, 7):
                raise Exception('COMPSs does not support Python version %s', sys.version)
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
	try:
		backend_install(target_path)
	except:
		raise Exception('Something went wrong during COMPSs installation process.')


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
	platforms=['Linux', 'Mac OS-X'],
	package_dir={'pycompss':os.path.join('src','pycompss')},
	packages=['pycompss', 'pycompss.api', 'pycompss.runtime', 'pycompss.worker', 'pycompss.storage', 'pycompss.util', 'pycompss.util.serialization', 'pycompss.api.dummy', 'pycompss.functions', 'pycompss.matlib', 'pycompss.matlib.algebra', 'pycompss.matlib.classification', 'pycompss.matlib.clustering'],
	package_data={'' : [os.path.join('log','logging.json'), os.path.join('log','logging.json.debug'), os.path.join('log','logging.json.off'), os.path.join('bin','worker_python.sh')]},
	ext_modules=[compssmodule])

