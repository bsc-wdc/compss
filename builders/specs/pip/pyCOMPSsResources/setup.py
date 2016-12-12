import subprocess
import platform
import site
import sys
import os
from distutils.core import setup, Extension
from distutils.command.install import install
from distutils import log

'''
	Distutils installer. This script will be called by pip when:
	- We want to create a distributable (dist) tar.gz
	- We want to build the C extension (build)
	- We want to install pyCOMPSs (install)

	It can be invoked to do these functionalities with
	python setup.py (install|dist|build)
'''

bindings_location = os.path.join('COMPSs', 'Bindings')

def check_dependencies():
	pass

target_path = os.path.join(site.getsitepackages()[0], 'pycompss')

'''
	Pre-install operation: download and install COMPSs
	This will try to stop the installation if some error is
	found during that part. However, some sub-scripts don't
	propagate the errors they find, so there is not absolute
	guarantee that this script will lead to a perfect, clean
	installation.
'''
if 'install' in sys.argv:
	check_dependencies()
	if subprocess.call(['python', 'backend.py', target_path]) != 0:
		print ('Something went wrong during COMPSs install process. Now leaving...')
		exit(1)

'''
	C extension for pyCOMPSs. Declaring it that way allows us
	to compile and place it with setup.py build	
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
	url='http://compss.bsc.es',
	install_requires=['wget'],
	license='Apache 2.0',
	package_dir={'pycompss':os.path.join('src','pycompss')},
	packages=['pycompss', 'pycompss.api', 'pycompss.runtime', 'pycompss.worker', 'pycompss.storage', 'pycompss.util', 'pycompss.util.serialization', 'pycompss.api.dummy', 'pycompss.functions', 'pycompss.matlib', 'pycompss.matlib.algebra', 'pycompss.matlib.classification', 'pycompss.matlib.clustering'],
	package_data={'' : [os.path.join('log','logging.json'), os.path.join('log','logging.json.debug'), os.path.join('log','logging.json.off'), os.path.join('bin','worker_python.sh')]},
	ext_modules=[compssmodule])

