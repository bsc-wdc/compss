from distutils.core import setup, Extension
from distutils.command.install_lib import install_lib
from distutils import log

compssmodule = Extension('compss',
        include_dirs = [
            '../bindings-common/src',
		    '../bindings-common/include'
		],
        library_dirs = [
		    '../bindings-common/lib'
		],
        libraries = ['bindings_common'],
        extra_compile_args = ['-fPIC'],
        sources = ['src/ext/compssmodule.c'])

setup (name='pycompss',
	version='2.0',
	description='Python Binding for COMP Superscalar Runtime',
	long_description=open('README.txt').read(),
	author='Javier Conejero',
	author_email='francisco.conejero@bsc.es',
	url='http://compss.bsc.es',
	license='Apache 2.0',
    package_dir={'pycompss':'src/pycompss'},
	packages=['', 'pycompss', 'pycompss.api', 'pycompss.runtime', 'pycompss.worker', 'pycompss.storage', 'pycompss.util', 'pycompss.util.serialization', 'pycompss.api.dummy', 'pycompss.functions', 'pycompss.matlib', 'pycompss.matlib.algebra', 'pycompss.matlib.classification', 'pycompss.matlib.clustering'],
	package_data={'' : ['log/logging.json', 'log/logging.json.debug', 'log/logging.json.off', 'bin/worker_python.sh']},
	ext_modules=[compssmodule])

