#
#  Copyright 2.02-2017 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
from distutils.core import setup, Extension
#from setuptools import setup, Extension
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
	version='2.0.rc1702',
	description='Python Binding for COMP Superscalar Runtime',
	long_description=open('README.txt').read(),
	author='Javier Conejero',
	author_email='francisco.conejero@bsc.es',
	url='http://compss.bsc.es',
	license='Apache 2.0.rc1702',
    package_dir={'pycompss':'src/pycompss'},
	packages=['', 'pycompss', 'pycompss.api', 'pycompss.runtime', 'pycompss.worker', 'pycompss.storage', 'pycompss.util', 'pycompss.util.serialization', 'pycompss.api.dummy', 'pycompss.functions', 'pycompss.matlib', 'pycompss.matlib.algebra', 'pycompss.matlib.classification', 'pycompss.matlib.clustering'],
	package_data={'' : ['log/logging.json', 'log/logging.json.debug', 'log/logging.json.off', 'bin/worker_python.sh']},
	ext_modules=[compssmodule])

