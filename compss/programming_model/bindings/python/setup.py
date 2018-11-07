#!/usr/bin/python
#
#  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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

# -*- coding: utf-8 -*-

from distutils.core import setup, Extension
# from setuptools import setup, Extension
import re
import os


gcc_debug_flags = [
'-Wall',
'-Wextra',
'-pedantic',
'-O2',
'-Wshadow',
'-Wformat=2',
'-Wfloat-equal',
'-Wconversion',
'-Wlogical-op',
'-Wcast-qual',
'-Wcast-align',
'-D_GLIBCXX_DEBUG',
'-D_GLIBCXX_DEBUG_PEDANTIC',
'-D_FORTIFY_SOURCE=2',
'-fsanitize=address',
'-fstack-protector'
]


# Bindings common extension
compssmodule = Extension(
        'compss',
        include_dirs=[
                '../bindings-common/src',
                '../bindings-common/include',
                os.environ['JAVA_HOME']+'/include', 
                os.environ['JAVA_HOME']+'/include/linux/'
        ],
        library_dirs=[
                '../bindings-common/lib'
        ],
        libraries=['bindings_common'],
        extra_compile_args=['-fPIC', '-std=c++11'],
        #extra_compile_args=['-fPIC'],
        sources=['src/ext/compssmodule.cc']
)

# Thread affinity extension
thread_affinity = Extension(
        'thread_affinity',
        include_dirs=['src/ext'],
        extra_compile_args=['-std=c++11'],
        #extra_compile_args=['-fPIC %s' % (' '.join(gcc_debug_flags.split('\n')))],
        sources=['src/ext/thread_affinity.cc']
)


# Helper method to find packages
def find_packages(path='./src'):
        ret = []
        for root, _, files in os.walk(path, followlinks=True):
                if '__init__.py' in files:
                        # Erase src header from package name
                        pkg_name = root[6:]
                        # Replace / by .
                        pkg_name = pkg_name.replace('/', '.')
                        # Erase non UTF characters
                        pkg_name = re.sub('^[^A-z0-9_]+', '', pkg_name)
                        # Add package to list
                        ret.append(pkg_name)
        return ret


# Setup
setup(
        # Metadata
        name='pycompss',
        version='2.4',
        description='Python Binding for COMP Superscalar Runtime',
        long_description=open('README.txt').read(),
        author='Workflows and Distributed Computing Group (WDC) - Barcelona Supercomputing Center (BSC)',
        author_email='support-compss@bsc.es',
        url='https://compss.bsc.es',

        # License
        license='Apache 2.0',

        # Test
        tests_require=[
                'nose>=1.0',
                'coverage'
        ],
        test_suite='nose.collector',
        entry_points={
                'nose.plugins.0.10': ['nose_tests = nose_tests:ExtensionPlugin']
        },

        # Build
        package_dir={'pycompss': 'src/pycompss'},
        packages=[''] + find_packages(),
        package_data={
                '': ['log/logging.json', 'log/logging.json.debug', 'log/logging.json.off', 'README.md', 'tests/*']
        },
        ext_modules=[compssmodule, thread_affinity]
)

# Only available with setuptools
# entry_points={'console_scripts':['pycompss = pycompss.__main__:main']})
