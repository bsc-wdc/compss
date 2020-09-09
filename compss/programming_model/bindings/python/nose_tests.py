#!/usr/bin/python
#
#  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

# Imports
import nose
import sys
from nose.plugins.base import Plugin


DIRECTORIES_WHITE_LIST = [
    'src',
    'pycompss',
    'pycompss/tests',
    'pycompss/tests/api',
    'pycompss/tests/api/commons',
    'pycompss/tests/functions',
    'pycompss/tests/integration',
    'pycompss/tests/main',
    'pycompss/tests/runtime',
    'pycompss/tests/util',
    'pycompss/tests/worker',
    # ############ @parallel related ############ #
    'pycompss/api',
    'pycompss/util',
    'pycompss/util/translators',
    'pycompss/util/translators/code_loader',
    'pycompss/util/translators/code_replacer',
    'pycompss/util/translators/py2pycompss',
    'pycompss/util/translators/py2scop',
    'pycompss/util/translators/scop2pscop2py',
    'pycompss/util/translators/scop_types',
    'pycompss/util/translators/scop_types/scop',
    'pycompss/util/translators/scop_types/scop/extensions',
    'pycompss/util/translators/scop_types/scop/globl',
    'pycompss/util/translators/scop_types/scop/globl/parameters',
    'pycompss/util/translators/scop_types/scop/statement'
]
FILES_WHITE_LIST = [
    # Include all tests that check only the sources.
    # (Do not include tests that use the installed runtime. Use
    # INTEGRATION_WHITE_LIST instead of this)
    'pycompss/tests/main/test_main.py',
    'pycompss/tests/api/test_api.py',
    'pycompss/tests/api/test_binary.py',
    'pycompss/tests/api/test_compss.py',
    'pycompss/tests/api/test_constraint.py',
    'pycompss/tests/api/test_decaf.py',
    'pycompss/tests/api/test_decorator.py',
    'pycompss/tests/api/test_err_msgs.py',
    'pycompss/tests/api/test_exceptions.py',
    'pycompss/tests/api/test_implement.py',
    'pycompss/tests/api/test_io.py',
    'pycompss/tests/api/test_local.py',  # blacklisted
    'pycompss/tests/api/test_mpi.py',
    'pycompss/tests/api/test_multinode.py',
    'pycompss/tests/api/test_ompss.py',
    'pycompss/tests/api/test_opencl.py',
    'pycompss/tests/api/test_information.py',
    'pycompss/tests/functions/test_data.py',
    'pycompss/tests/functions/test_elapsed_time.py',
    'pycompss/tests/functions/test_reduce.py',
    'pycompss/tests/runtime/test_object_tracker.py',
    'pycompss/tests/worker/test_gat.py',
    'pycompss/tests/worker/test_piper.py',
    'pycompss/tests/worker/test_mpi_piper.py',
    # ############ @parallel related ############ #
    'pycompss/api/parallel.py',
    'pycompss/util/translators/code_loader/code_loader.py',
    'pycompss/util/translators/code_replacer/code_replacer.py',
    'pycompss/util/translators/py2pycompss/translator_py2pycompss.py',
    'pycompss/util/translators/py2scop/translator_py2scop.py',
    'pycompss/util/translators/scop2pscop2py/translator_scop2pscop2py.py',
    'pycompss/util/translators/scop_types/scop_class.py',
    'pycompss/util/translators/scop_types/scop/statement_class.py',
    'pycompss/util/translators/scop_types/scop/extensions/coordinates_class.py',
    'pycompss/util/translators/scop_types/scop/extensions/scatnames_class.py',
    'pycompss/util/translators/scop_types/scop/extensions/arrays_class.py',
    'pycompss/util/translators/scop_types/scop/global_class.py',
    'pycompss/util/translators/scop_types/scop/globl/parameters/parameter_class.py',
    'pycompss/util/translators/scop_types/scop/globl/parameters_class.py',
    'pycompss/util/translators/scop_types/scop/globl/context_class.py',
    'pycompss/util/translators/scop_types/scop/statement/statement_extension_class.py',
    'pycompss/util/translators/scop_types/scop/statement/relation_class.py',
    'pycompss/util/translators/scop_types/scop/extensions_class.py'
]
FILES_BLACK_LIST = [
    'pycompss/tests/api/test_local.py',  # fails due to replace util.
]
INTEGRATION_WHITE_LIST = [
    # Include here all tests that require the runtime installed
    'pycompss/tests/integration/test_launch_application.py',
    'pycompss/tests/integration/test_launch_synthetic_application.py',
    'pycompss/tests/integration/test_launch_functions.py',
]


class ExtensionPlugin(Plugin):

    name = "ExtensionPlugin"

    def __init__(self, integration=False):
        super(ExtensionPlugin, self).__init__()
        if integration:
            self.files_white_list = INTEGRATION_WHITE_LIST
        else:
            self.files_white_list = FILES_WHITE_LIST
        self.directories_white_list = DIRECTORIES_WHITE_LIST
        self.files_black_list = FILES_BLACK_LIST

    def options(self, parser, env):
        Plugin.options(self, parser, env)

    def configure(self, options, config):
        Plugin.configure(self, options, config)
        self.enabled = True

    def wantFile(self, file):  # noqa
        print("FILE: " + str(file))
        # Check that is a python file
        if file.endswith('.py'):
            # Check that is white-listed
            for white_file in self.files_white_list:
                if white_file not in self.files_black_list and file.endswith(white_file):
                    print("Testing File: " + str(file))
                    return True
        # Does not match any pattern
        return False

    def wantDirectory(self, directory):  # noqa
        # Check that the directory is white-listed
        for white_dir in self.directories_white_list:
            if directory.endswith(white_dir):
                return True
        # Does not match any pattern
        return False

    def wantModule(self, file):  # noqa
        print("MODULE: " + str(file))
        return True


if __name__ == '__main__':
    do_integration_tests = sys.argv.pop() == "True"
    includeDirs = ["-w", "."]
    nose.main(addplugins=[ExtensionPlugin(integration=do_integration_tests)],
              argv=sys.argv.extend(includeDirs))
