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

# Imports
import nose
import sys
from nose.plugins.base import Plugin


class ExtensionPlugin(Plugin):
        name = "ExtensionPlugin"
        directories_white_list = [
                'src',
                'pycompss',
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
        files_white_list = [
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

        def options(self, parser, env):
                Plugin.options(self, parser, env)

        def configure(self, options, config):
                Plugin.configure(self, options, config)
                self.enabled = True

        @classmethod
        def wantFile(cls, file):
                print("FILE: " + str(file))
                # Check that is a python file
                if file.endswith('.py'):
                        # Check that is white-listed
                        for white_file in ExtensionPlugin.files_white_list:
                                if file.endswith(white_file):
                                        print("Testing File: " + str(file))
                                        return True

                # Does not match any pattern
                return False

        @classmethod
        def wantDirectory(cls, directory):
                # Check that the directory is white-listed
                for white_dir in ExtensionPlugin.directories_white_list:
                        if directory.endswith(white_dir):
                                return True

                # Does not match any pattern
                return False

        @classmethod
        def wantModule(cls, file):
                print("MODULE: " + str(file))
                return True


if __name__ == '__main__':
        includeDirs = ["-w", "."]
        nose.main(addplugins=[ExtensionPlugin()],
                  argv=sys.argv.extend(includeDirs))
