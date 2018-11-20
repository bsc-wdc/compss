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

# For better print formatting
from __future__ import print_function

# Imports
import unittest


class Global(object):
    """
    Represents a Global clause within a SCOP

    Attributes:
            - language : Language
            - context : Global context
            - parameters : Global list of parameters
    """

    def __init__(self, language=None, context=None, parameters=None):
        self.language = language
        self.context = context
        self.parameters = parameters

    def get_language(self):
        return self.language

    def get_context(self):
        return self.context

    def get_parameters(self):
        return self.parameters

    @staticmethod
    def read_os(content, index):
        # Skip header and any annotation
        while content[index].startswith('#') or content[index] == '\n':
            index = index + 1

        # Process mandatory field: language
        language = content[index].strip()
        index = index + 1

        # Process context
        from pycompss.util.translators.scop_types.scop.globl.context_class import Context
        context, index = Context.read_os(content, index)

        # Process parameters
        from pycompss.util.translators.scop_types.scop.globl.parameters_class import Parameters
        parameters, index = Parameters.read_os(content, index)

        # Build Global
        g = Global(language, context, parameters)

        # Return structure
        return g, index

    def write_os(self, f):
        # Write header
        print("# =============================================== Global", file=f)

        # Write language
        print("# Language", file=f)
        print(str(self.language), file=f)
        print("", file=f)

        # Print context
        print("# Context", file=f)
        if self.context is not None:
            self.context.write_os(f)

        # Print parameters
        print("# Parameters are provided", file=f)
        if self.parameters is not None:
            self.parameters.write_os(f)


#
# UNIT TESTS
#

class TestGlobal(unittest.TestCase):

    def test_empty(self):
        g = Global()

        self.assertEqual(g.get_language(), None)
        self.assertEqual(g.get_context(), None)
        self.assertEqual(g.get_parameters(), None)

    def test_full(self):
        lang = "C"

        from pycompss.util.translators.scop_types.scop.globl.context_class import Context, ContextType
        context = Context(ContextType.CONTEXT, 0, 5, 0, 0, 0, 3)

        from pycompss.util.translators.scop_types.scop.globl.parameters_class import Parameters
        from pycompss.util.translators.scop_types.scop.globl.parameters.parameter_class import Parameter
        t = "strings"
        val = ["mSize", "kSize", "nSize"]
        p1 = Parameter(t, val)
        params = Parameters([p1])

        g = Global(lang, context, params)

        self.assertEqual(g.get_language(), lang)
        self.assertEqual(g.get_context(), context)
        self.assertEqual(g.get_parameters(), params)

    def test_write_os(self):
        lang = "C"

        from pycompss.util.translators.scop_types.scop.globl.context_class import Context, ContextType
        context = Context(ContextType.CONTEXT, 0, 5, 0, 0, 0, 3)

        from pycompss.util.translators.scop_types.scop.globl.parameters_class import Parameters
        from pycompss.util.translators.scop_types.scop.globl.parameters.parameter_class import Parameter
        t = "strings"
        val = ["mSize", "kSize", "nSize"]
        p1 = Parameter(t, val)
        params = Parameters([p1])

        g = Global(lang, context, params)

        import os
        dir_path = os.path.dirname(os.path.realpath(__file__))
        output_file = dir_path + "/tests/global_test.out.scop"
        expected_file = dir_path + "/tests/global_test.expected.scop"
        try:
            # Generate file
            with open(output_file, 'w') as f:
                g.write_os(f)

            # Check file content
            with open(expected_file, 'r') as f:
                expected_content = f.read()
            with open(output_file, 'r') as f:
                output_content = f.read()
            self.assertEqual(output_content, expected_content)
        except Exception:
            raise
        finally:
            # Erase file
            os.remove(output_file)

    def test_read_os(self):
        # Store all file content
        import os
        dir_path = os.path.dirname(os.path.realpath(__file__))
        global_file = dir_path + "/tests/global_test.expected.scop"
        with open(global_file, 'r') as f:
            content = f.readlines()

        # Read from file
        g, index = Global.read_os(content, 0)

        # Check index value
        self.assertEqual(index, len(content))

        # Check Global object content
        output_file = dir_path + "/tests/global_test2.out.scop"
        try:
            # Write to file
            with open(output_file, 'w') as f:
                g.write_os(f)

            # Check file content
            with open(global_file, 'r') as f:
                expected_content = f.read()
            with open(output_file, 'r') as f:
                output_content = f.read()
            self.assertEqual(output_content, expected_content)
        except Exception:
            raise
        finally:
            # Remove test file
            os.remove(output_file)


#
# MAIN
#

if __name__ == '__main__':
    unittest.main()
