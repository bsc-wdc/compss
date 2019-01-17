#!/usr/bin/python
# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import unittest


class Scatnames(object):
    """
    Represents an scatname object inside the extensions section

    Attributes:
            - names : Array of scatnames
    """

    def __init__(self, names=None):
        self.names = names

    def get_names(self):
        return self.names

    @staticmethod
    def read_os(content, index):
        # Skip header and any annotation
        while content[index].startswith('#') or content[index] == '\n' or content[index] == '<scatnames>\n':
            index = index + 1

        # Process mandatory field: scatnames
        names = content[index].split()
        index = index + 1

        # Skip empty lines, any annotation, and footer
        while index < len(content) and (
                content[index].startswith('#') or content[index] == '\n' or content[index] == '</scatnames>\n'):
            index = index + 1

        # Build Scatnames
        scatnames = Scatnames(names)

        # Return structure
        return scatnames, index

    def write_os(self, f):
        # Print header
        print("<scatnames>", file=f)

        # Print arrays
        if self.names is not None:
            line = ""
            for val in self.names:
                line = line + str(val) + " "
            print(line, file=f)

        # Print footer
        print("</scatnames>", file=f)
        print("", file=f)


#
# UNIT TESTS
#

class TestScatnames(unittest.TestCase):

    def test_empty(self):
        s = Scatnames()

        self.assertEqual(s.get_names(), None)

    def test_full(self):
        names = ["b0", "i", "b1", "j", "b2", "k", "b3"]
        s = Scatnames(names)

        self.assertEqual(s.get_names(), names)

    def test_write_os(self):
        names = ["b0", "i", "b1", "j", "b2", "k", "b3"]
        s = Scatnames(names)

        file_name = "scatnames_test.out"
        try:
            # Generate file
            with open(file_name, 'w') as f:
                s.write_os(f)

            # Check file content
            expected = "<scatnames>\nb0 i b1 j b2 k b3 \n</scatnames>\n\n"
            with open(file_name, 'r') as f:
                content = f.read()
            self.assertEqual(content, expected)
        except Exception:
            raise
        finally:
            # Erase file
            import os
            os.remove(file_name)

    def test_read_os(self):
        # Store all file content
        import os
        dir_path = os.path.dirname(os.path.realpath(__file__))
        scatnames_file = dir_path + "/tests/scatnames_test.expected.scop"
        with open(scatnames_file, 'r') as f:
            content = f.readlines()

        # Read from file
        scatnames, index = Scatnames.read_os(content, 0)

        # Check index value
        self.assertEqual(index, len(content))

        # Check Scatnames object content
        output_file = dir_path + "/tests/scatnames_test.out.scop"
        try:
            # Write to file
            with open(output_file, 'w') as f:
                scatnames.write_os(f)

            # Check file content
            with open(scatnames_file, 'r') as f:
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
