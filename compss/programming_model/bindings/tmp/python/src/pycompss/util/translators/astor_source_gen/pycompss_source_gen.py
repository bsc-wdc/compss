#!/usr/bin/python

# This class is based on test class astor/tests/test_subclass_code_gen.py
# available on PR 75 on https://github.com/berkerpeksag/astor/pull/75
#
# Original file is licensed under:
#   Part of the astor library for Python AST manipulation
#   License: 3-clause BSD
#   Copyright (c) 2014 Berker Peksag
#   Copyright (c) 2015, 2017 Patrick Maupin
#   Shows an example of subclassing of SourceGenerator to insert comment nodes.
#
# WARN: This code is unused until PR is released (except PrettyPrinters)
#

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import unittest

from astor.code_gen import SourceGenerator


#
# Block Comment class
#

class BlockComment(object):
    """
    Represents a block comment.
    """

    def __init__(self, text):
        """
        Initializes the comment node with inside text

        :param text: Comment text
        """

        self.text = text


#
# Source Generator custom class
#

class PyCOMPSsSourceGen(SourceGenerator):
    """
    Source Generator with comments
    """

    def visit_BlockComment(self, node):
        """
        Print a block comment.

        :param node:
        :return:
        """

        self.statement(node, '# ', node.text)

    #
    # Pretty writers
    #

    @staticmethod
    def long_line_ps(source):
        # A log pretty source printer for ASTOR because OSL parser does
        # not accept breaking statements into different lines
        from astor.source_repr import split_lines
        return ''.join(split_lines(source, maxline=120))

    @staticmethod
    def long_long_line_ps(source):
        # A log pretty source printer for ASTOR because OSL parser does
        # not accept breaking statements into different lines
        from astor.source_repr import split_lines
        return ''.join(split_lines(source, maxline=200))


#
# UNIT TESTS
#

class TestPyCOMPSsSourceGen(unittest.TestCase):

    def disabled_test_comment_node(self):
        # Generate source code
        import textwrap
        orig_source = textwrap.dedent("""
            if 1:

                def sam(a, b, c):
                    # This is a block comment
                    x, y, z = a, b, c

                def mary(a, b, c):
                    x, y, z = a, b, c
        """).strip()

        # Strip the block comment
        uncommented_src = orig_source.replace('        # This is a block comment\n', '')

        # Uncomment the bill function and generate the AST
        import ast
        node = ast.parse(uncommented_src)

        # Add a comment
        node.body[0].body[0].body.insert(0, BlockComment("This is a block comment"))

        # Assert it round-trips OK
        # TODO: Uncomment test generation and add generated_source to checker
        # generated_source = textwrap.dedent(PyCOMPSsSourceGen.to_source(node)).strip()
        self.assertEqual(orig_source, orig_source)


if __name__ == '__main__':
    unittest.main()
