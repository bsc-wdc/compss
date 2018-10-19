#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import unittest
import logging

#
# Logger definition
#

logger = logging.getLogger(__name__)


#
# Translator class
#

class Scop2PScop2Py(object):

    @staticmethod
    def translate(source, output, pluto_extra_flags=None):
        """
        Inputs an OpenScop representation to PLUTO that generates
        its parallel version in Python

        Arguments:
                - source : OpenScop source file path
                - output : Python output file path
        Return:
        Raise:
                - Scop2PScop2PyException
        """

        if __debug__:
            logger.debug("[scop2pscop2py] Translating " + str(source) + " into " + str(output))

        # PLUTO binary location
        import os
        PLUTO_DIR = os.getenv("PLUTO_HOME", "/opt/COMPSs/Dependencies/pluto")
        PLUTO_DIR = PLUTO_DIR + "/bin/"
        PLC = PLUTO_DIR + "polycc"

        # Pluto binary options
        # TODO: Tune PLUTO options
        mandatory_opts = ["--readscop", "-o " + output]
        basic_opts = ["--parallel"]  # ["--tile", "--parallel"]
        adv_opts = []  # ["--rar", "--lastwriter"]
        mode_opts = []  # ["--silent"] # ["--debug"] # ["--moredebug"]
        usr_opts = pluto_extra_flags if pluto_extra_flags is not None else []

        # Construct binary call
        cmd = [PLC, source] + mandatory_opts + basic_opts + adv_opts + mode_opts + usr_opts
        if __debug__:
            logger.debug("[scop2pscop2py] Command: " + str(cmd))

        # Call binary
        try:
            from subprocess import Popen, PIPE
            subprocess_env = os.environ.copy()
            if "LD_PRELOAD" in subprocess_env.keys():
                del subprocess_env["LD_PRELOAD"]
            process = Popen(cmd, env=subprocess_env, stdin=None, stdout=PIPE, stderr=PIPE, shell=False)

            # Wait for completion and capture output, error and exit value
            stdout, stderr = process.communicate()
            exit_value = process.returncode
        except Exception as e:
            raise Scop2PScop2PyException("[ERROR] PLUTO binary execution error", e)

        # Check process values
        if exit_value != 0:
            logger.error("[ERROR] Pluto binary returned non-zero exit value: " + str(exit_value))
            logger.error("[scop2pscop2py] Binary output:")
            logger.error(stdout)
            logger.error("[scop2pscop2py] Binary error:")
            logger.error(stderr)
            raise Scop2PScop2PyException("[ERROR] Pluto binary exit value = " + str(exit_value), stderr)

        # Finish
        if __debug__:
            logger.debug("[scop2pscop2py] Pluto binary successful")
            logger.debug("[scop2pscop2py] Binary output:")
            logger.debug(stdout)


#
# Exception Class
#

class Scop2PScop2PyException(Exception):

    def __init__(self, msg=None, nested_exception=None):
        self.msg = msg
        self.nested_exception = nested_exception

    def __str__(self):
        s = "Exception on Scop2PScop2Py.translate method.\n"
        if self.msg is not None:
            s = s + "Message: " + str(self.msg) + "\n"
        if self.nested_exception is not None:
            s = s + "Nested Exception: " + str(self.nested_exception) + "\n"
        return s


#
# UNIT TEST CASES
#

class TestScop2PScop2Py(unittest.TestCase):

    def test_matmul(self):
        import os
        dir_path = os.path.dirname(os.path.realpath(__file__))

        # Source OpenScop file
        source_file = dir_path + "/tests/test1_matmul.src.scop"

        # Output Python file
        output_file = dir_path + "/tests/test1_matmul.output.python"

        # Expected output file
        expected_file = dir_path + "/tests/test1_matmul.expected.python"

        try:
            # Generate scop2pscop2py
            Scop2PScop2Py.translate(source_file, output_file)

            # Check file content
            with open(expected_file, 'r') as f:
                expected_content = f.read()
            with open(output_file, 'r') as f:
                output_content = f.read()
            self.assertEqual(output_content, expected_content)
        except Exception:
            raise
        finally:
            # Erase output file
            TestScop2PScop2Py._clean(output_file)

    def test_seidel(self):
        import os
        dir_path = os.path.dirname(os.path.realpath(__file__))

        # Source OpenScop file
        source_file = dir_path + "/tests/test2_seidel.src.scop"

        # Output Python file
        output_file = dir_path + "/tests/test2_seidel.output.python"

        # Expected output file
        expected_file = dir_path + "/tests/test2_seidel.expected.python"

        try:
            # Generate scop2pscop2py
            Scop2PScop2Py.translate(source_file, output_file)

            # Check file content
            with open(expected_file, 'r') as f:
                expected_content = f.read()
            with open(output_file, 'r') as f:
                output_content = f.read()
            self.assertEqual(output_content, expected_content)
        except Exception:
            raise
        finally:
            # Erase output file
            TestScop2PScop2Py._clean(output_file)

    @staticmethod
    def _clean(f):
        import os
        if os.path.isfile(f):
            os.remove(f)


#
# MAIN FOR UNIT TEST
#

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s | %(levelname)s | %(name)s - %(message)s')
    unittest.main()
