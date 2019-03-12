#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import os

from constants import DEFAULT_SKIP
from constants import DEFAULT_NUM_RETRIES
from constants import DEFAULT_FAIL_FAST
from constants import DEFAULT_FAMILIES
from constants import DEFAULT_CFG_FILE
from constants import DEFAULT_TESTS
from constants import TESTS_DIR


############################################
# ERROR AND EXIT CLASSES
############################################

class ArgumentError(Exception):
    """
    Class representing an error when parsing a command line argument

    :attribute exit_value: Internal exit value when parsing the command line arguments
        + type: int
    """

    def __init__(self, exit_value):
        """
        Initializes the ArgumentError class with the given exit_value

        :param exit_value: Exit value when parsing the command line arguments
        """
        self.exit_value = exit_value

    def __str__(self):
        return "[ERROR] ArgumentError with exitValue = " + repr(self.exit_value)


class ArgumentExit(Exception):
    """
    Class representing an success exit while processing the command line arguments
    """

    def __init__(self):
        """
        Initializes the ArgumentExit class 
        """
        pass

    def __str__(self):
        return "[DONE] ArgumentExit"


############################################
# PUBLIC METHODS
############################################

def get_args():
    """
    Constructs an object representing the command line arguments

    :return: Object representing the command line arguments
        + type: argparse.Namespace
    :raise ArgumentError: Error parsing command line arguments
    :raise ArgumentExit: Exit parsing command line arguments
    :exit 0: This method exits when test numbering is provided
    """
    print()
    print("[INFO] Parsing arguments...")

    import argparse

    # Create the argument parser
    if __debug__:
        print("[DEBUG] Creating argument parser...")

    parser = argparse.ArgumentParser(description="Launch COMPSs tests",
                                     add_help=True)

    # Add version
    parser.add_argument('--version', action='version', version='2.4.rc1902')

    # Add skip options
    parser.add_argument("-s", "--skip",
                        action="store_const",
                        dest="skip",
                        const=True,
                        default=DEFAULT_SKIP,
                        help="Enables test skipping")
    parser.add_argument("-S", "--no-skip",
                        action="store_const",
                        dest="skip",
                        const=False,
                        default=DEFAULT_SKIP,
                        help="Disables test skipping")

    # Add retry options
    parser.add_argument("-r", "--retry",
                        action="store",
                        dest="retry",
                        type=int,
                        choices=range(1, 6),
                        default=DEFAULT_NUM_RETRIES,
                        help="Sets the test retries (must be bigger or equal to 1)")
    parser.add_argument("-R", "--no-retry",
                        action="store_const",
                        dest="retry",
                        const=1,
                        default=DEFAULT_NUM_RETRIES,
                        help="Disables the test retries")

    # Add fail fast option
    parser.add_argument("-FF", "--fail-fast",
                        action="store_const",
                        dest="fail_fast",
                        const=True,
                        default=DEFAULT_FAIL_FAST,
                        help="Enables fail-fast option on test execution")

    # Add numbering option
    parser.add_argument("-n", "--numbering",
                        action="store_const",
                        dest="numbering",
                        const=True,
                        default=False,
                        help="Displays the test numbering and exits")

    # Add test lang options
    parser.add_argument("-f", "--family",
                        action="append",
                        dest="families",
                        choices=DEFAULT_FAMILIES,
                        default=[],
                        help="Executes only the tests of the specified family")
    parser.add_argument("-fj", "--family-java",
                        action="append_const",
                        dest="families",
                        const="java",
                        default=[],
                        help="Executes only the Java tests")
    parser.add_argument("-fp", "--family-python",
                        action="append_const",
                        dest="families",
                        const="python",
                        default=[],
                        help="Executes only the Python tests")
    parser.add_argument("-fc", "--family-c",
                        action="append_const",
                        dest="families",
                        const="c",
                        default=[],
                        help="Executes only the C/C++ tests")

    # Add cfg file option
    parser.add_argument("-cfg", "--cfg-file",
                        action="store",
                        dest="cfg_file",
                        default=DEFAULT_CFG_FILE,
                        help="Path to a valid CFG file")

    # Add specific test option
    parser.add_argument("-t", "--specific-test",
                        action="append",
                        dest="tests",
                        default=DEFAULT_TESTS,
                        help="Executes only the given specific test number.\n"
                             "If <int> is provided, executes test with global number <int>.\n"
                             "If <str>:<int> is provided, executes test with local number <int> in family <str>.\n"
                             "If <str1>:<str2> is provided, executes test with name <str2> in family <str1>.")

    if __debug__:
        print("[DEBUG] Argument parser created")

    # Parse arguments
    # WARN: Stops the execution if an invalid argument is provided
    if __debug__:
        print("[DEBUG] Executing parser on command arguments...")
    try:
        args = parser.parse_args()
    except SystemExit as se:
        if se.code is None or se.code == 0:
            raise ArgumentExit()
        raise ArgumentError(se)

    # Check arguments
    args = _check_args(args)

    # If numbering cmd arg is provided, display numbering and exit
    if args.numbering:
        _display_numbering(args.test_numbers)

    # Print arguments
    print("[INFO] Arguments parsed:")
    print("[INFO]   - skip: " + str(args.skip))
    print("[INFO]   - retry: " + str(args.retry))
    print("[INFO]   - fail_fast: " + str(args.fail_fast))
    print("[INFO]   - numbering: " + str(args.numbering))
    print("[INFO]   - families: " + str(args.families))
    print("[INFO]   - cfg_file: " + str(args.cfg_file))
    print("[INFO]   - tests: " + str(args.tests))
    # print("[INFO]   - test_numbers: " + str(args.test_numbers))

    return args


############################################
# INTERNAL METHODS
############################################

def _check_args(cmd_args):
    """
    Validates and completes the parsed cmd_args

    :return: Validated object representing the command line arguments
        + type: argparse.Namespace
    """

    # If no family provided, load all
    if cmd_args.families is None or not cmd_args.families:
        cmd_args.families = DEFAULT_FAMILIES

    # Add test numbering to cmd_args
    cmd_args.test_numbers = _get_test_numbers()

    return cmd_args


def _get_test_numbers():
    """
    Builds the numbering of each available test

    :return: Object containing the numbering of each available test
        + dict<str, dict<int, Tuple<str, str>> : Family -> test num -> test info
    """
    if __debug__:
        print("[DEBUG] Get test numbering")

    # Number all tests
    test_numbers = {"global": {}}
    num_global = 1
    for family_dir in sorted(os.listdir(TESTS_DIR)):
        family_path = os.path.join(TESTS_DIR, family_dir)
        if os.path.isdir(family_path):
            test_numbers[family_dir] = {}
            num_family = 1
            for test_dir in sorted(os.listdir(family_path)):
                test_path = os.path.join(family_path, test_dir)
                if test_dir != ".target" and test_dir != ".settings" and test_dir != "target" and os.path.isdir(test_path):
                    test_numbers["global"][num_global] = (test_dir, test_path, family_dir, num_family)
                    test_numbers[family_dir][num_family] = (test_dir, test_path, num_global)
                    num_global = num_global + 1
                    num_family = num_family + 1

    # Return numbering
    if __debug__:
        print("[DEBUG] Test numbering retrieved")

    return test_numbers


def _display_numbering(test_numbers):
    """
    Displays the test numbering and exits

    :return:
    :exit 0: Exits (without error) after displaying the test numbering
    """
    print()
    print("[INFO] Displaying numbering and exit")
    print()

    # Construct numbering information
    headers = ["Global Number", "Family", "Family Number", "Test Name"]
    info = []
    for test_num, test_info in test_numbers["global"].items():
        test_dir, test_path, family_dir, num_family = test_info
        row_info = ["{:03d}".format(test_num), str(family_dir), "{:03d}".format(num_family), str(test_dir)]
        info.append(row_info)

    # Display information
    from tabulate import tabulate
    print(tabulate(info, headers=headers))

    # Exit
    print()
    print("----------------------------------------")
    print("[INFO] Tests numbering displayed. Exit")
    print("----------------------------------------")
    exit(0)
