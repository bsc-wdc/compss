#!/usr/bin/env python3
#
#  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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

"""
PyCOMPSs Binding - Runnable as module.

Provides the functionality to be run as a module:
e.g. python -m pycompss run -dgt myapp.py
"""

import argparse
import sys
from subprocess import Popen

from pycompss.runtime.commons import CONSTANTS
from pycompss.util.typing_helper import typing

RUN_TAG = "run"
ENQUEUE_TAG = "enqueue"
RUN_EXECUTABLE = "runcompss"
ENQUEUE_EXECUTABLE = "enqueue_compss"
TAGS = [RUN_TAG, ENQUEUE_TAG]


class Object:  # pylint: disable=too-few-public-methods
    """Dummy class to mimic argparse return object."""

    action = "None"
    params = []


def setup_parser() -> argparse.ArgumentParser:
    """Argument parser.

    - Argument defining run for runcompss or enqueue for enqueue_compss.
    - The rest of the arguments as a list.

    :return: the parser
    """
    parser = argparse.ArgumentParser(prog="python -m pycompss")
    parser.add_argument(
        "action",
        choices=TAGS,
        nargs="?",
        help='Execution mode: "run" for launching an'
        + ' execution and "enqueue" for submitting a'
        + " job to the queuing system."
        + ' Default value: "run"',
    )
    parser.add_argument(
        "params",
        nargs=argparse.REMAINDER,
        help="COMPSs and application arguments"
        + ' (check "runcompss" or "enqueue_compss"'
        + " commands help).",
    )
    return parser


def run(cmd: typing.List[str]) -> None:
    """Execute a command line in a subprocess.

    :param cmd: Command to execute (list of <String>)
    :return: None
    """
    with Popen(cmd, stdout=sys.stdout, stderr=sys.stderr) as command_process:
        command_process.communicate()


def main() -> None:
    """Run PyCOMPSs as module.

    SAMPLE: python -m pycompss run <COMPSs_FLAGS> myapp.py

    :return: None
    """
    _help = ["-h", "--help"]
    parser = None  # type: typing.Optional[argparse.ArgumentParser]

    # Check params
    if (
        len(sys.argv) > 1
        and sys.argv[1] not in TAGS
        and sys.argv[1] not in _help
    ):
        # No action specified. Assume run.
        args = Object()
        args.action = RUN_TAG
        args.params = sys.argv[1:]
    else:
        parser = setup_parser()
        args = parser.parse_args()

    # Check if the user has specified to use a specific python interpreter
    if any("--python_interpreter=" in param for param in args.params):
        # The user specified explicitly a python interpreter to use
        # Do not include the python_interpreter variable in the cmd call
        python_interpreter = []
    else:
        # Use the same as current
        python_interpreter = [
            f"--python_interpreter={CONSTANTS.python_interpreter}"
        ]

    # Take an action
    if args.action == RUN_TAG:
        cmd = [RUN_EXECUTABLE] + python_interpreter + args.params
        run(cmd)
    elif args.action == ENQUEUE_TAG:
        cmd = [ENQUEUE_EXECUTABLE] + python_interpreter + args.params
        run(cmd)
    else:
        # Reachable only when python -m pycompss (and nothing else)
        parser.print_usage()


if __name__ == "__main__":
    main()
