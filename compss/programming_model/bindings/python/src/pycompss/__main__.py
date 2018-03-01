#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Binding - Runnable as module
=====================================
Provides the current functionality to be run as a module.
e.g. python -m pycompss run -dgt myapp.py
"""

import sys
import argparse
from subprocess import Popen

RUN_TAG = 'run'
ENQUEUE_TAG = 'enqueue'
RUN_EXECUTABLE = 'runcompss'
ENQUEUE_EXECUTABLE = 'enqueue_compss'
TAGS = [RUN_TAG, ENQUEUE_TAG]


def setup_parser():
    """
    Argument parser.
        * Argument defining run for runcompss or enqueue for enqueue_compss.
        * The rest of the arguments as a list
    :return: the parser
    """
    parser = argparse.ArgumentParser(prog='python -m pycompss')
    parser.add_argument('action', choices=TAGS, nargs='?', help="Execution mode: \'run\' for launching an execution and \'enqueue\' for submitting a job to the queuing system. Default value: \'run\'")
    parser.add_argument('params', nargs=argparse.REMAINDER, help="COMPSs and application arguments (check \'runcompss\' or \'enqueue_compss\' commands help).")
    return parser


def run(cmd):
    p = Popen(cmd, stdout=sys.stdout, stderr=sys.stderr)
    p.communicate()


def main():
    """
    Main method.
    """
    help = ['-h', '--help']
    if len(sys.argv) > 1 and sys.argv[1] not in TAGS and sys.argv[1] not in help:
        # No action specified. Assume run.
        class Object(object):
            # Dummy class to mimic argparse return object
            pass
        args = Object()
        args.action = RUN_TAG
        args.params = sys.argv[1:]
    else:
        parser = setup_parser()
        args = parser.parse_args()

    if args.action == RUN_TAG:
        cmd = [RUN_EXECUTABLE] + args.params
        run(cmd)
    elif args.action == ENQUEUE_TAG:
        cmd = [ENQUEUE_EXECUTABLE] + args.params
        run(cmd)
    else:
        # Reachable only when python -m pycompss (and nothing else)
        parser.print_usage()


if __name__ == '__main__':
    main()
