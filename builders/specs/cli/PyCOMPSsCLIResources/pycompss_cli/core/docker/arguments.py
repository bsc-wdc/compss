import sys
import argparse
import os

FORMATTER_CLASS = argparse.ArgumentDefaultsHelpFormatter


def docker_init_parser():
    """ Parses the sys.argv.

    :returns: All arguments as namespace.
    """
    parser = argparse.ArgumentParser(formatter_class=FORMATTER_CLASS)

    # Parent parser - includes all arguments which are common to all actions
    parent_parser = argparse.ArgumentParser(add_help=False,
                                            formatter_class=FORMATTER_CLASS)
    # Action sub-parser
    subparsers = parser.add_subparsers(dest="action")
    # INIT
    parser_init = subparsers.add_parser("init",
                                        aliases=["i"],
                                        help="Initialize COMPSs with docker.",
                                        parents=[parent_parser],
                                        formatter_class=FORMATTER_CLASS)
    parser_init.add_argument("-w", "--working_dir",
                             default='current directory',
                             type=str,
                             help="Working directory")
    parser_init.add_argument("-i", "--image",
                             default="",
                             type=str,
                             help="Docker image")
    parser_init.add_argument("-nr", "--no_restart",
                             dest="restart",
                             action="store_false",
                             help='Do not restart if already deployed.')
    
    return parser_init