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
import argparse

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
    parser_init.add_argument("-log", "--log_dir",
                             default='current directory',
                             type=str,
                             help="Logs directory (.COMPSs)")
    parser_init.add_argument("-i", "--image",
                             default="",
                             type=str,
                             help="Docker image")
    parser_init.add_argument("-nr", "--no_restart",
                             dest="restart",
                             action="store_false",
                             help='Do not restart if already deployed.')
    parser_init.add_argument("-p", "--privileged",
                             dest="privileged",
                             action="store_true",
                             help='Run container with privileged permissions.')
    parser_init.add_argument("-ui", "--update_image",
                             dest="update_image",
                             action="store_true",
                             help='Update image before deployment.')
    
    return parser_init
