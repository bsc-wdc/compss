#!/usr/bin/python
#
#  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
import os
import subprocess

from pycompss_cli.core import utils

from pycompss_cli.core.unicore import UNICORE_URL_ENVAR
from pycompss_cli.core.unicore import UNICORE_USER_ENVAR
from pycompss_cli.core.unicore import UNICORE_PASSWORD_ENVAR
from pycompss_cli.core.unicore import UNICORE_TOKEN_ENVAR

FORMATTER_CLASS = argparse.RawDescriptionHelpFormatter

def unicore_init_parser():
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
                                        help="Initialize COMPSs within a given unicore node.",
                                        parents=[parent_parser],
                                        formatter_class=FORMATTER_CLASS)

    parser_init.add_argument("-url", "--base_url",
                             type=str,
                             help=f"Unicore base url. Also can be used envar {UNICORE_URL_ENVAR}")

    parser_init.add_argument("-u", "--user",
                             type=str,
                             help=f"Unicore username for login. Also can be used envar {UNICORE_USER_ENVAR}")

    parser_init.add_argument("-p", "--password",
                             type=str,
                             help=f"Unicore password for login. Also can be used envar {UNICORE_PASSWORD_ENVAR}")

    parser_init.add_argument("-t", "--token",
                             type=str,
                             help=f"Unicore JWT token signed by the server. Also can be used envar {UNICORE_TOKEN_ENVAR}")

    parser_init.add_argument("-m", "--modules",
                             nargs='*',
                             help="Modules file to load in remote environment")

    return parser_init