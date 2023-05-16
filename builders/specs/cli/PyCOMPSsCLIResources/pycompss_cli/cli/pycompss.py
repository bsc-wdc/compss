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

from pycompss_cli.core.arguments import parse_sys_argv
from pycompss_cli.core.actions_dispatcher import ActionsDispatcher
import sys

# Globals
LINE_LENGTH = 79
LINE = "-" * LINE_LENGTH


def main():
    """
    MAIN ENTRY POINT
    """        
    
    arguments = parse_sys_argv()

    if arguments.debug:
        print(LINE)
        
        if 'enqueue_args' in arguments:
            del arguments.enqueue_args
        if 'runcompss_args' in arguments:
            del arguments.runcompss_args
        print("Calling pycompss-cli for action: " + arguments.action)
        print()
        print("Arguments: " + str(arguments))

    ActionsDispatcher().run_action(arguments)


if __name__ == "__main__":
    main()
