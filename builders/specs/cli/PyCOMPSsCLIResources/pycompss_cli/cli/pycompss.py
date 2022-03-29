#!/usr/bin/env python3

from pycompss_cli.core import utils
from pycompss_cli.core.arguments import parse_sys_argv
from pycompss_cli.core.actions_dispatcher import ActionsDispatcher

# Globals
DEBUG = False
LINE_LENGTH = 79
LINE = "-" * LINE_LENGTH


def main():
    """
    MAIN ENTRY POINT
    """
    
    arguments = parse_sys_argv()

    if utils.is_debug():
        print(LINE)
        
        if 'enqueue_args' in arguments:
            del arguments.enqueue_args
        if 'runcompss_args' in arguments:
            del arguments.runcompss_args
        print("Calling pycompss-cli for action: " + arguments.action)
        print()
        print("Arguments: " + str(arguments))

    ActionsDispatcher().run_action(arguments, DEBUG)


if __name__ == "__main__":
    main()
