#!/usr/bin/env python3

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
    global DEBUG
    arguments = parse_sys_argv()
    DEBUG = arguments.debug

    if DEBUG:
        print(LINE)
        print("Calling pycompss-cli for action: " + arguments.action)
        print("Arguments: " + str(arguments))

    ActionsDispatcher().run_action(arguments, DEBUG)

    if DEBUG:
        print(LINE)


if __name__ == "__main__":
    main()
