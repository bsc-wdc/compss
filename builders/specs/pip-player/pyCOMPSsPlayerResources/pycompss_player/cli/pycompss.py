#!/usr/bin/env python3

import os
from pycompss_player.core.arguments import parse_sys_argv
from pycompss_player.core.actions import init
from pycompss_player.core.actions import update
from pycompss_player.core.actions import kill
from pycompss_player.core.actions import exec
from pycompss_player.core.actions import run
from pycompss_player.core.actions import monitor
from pycompss_player.core.actions import jupyter
from pycompss_player.core.actions import gengraph
from pycompss_player.core.actions import components

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
        print("Calling pycompss-player for action: " + arguments.action)
        print("Arguments: " + str(arguments))

    if arguments.action == "init":
        init(arguments, DEBUG)
    elif arguments.action == "update":
        update(arguments, DEBUG)
    elif arguments.action == "kill":
        kill(arguments, DEBUG)
    elif arguments.action == "exec":
        exec(arguments, DEBUG)
    elif arguments.action == "run":
        run(arguments, DEBUG)
    elif arguments.action == "monitor":
        monitor(arguments, DEBUG)
    elif arguments.action == "jupyter":
        jupyter(arguments, DEBUG)
    elif arguments.action == "gengraph":
        gengraph(arguments, DEBUG)
    elif arguments.action == "components":
        components(arguments, DEBUG)
    else:
        raise Exception("Not implemented action " + arguments.action)

    if DEBUG:
        print(LINE)


if __name__ == "__main__":
    main()
