import sys
import argparse


FORMATTER_CLASS = argparse.ArgumentDefaultsHelpFormatter


def parse_sys_argv():
    """ Parses the sys.argv.

    :returns: All arguments as namespace.
    """
    parser = argparse.ArgumentParser(formatter_class=FORMATTER_CLASS)
    parser.add_argument("-d", "--debug",
                        help="Enable debug mode. Overrides log_level",
                        action="store_true")

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
                             default="",
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
    # UPDATE
    parser_update = subparsers.add_parser("update",
                                          aliases=["u"],
                                          help="Update docker image.",
                                          parents=[parent_parser],
                                          formatter_class=FORMATTER_CLASS)
    # KILL
    parser_kill = subparsers.add_parser("kill",
                                        aliases=["k"],
                                        help="Kill all COMPSs\' docker instances.",  # noqa: E501
                                        parents=[parent_parser],
                                        formatter_class=FORMATTER_CLASS)
    parser_kill.add_argument("-nc", "--no_clean",
                             dest="clean",
                             action="store_false",
                             help='Do not clean the generated files (xmls).')
    # EXEC
    parser_exec = subparsers.add_parser("exec",
                                        aliases=["e"],
                                        help="Execute the given command within the COMPSs\' docker instance.",  # noqa: E501
                                        parents=[parent_parser],
                                        formatter_class=FORMATTER_CLASS)
    parser_exec.add_argument("command",
                             type=str,
                             help="Command to execute")
    parser_exec.add_argument("argument",
                             nargs="*",
                             type=str,
                             help="Command\'s arguments")
    # RUN
    parser_run = subparsers.add_parser("run",
                                       aliases=["r"],
                                       help="Run the application (with runcompss) within the COMPSs\' docker instance.",  # noqa: E501
                                       parents=[parent_parser],
                                       formatter_class=FORMATTER_CLASS)
    parser_run.add_argument("application",
                            type=str,
                            help="Application to execute")
    parser_run.add_argument("argument",
                            nargs="*",
                            type=str,
                            help="Application\'s arguments")
    # MONITOR
    parser_monitor = subparsers.add_parser("monitor",
                                           aliases=["m"],
                                           help="Start the monitor within the COMPSs\' docker instance.",  # noqa: E501
                                           parents=[parent_parser],
                                           formatter_class=FORMATTER_CLASS)
    parser_monitor.add_argument("option",
                                help="Start or stop de monitoring service.",
                                choices=["start", "stop"],
                                default="start",
                                type=str)
    # JUPYTER
    parser_jupyter = subparsers.add_parser("jupyter",
                                           aliases=["j"],
                                           help="Starts Jupyter within the COMPSs\' docker instance.",  # noqa: E501
                                           parents=[parent_parser],
                                           formatter_class=FORMATTER_CLASS)
    parser_jupyter.add_argument("argument",
                                nargs="*",
                                type=str,
                                help="Jupyter\'s arguments")
    # GENGRAPH
    parser_gengraph = subparsers.add_parser("gengraph",
                                            aliases=["g"],
                                            help="Converts the given graph into pdf.",  # noqa: E501
                                            parents=[parent_parser],
                                            formatter_class=FORMATTER_CLASS)
    parser_gengraph.add_argument("dot_file",
                                 type=str,
                                 help="Dot file to convert to pdf")
    # COMPONENTS
    parser_components = subparsers.add_parser("components",
                                              aliases=["c"],
                                              help="Manage infrastructure components.",  # noqa: E501
                                              parents=[parent_parser],
                                              formatter_class=FORMATTER_CLASS)
    subparsers_components = parser_components.add_subparsers(dest="components")
    parser_components_list = subparsers_components.add_parser("list",
                                                              aliases=["l"],
                                                              help="List COMPSs active components.",  # noqa: E501
                                                              formatter_class=FORMATTER_CLASS)        # noqa: E501
    parser_components_add = subparsers_components.add_parser("add",
                                                             aliases=["a"],
                                                             help="Adds the RESOURCE to the pool of workers of the COMPSs.",  # noqa: E501
                                                             formatter_class=FORMATTER_CLASS)                                 # noqa: E501
    subparsers_components_add = parser_components_add.add_subparsers(dest="add")                          # noqa: E501
    parser_components_add_worker = subparsers_components_add.add_parser("worker",                         # noqa: E501
                                                                        aliases=["w"],                    # noqa: E501
                                                                        help="Add a worker.",             # noqa: E501
                                                                        formatter_class=FORMATTER_CLASS)  # noqa: E501
    parser_components_add_worker.add_argument("worker",
                                              type=str,
                                              default="1",
                                              help="Number of workers to add (can be integer or <IP>:<CORES> to add remote workers).")  # noqa: E501
    parser_components_remove = subparsers_components.add_parser("remove",
                                                                aliases=["r"],
                                                                help="Removes the RESOURCE to the pool of workers of the COMPSs.",  # noqa: E501
                                                                formatter_class=FORMATTER_CLASS)                                    # noqa: E501
    subparsers_components_remove = parser_components_remove.add_subparsers(dest="remove")                                           # noqa: E501
    parser_components_remove_worker = subparsers_components_remove.add_parser("worker",                                             # noqa: E501
                                                                              aliases=["w"],                                        # noqa: E501
                                                                              help="Remove a worker.",                              # noqa: E501
                                                                              formatter_class=FORMATTER_CLASS)                      # noqa: E501
    parser_components_remove_worker.add_argument("worker",
                                                 type=str,
                                                 default="1",
                                                 help="Number of workers to remove (can be integer or <IP>:<CORES> to add remote workers).")  # noqa: E501

    # Check if the user does not include any argument
    if len(sys.argv) < 2:
        #  Show the usage
        print(parser.print_help())
        sys.exit(1)

    arguments = parser.parse_args()

    return arguments
