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
import os
import sys
import argparse
import subprocess

from pycompss_cli.core.docker.arguments import docker_init_parser
from pycompss_cli.core.local.arguments import local_init_parser
from pycompss_cli.core.remote.arguments import remote_init_parser
from pycompss_cli.core.remote.arguments import remote_parser_job
from pycompss_cli.core.remote.arguments import remote_parser_app
from pycompss_cli.core.unicore.arguments import unicore_init_parser
from pycompss_cli.core import utils

FORMATTER_CLASS = argparse.RawTextHelpFormatter

def parse_sys_argv():
    """ Parses the sys.argv.

    :returns: All arguments as namespace.
    """
    parser = argparse.ArgumentParser(formatter_class=FORMATTER_CLASS)
    parser.add_argument("-d", "--debug",
                        help="Enable debug mode. Overrides log_level",
                        action="store_true")

    parser.add_argument("-eid", "--env_id",
                             default="",
                             type=str,
                             help="Environment ID")

    # Parent parser - includes all arguments which are common to all actions
    parent_parser = argparse.ArgumentParser(add_help=False,
                                            formatter_class=FORMATTER_CLASS)
    # Action sub-parser
    subparsers = parser.add_subparsers(dest="action")
    # INIT
    parser_init = subparsers.add_parser("init",
                                        aliases=["i"],
                                        help="Initialize COMPSs environment (default local).",
                                        parents=[parent_parser],
                                        formatter_class=FORMATTER_CLASS)
                                        
    parser_init.set_defaults(func=lambda: print(parser_init.format_help()))
    
    parser_init.add_argument("-cfg", "--config",
                             default="",
                             type=str,
                             help="Configuration file")

    parser_init.add_argument("-n", "--name",
                             default='unique uuid',
                             type=str,
                             help="Environment name")

    init_env_subparser = parser_init.add_subparsers(title="environment", dest="env")
    # init_env_subparser.default = "local"

    init_env_subparser.add_parser("docker", add_help=False, 
                                    parents=[docker_init_parser()])

    init_env_subparser.add_parser("local", add_help=False, 
                                    parents=[local_init_parser()])

    init_env_subparser.add_parser("remote", add_help=False, 
                                    parents=[remote_init_parser()])

    init_env_subparser.add_parser("unicore", add_help=False, 
                                    parents=[unicore_init_parser()])

    # EXEC
    parser_exec = subparsers.add_parser("exec",
                                        aliases=["ex"],
                                        help="Execute the given command within the COMPSs\' environment.",  # noqa: E501
                                        parents=[parent_parser],
                                        formatter_class=FORMATTER_CLASS)
    parser_exec.set_defaults(action='exec')

    
    parser_exec.add_argument('exec_cmd', 
                            nargs=argparse.REMAINDER,   
                            help="Exec program arguments")
    # RUN
    parser_run = subparsers.add_parser("run",
                                       aliases=["r"],
                                       help="Run the application (with runcompss) within the COMPSs\' environment.",  # noqa: E501
                                       parents=[parent_parser],
                                       formatter_class=FORMATTER_CLASS)
    parser_run.set_defaults(action='run')

    if utils.check_exit_code('runcompss -h') == 0:
        parser_run.epilog = subprocess.check_output('runcompss -h', shell=True).decode()
    else:
        assets_folder = os.path.dirname(os.path.abspath(__file__)) + '/..'
        with open(assets_folder + '/assets/runcompss_args.txt', 'r', encoding='utf-8') as f:
            parser_run.epilog = f.read()
            

    parser_run.add_argument("-app", "--app_name",
                             default="",
                             type=str,
                             help="Name of the app where to execute runcompss. Only required for `remote` type environment")
    parser_run.add_argument('rest_args',
                            nargs=argparse.REMAINDER,   
                            help="Runcompss program arguments")

    # APP  remote_parser_app
    parser_app = subparsers.add_parser("app", aliases=["a"], add_help=False,
                                    help="Manage applications within remote environments.",  # noqa: E501
                                    parents=[remote_parser_app()],
                                    formatter_class=FORMATTER_CLASS)
    parser_app.set_defaults(action='app')


    # JOB
    parser_job = subparsers.add_parser("job", aliases=["j"], add_help=False,
                                    help="Manage jobs within remote environments.",  # noqa: E501
                                    parents=[remote_parser_job()],
                                    formatter_class=FORMATTER_CLASS)
    parser_job.set_defaults(action='job')
    

    # MONITOR
    parser_monitor = subparsers.add_parser("monitor",
                                           aliases=["m"],
                                           help="Start the monitor within the COMPSs\' environment.",  # noqa: E501
                                           parents=[parent_parser],
                                           formatter_class=FORMATTER_CLASS)
    parser_monitor.set_defaults(action='monitor')
    
    parser_monitor.add_argument("option",
                                help="Start or stop de monitoring service.",
                                choices=["start", "stop"],
                                default="start",
                                type=str)

    # JUPYTER
    parser_jupyter = subparsers.add_parser("jupyter",
                                           aliases=["jpy"],
                                           help="Starts Jupyter within the COMPSs\' environment.",  # noqa: E501
                                           parents=[parent_parser],
                                           formatter_class=FORMATTER_CLASS)
    parser_jupyter.set_defaults(action='jupyter')
    
    
    parser_jupyter.add_argument("-app", "--app_name",
                             default="",
                             type=str,
                             help="Name of the app where the notebook will be deployed. Only required for `remote` type environment")
    parser_jupyter.add_argument('rest_args', 
                            nargs=argparse.REMAINDER,   
                            help="Jupyter arguments")

    # GENGRAPH
    parser_gengraph = subparsers.add_parser("gengraph",
                                            aliases=["gg"],
                                            help="Converts the given graph into pdf.",  # noqa: E501
                                            parents=[parent_parser],
                                            formatter_class=FORMATTER_CLASS)
    parser_gengraph.set_defaults(action='gengraph')

    parser_gengraph.add_argument("dot_file",
                                 type=str,
                                 help="Dot file to convert to pdf")


    # GENTRACE
    parser_gentrace = subparsers.add_parser("gentrace",
                                            aliases=["gt"],
                                            help="Merges traces from all nodes into a Paraver trace.",  # noqa: E501
                                            parents=[parent_parser],
                                            formatter_class=FORMATTER_CLASS)
    parser_gentrace.set_defaults(action='gentrace')

    parser_gentrace.add_argument("trace_dir",
                                 type=str,
                                 help="Directory where the traces are located.")

    parser_gentrace.add_argument("--download_dir",
                                    type=str,
                                    help="Directory where the traces will be downloaded.")

    parser_gentrace.add_argument('rest_args', 
                            nargs=argparse.REMAINDER,
                            help="compss_gentrace arguments")

    # COMPONENTS
    parser_components = subparsers.add_parser("components",
                                              aliases=["c"],
                                              help="Manage infrastructure components.",  # noqa: E501
                                              parents=[parent_parser],
                                              formatter_class=FORMATTER_CLASS)
    parser_components.set_defaults(action='components')

                                                 
    subparsers_components = parser_components.add_subparsers(dest="components")
    
    subparsers_components.add_parser("list",
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


    # ENVIRONMENT
    parser_environment = subparsers.add_parser("environment",
                                              aliases=["env"],
                                              help="Manage COMPSs environments.",  # noqa: E501
                                              parents=[parent_parser],
                                              formatter_class=FORMATTER_CLASS)

    parser_environment.set_defaults(action='environment')

    subparsers_environment = parser_environment.add_subparsers(dest="environment")

    subparsers_environment.add_parser("list",
                                        aliases=["l"],
                                        help="List COMPSs active environments.",  # noqa: E501
                                        formatter_class=FORMATTER_CLASS)        # noqa: E501
    parser_environment_change = subparsers_environment.add_parser("change",
                                                             aliases=["c"],
                                                             help="Change current COMPSs environment.",  # noqa: E501
                                                             formatter_class=FORMATTER_CLASS)
    parser_environment_change.add_argument("env_id",
                                            type=str,
                                            help="ID of the environment to set as active")  # noqa: E501                 

    parser_environment_remove = subparsers_environment.add_parser("remove",
                                                                aliases=["r"],
                                                                help="Removes COMPSs environment.",  # noqa: E501
                                                                formatter_class=FORMATTER_CLASS)                  # noqa: E501

    parser_environment_remove.add_argument("-f", "--force",
                             action='store_true',
                             default=False,
                             help="Force deleting de environment and the applications") 

    parser_environment_remove.add_argument("env_id",
                                            nargs="+",
                                            type=str,
                                            help="ID of the environment to remove")  # noqa: E501                                                                           

    # Check if the user does not include any argument
    if len(sys.argv) < 2:
        print(parser.print_help())
        sys.exit(1)

    arguments, leftovers = parser.parse_known_args()
    if leftovers:
        arguments.rest_args = leftovers + arguments.rest_args

    return arguments
