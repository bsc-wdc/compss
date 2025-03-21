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
import os
import subprocess

from pycompss_cli.core import utils

FORMATTER_CLASS = argparse.RawDescriptionHelpFormatter

def remote_init_parser():
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
                                        help="Initialize COMPSs within a given remote node.",
                                        parents=[parent_parser],
                                        formatter_class=FORMATTER_CLASS)

    parser_init.add_argument("-l", "--login",
                             type=str,
                             required=True,
                             help="Login info username@remote_hostname")

    parser_init.add_argument("-m", "--modules",
                             help="Module list or file to load in remote environment")

    return parser_init

def remote_parser_app():
    parser = argparse.ArgumentParser(formatter_class=FORMATTER_CLASS)

    # Parent parser - includes all arguments which are common to all actions
    parent_parser = argparse.ArgumentParser(add_help=False,
                                            formatter_class=FORMATTER_CLASS)
    # Action sub-parser
    subparsers = parser.add_subparsers(dest="action")

    # APP
    parser_app = subparsers.add_parser("app",
                                        aliases=["a"],
                                        parents=[parent_parser],
                                        formatter_class=FORMATTER_CLASS)
    parser_app.set_defaults(func=lambda: print(parser_app.format_help()))

    app_subparsers = parser_app.add_subparsers(dest="app")

    # DEPLOY
    app_deploy_parser = app_subparsers.add_parser("deploy",
                                aliases=["d"],
                                help="Deploy an application to a remote environment",
                                parents=[parent_parser],
                                formatter_class=FORMATTER_CLASS)

    app_deploy_parser.add_argument("app_name",
                             type=str,
                             help="Name of the application")

    app_deploy_parser.add_argument("-s", "--source_dir",
                             default='current directory',
                             type=str,
                             help="Directory from which the files will be copied.")

    app_deploy_parser.add_argument("-d", "--destination_dir",
                             type=str,
                             help="Remote destination directory to copy the local app files")

    app_deploy_parser.add_argument("--env_file",
                                type=str,
                                help="File containing modules or environment variables to be set for this application.")

    app_deploy_parser.add_argument("-o", "--overwrite",
                                action="store_true",
                                help="Overwrite the application if it already exists.")

    # REMOVE
    app_remove_parser = app_subparsers.add_parser("remove",
                                aliases=["r"],
                                help="Delete one or more deployed applications",
                                parents=[parent_parser],
                                formatter_class=FORMATTER_CLASS)

    app_remove_parser.add_argument("app_name",
                             type=str,
                             nargs='+',
                             help="Name of the application")

    # LIST
    app_subparsers.add_parser("list",
                                aliases=["l"],
                                help="List all deployed applications",
                                parents=[parent_parser],
                                formatter_class=FORMATTER_CLASS)

    return parser_app

def remote_parser_job():
    """ Parses the sys.argv.

    :returns: All arguments as namespace.
    """
    parser = argparse.ArgumentParser(formatter_class=FORMATTER_CLASS)

    # Parent parser - includes all arguments which are common to all actions
    parent_parser = argparse.ArgumentParser(add_help=False,
                                            formatter_class=FORMATTER_CLASS)
    # Action sub-parser
    subparsers = parser.add_subparsers(dest="action")

    # JOB
    parser_job = subparsers.add_parser("job",
                                        aliases=["j"],
                                        parents=[parent_parser],
                                        formatter_class=FORMATTER_CLASS)
    parser_job.set_defaults(func=lambda: print(parser_job.format_help()))

    job_subparsers = parser_job.add_subparsers(dest="job")

    submit_job_parser = job_subparsers.add_parser("submit",
                                aliases=["sub"],
                                help="Submit a job to a remote environment",
                                parents=[parent_parser],
                                formatter_class=FORMATTER_CLASS)
    
    if utils.check_exit_code('enqueue_compss -h') == 0:
        enqueue_args = subprocess.check_output('enqueue_compss -h', shell=True).decode()
    else:
        assets_folder = os.path.dirname(os.path.abspath(__file__)) + '/../..'
        with open(assets_folder + '/assets/enqueue_compss_args.txt', 'r', encoding='utf-8') as f:
            enqueue_args = f.read()

    submit_job_parser.set_defaults(enqueue_args=enqueue_args)
    submit_job_parser.epilog = enqueue_args

    submit_job_parser.add_argument("--verbose", "-v",
                                    action='store_true',
                                    help="Shows the final command that is executed on the remote")

    submit_job_parser.add_argument("--env_var", "-e",
                                    action='append',
                                    nargs=1,
                                    type=str,
                                    default=[],
                                    help="Set environment variables")  # noqa: E501   

    submit_job_parser.add_argument("-app", "--app_name",
                             default="",
                             type=str,
                             help="Name of the app where to execute runcompss. Only required for `remote` environments")

    submit_job_parser.add_argument('rest_args', 
                            nargs=argparse.REMAINDER,   
                            help="Remote enqueue_compss arguments")

    status_parser = job_subparsers.add_parser("status",
                                aliases=["st"],
                                help="Check status of submitted job",
                                parents=[parent_parser],
                                formatter_class=FORMATTER_CLASS)

    status_parser.add_argument("job_id",
                             type=str,
                             help="Job ID to check status")

    cancel_parser = job_subparsers.add_parser("cancel",
                                aliases=["c"],
                                help="Cancel a submitted job",
                                parents=[parent_parser],
                                formatter_class=FORMATTER_CLASS)

    cancel_parser.add_argument("job_id",
                             type=str,
                             help="Job ID to cancel")

    job_subparsers.add_parser("list",
                                aliases=["l"],
                                help="List all the submitted jobs",
                                parents=[parent_parser],
                                formatter_class=FORMATTER_CLASS)

    history_parser = job_subparsers.add_parser("history",
                                aliases=["h"],
                                help="List all past submitted jobs and their app arguments",
                                parents=[parent_parser],
                                formatter_class=FORMATTER_CLASS)

    history_parser.add_argument("--job_id", '-j',
                             type=str,
                             help="")

    return parser_job
