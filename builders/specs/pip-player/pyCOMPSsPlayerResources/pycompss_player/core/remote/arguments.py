import sys
import argparse
import pathlib

FORMATTER_CLASS = argparse.ArgumentDefaultsHelpFormatter

def cluster_init_parser():
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
                                        help="Initialize COMPSs within a given cluster node.",
                                        parents=[parent_parser],
                                        formatter_class=FORMATTER_CLASS)
    parser_init.add_argument("-w", "--working_dir",
                             default=str(pathlib.Path().resolve()),
                             type=str,
                             help="Working directory")

    parser_init.add_argument("-l", "--login",
                             type=str,
                             required=True,
                             help="Login info username@cluster_hostname")

    return parser_init

def cluster_parser_job():
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

    job_subparsers = parser_job.add_subparsers(dest="job")

    submit_job_parser = job_subparsers.add_parser("submit",
                                aliases=["sub"],
                                help="Submit a job to a cluster or remote environment",
                                parents=[parent_parser],
                                formatter_class=FORMATTER_CLASS)

    submit_job_parser.add_argument("application",
                            type=str,
                            help="Application to execute")
    submit_job_parser.add_argument("argument",
                            nargs="*",
                            type=str,
                            help="Application\'s arguments")

    job_subparsers.add_parser("status",
                                aliases=["st"],
                                help="Check status of submitted job",
                                parents=[parent_parser],
                                formatter_class=FORMATTER_CLASS)

    cancel_parser = job_subparsers.add_parser("cancel",
                                aliases=["c"],
                                help="Cancel a submitted job",
                                parents=[parent_parser],
                                formatter_class=FORMATTER_CLASS)

    cancel_parser.add_argument("-jid", "--job_id",
                             required=True,
                             type=str,
                             help="Job ID to cancel")                        

    job_subparsers.add_parser("list",
                                aliases=["l"],
                                help="List all the submitted jobs",
                                parents=[parent_parser],
                                formatter_class=FORMATTER_CLASS)

    return parser_job