import sys
import argparse


FORMATTER_CLASS = argparse.ArgumentDefaultsHelpFormatter


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
    # INIT
    parser_job = subparsers.add_parser("job",
                                        aliases=["j"],
                                        help="Manage COMPSs jobs on cluster or remote.",
                                        parents=[parent_parser],
                                        formatter_class=FORMATTER_CLASS)

    return parser_job