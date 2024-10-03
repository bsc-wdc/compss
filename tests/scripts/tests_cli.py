#!/usr/bin/env python3

# Imports
import sys
import time

from arguments import get_cli_args
from arguments import ArgumentExit
from arguments import ArgumentError

from configuration import load_configuration_file
from configuration import load_sc_configuration_file
from configuration import ConfigurationError

from compilation_and_deployment import compile_and_deploy_tests
from compilation_and_deployment import TestCompilationError
from compilation_and_deployment import TestDeploymentError

from execution import execute_tests_cli
from execution import TestExecutionError
from execution import str_exit_value_coloured
from execution import get_exit_code

from constants import TESTS_CLI_DIR


def launch_tests():
    """
    Parses the cmd_args, loads the configuration file, and compiles, deploys, and executes the tests
    :return: Exit value indicating whether the tests have failed or not
        + int
    :raise ArgumentError: Error parsing command line arguments
    :raise ArgumentExit: Exit parsing command line arguments
    :raise ConfigurationError: If the provided file path is invalid or there is an error when loading the cfg content
    :raise TestCompilationError: If any error is found during compilation
    :raise TestDeploymentError: If any error is found during deployment
    :raise TestExecutionError: If any error is found during execution (because of infrastructure, not test failure)
    :exit 0: This method exits when test numbering is provided
    """
    # Process command line arguments
    cmd_args = get_cli_args()

    # Load configuration file
    compss_cfg = load_configuration_file(cmd_args.cfg_file)
    compss_cfg_sc = load_sc_configuration_file(cmd_args.cfg_file_sc)

    # Compile and Deploy tests
    compile_and_deploy_tests(cmd_args, compss_cfg, TESTS_CLI_DIR)

    # Execute tests
    return execute_tests_cli(cmd_args, compss_cfg, compss_cfg_sc)


############################################
# MAIN FUNCTION
############################################


def main():
    """
    Main method to execute the tests workflow
    :return:
    """
    # Log start
    print("----------------------------------------")
    print("[INFO] Launching tests...")
    print("----------------------------------------")
    start_time = time.time()

    # Launch main function
    try:
        ev = launch_tests()
    except ArgumentExit as ae:
        print("----------------------------------------")
        print(ae)
        print("----------------------------------------")
        exit(0)  # no error
    except ArgumentError as ae:
        print("----------------------------------------")
        print("[ERROR] Parsing arguments")
        print(ae)
        print("----------------------------------------")
        exit(10)
    except ConfigurationError as ce:
        print("----------------------------------------")
        print("[ERROR] Cannot load configuration file")
        print(ce)
        print("----------------------------------------")
        exit(11)
    except TestCompilationError as tce:
        print("----------------------------------------")
        print("[ERROR] Cannot compile tests")
        print(tce)
        print("----------------------------------------")
        exit(12)
    except TestDeploymentError as tde:
        print("----------------------------------------")
        print("[ERROR] Cannot deploy tests")
        print(tde)
        print("----------------------------------------")
        exit(13)
    except TestExecutionError as tee:
        # WARN: This is received when there is an infrastructure
        # issue executing the tests, not when the tests fail themselves
        print("----------------------------------------")
        print("[ERROR] Cannot execute tests")
        print(tee)
        print("----------------------------------------")
        exit(14)
    else:
        # All tests executed, they may have failed though
        end_time = time.time()
        elapsed_time = end_time - start_time
        print()
        print("----------------------------------------")
        print("[INFO] Tests finished")
        print(f"[INFO]    - Success = {str_exit_value_coloured(ev)}")
        print(f"[INFO]    - Elapsed time = {elapsed_time}")
        print("----------------------------------------")
        sys.exit(get_exit_code(ev))


############################################
# ENTRY POINT
############################################

if __name__ == "__main__":
    main()
