#!/usr/bin/env python3

# Imports
import time
import os
import subprocess

from arguments import get_sc_args
from arguments import ArgumentExit
from arguments import ArgumentError

from configuration import load_sc_configuration_file
from configuration import ConfigurationError

from compilation_and_deployment import compile_and_deploy_tests
from compilation_and_deployment import TestCompilationError
from compilation_and_deployment import TestDeploymentError

from execution import TestExecutionError
from execution import execute_tests_sc
from execution import str_exit_value_coloured
from execution import get_exit_code

from constants import SCRIPT_DIR, REMOTE_SCRIPTS_REL_PATH, TESTS_SC_DIR
from constants import DEFAULT_REL_TARGET_TESTS_DIR


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
    cmd_args = get_sc_args()

    # Load configuration file
    compss_cfg = load_sc_configuration_file(cmd_args.cfg_file)

    # Compile and Deploy tests
    compile_and_deploy_tests(cmd_args, compss_cfg, TESTS_SC_DIR)

    _copy_to_sc(compss_cfg)

    # Execute tests
    return execute_tests_sc(cmd_args, compss_cfg)


def _copy_to_sc(compss_cfg):
    print(f"[INFO] Copy to SC: {compss_cfg}")
    compss_cfg.print_vars()
    username = compss_cfg.get_user()
    remote_dir = os.path.join(
        compss_cfg.get_remote_working_dir(), DEFAULT_REL_TARGET_TESTS_DIR
    )
    target_base_dir = compss_cfg.get_target_base_dir()
    print(f"[INFO] Username: {username}")
    print(f"[INFO] Remote dir: {remote_dir}")
    print(f"[INFO] Target dir: {target_base_dir}")
    print("[INFO] Copying...")
    cp_cmd = [
        "cp",
        "-R",
        os.path.join(SCRIPT_DIR, REMOTE_SCRIPTS_REL_PATH),
        target_base_dir,
    ]
    print(f"CP: {cp_cmd}")
    output = subprocess.check_output(cp_cmd)
    print(f"[INFO] CP OUT: {output}")
    ssh_cmd = ["ssh", username, f"rm -rf {remote_dir}"]
    print(f"SSH: {ssh_cmd}")
    output = subprocess.check_output(ssh_cmd)
    print(f"[INFO] SSH OUT: {output}")
    scp_cmd = ["scp", "-r", target_base_dir, f"{username}:{remote_dir}"]
    print(f"SCP: {scp_cmd}")
    output = subprocess.check_output(scp_cmd)
    print(f"[INFO] SCP OUT: {output}")
    print("[INFO] All tests deployed to Supercomputer")


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
        # WARN: This is received when there is an infrastructure issue
        # executing the tests, not when the tests fail themselves
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
        exit(get_exit_code(ev))


############################################
# ENTRY POINT
############################################

if __name__ == "__main__":
    main()
