#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import os


############################################
# ERROR CLASSES
############################################

class TestCompilationError(Exception):
    """
    Class representing an error when compiling the tests

    :attribute msg: Error message when compiling the tests
        + type: String
    """

    def __init__(self, msg):
        """
        Initializes the TestCompilationError class with the given error message

        :param msg: Error message when compiling the tests
        """
        self.msg = msg

    def __str__(self):
        return str(self.msg)


class TestDeploymentError(Exception):
    """
    Class representing an error when deploying the tests

    :attribute msg: Error message when deploying the tests
        + type: String
    """

    def __init__(self, msg):
        """
        Initializes the TestDeploymentError class with the given error message

        :param msg: Error message when deploying the tests
        """
        self.msg = msg

    def __str__(self):
        return str(self.msg)


############################################
# PUBLIC METHODS
############################################

def compile_and_deploy_tests(cmd_args, compss_cfg, tests_dir):
    """
    Compiles and deploys the required tests

    :param cmd_args: Object representing the command line arguments
        + type: argparse.Namespace
    :param compss_cfg:  Object representing the COMPSs test configuration options available in the given cfg file
        + type: COMPSsConfiguration
    :return:
    :raise TestCompilationError: If any error is found during compilation
    :raise TestDeploymentError: If any error is found during deployment
    """
    # Cleaning previous deployment
    print()
    print("[INFO] Cleaning deployment structure...")
    import shutil
    target_base_dir = compss_cfg.get_target_base_dir()
    try:
        print("[WARN] Script is attempting to erase " + str(target_base_dir))
        print("[WARN] You have 5s to abort...")
        import time
        # time.sleep(5) #  uncomment
        print("[WARN] Erasing deployment structure " + str(target_base_dir))
        shutil.rmtree(target_base_dir)
    except Exception:
        print("[ERROR] Cannot clean target directory " + str(target_base_dir))
        print("        Trying to proceed anyways...")
    compss_base_log_dir = compss_cfg.get_compss_base_log_dir()
    try:
        print("[WARN] Script is attempting to erase " + str(compss_base_log_dir))
        print("[WARN] You have 5s to abort...")
        import time
        # time.sleep(5) #  uncomment
        print("[WARN] Erasing COMPSs log root directory " + str(compss_base_log_dir))
        shutil.rmtree(compss_base_log_dir)
    except Exception:
        print("[ERROR] Cannot clean COMPSs log root directory " + str(compss_base_log_dir))
        print("        Trying to proceed anyways...")

    print("[INFO] Deployment structure cleaned")

    # Create deployment structure
    print("[INFO] Creating deployment structure...")
    try:
        os.makedirs(target_base_dir)
    except OSError:
        raise TestCompilationError("[ERROR] Cannot create base deployment directory: " + str(target_base_dir))

    tests_exec_sandbox = os.path.join(target_base_dir, "apps")
    try:
        os.makedirs(tests_exec_sandbox)
    except OSError:
        raise TestCompilationError(
            "[ERROR] Cannot create executing sandbox deployment directory: " + str(tests_exec_sandbox))

    tests_logs = os.path.join(target_base_dir, "logs")
    try:
        os.makedirs(tests_logs)
    except OSError:
        raise TestCompilationError("[ERROR] Cannot create log deployment directory: " + str(tests_logs))
    print("[INFO] Deployment structure created")

    # Compile and deploy tests
    print()
    print("[INFO] Compiling and Deploying tests...")
    if cmd_args.tests is None or not cmd_args.tests:
        _compile_and_deploy_all(cmd_args, compss_cfg, tests_dir)
    else:
        _compile_and_deploy_specific(cmd_args, compss_cfg)

    # End
    print("[INFO] Tests compiled and deployed")


############################################
# INTERNAL METHODS
############################################

def _compile_and_deploy_all(cmd_args, compss_cfg, tests_dir):
    """
    Compiles and deploys all tests

    :param cmd_args: Object representing the command line arguments
        + type: argparse.Namespace
    :param compss_cfg:  Object representing the COMPSs test configuration options available in the given cfg file
        + type: COMPSsConfiguration
    :return:
    :raise TestCompilationError: If any error is found during compilation
    :raise TestDeploymentError: If any error is found during deployment
    """
    # Compile all test families
    print("[INFO] Compiling all test families...")
    for family in cmd_args.families:
        print("[INFO] Compiling all tests in family " + family)
        family_path = os.path.join(tests_dir, family)
        _compile(family_path, compss_cfg)
    print("[INFO] Tests compiled")

    # Deploy all test families
    print()
    print("[INFO] Deploying tests...")
    target_base_dir = compss_cfg.get_target_base_dir()
    tests_exec_sandbox = os.path.join(target_base_dir, "apps")
    if __debug__:
        print("[DEBUG]   - target_dir : " + str(tests_exec_sandbox))

    for family in cmd_args.families:
        print("[INFO] Deploying all tests in family " + family)
        for test_num, test_info in cmd_args.test_numbers[family].items():
            test_dir, test_path, test_global_num = test_info
            _deploy(test_path, tests_exec_sandbox, test_global_num, cmd_args, compss_cfg)
    print("[INFO] Tests deployed")


def _compile_and_deploy_specific(cmd_args, compss_cfg):
    """
    Compiles and deploys the specified cmd_args tests

    :param cmd_args: Object representing the command line arguments
        + type: argparse.Namespace
    :param compss_cfg:  Object representing the COMPSs test configuration options available in the given cfg file
        + type: COMPSsConfiguration
    :return:
    :raise TestCompilationError: If any error is found during compilation
    :raise TestDeploymentError: If any error is found during deployment
    """
    # Specific tests to compile
    # WARN: Specific tests ignore cmd_args.families
    print("[WARN] Specific tests detected. Ignoring cmd_args.families")
    compiled_tests = []
    for test in cmd_args.tests:
        # Test can be:
        #   - global number
        #   - family:number
        #   - family:test_name

        # Check if its a single number
        try:
            int(test)
            is_global_number = True
        except ValueError:
            is_global_number = False

        if is_global_number:
            # Global number, retrieve test folder
            print("[INFO] Specific test detected as global number")
            test_num = int(test)
            if test_num not in cmd_args.test_numbers["global"].keys():
                raise TestCompilationError("[ERROR] Invalid test number " + str(test_num))
            test_dir, test_path, family_dir, family_num = cmd_args.test_numbers["global"][test_num]
        else:
            # Check if it is a valid family
            family_dir, test_num_or_name = test.split(":")
            if family_dir not in cmd_args.test_numbers.keys():
                raise TestCompilationError("[ERROR] Invalid family " + str(family_dir) + " on specific test")

            # Check if its a family number or name
            try:
                int(test_num_or_name)
                is_family_number = True
            except ValueError:
                is_family_number = False

            if is_family_number:
                # Test is family:number
                print("[INFO] Specific test detected as family number")
                family_num = int(test_num_or_name)
                if family_num not in cmd_args.test_numbers[family_dir].keys():
                    raise TestCompilationError("[ERROR] Invalid family number " + str(family_num) + " on specific test")
                test_dir, test_path, test_num = cmd_args.test_numbers[family_dir][family_num]
            else:
                # Test is family:name
                print("[INFO] Specific test detected as test name")
                test_dir = test_num_or_name
                is_valid = False
                for nf, (td, tp, tn) in cmd_args.test_numbers[family_dir].items():
                    if td == test_dir:
                        is_valid = True
                        break
                if is_valid:
                    test_path = tp
                    test_num = tn
                    family_num = nf
                else:
                    raise TestCompilationError("[ERROR] Invalid test name " + str(test_dir) + " on specific test")

        # Check if it must be skipped
        skip_file_path = os.path.join(test_path, "skip")
        if cmd_args.skip and os.path.isfile(skip_file_path):
            print("[WARN] Test " + test_dir + " will be skipped")
        else:
            # Compile otherwise
            print("[INFO] Compiling specific test")
            print("[INFO]   - number: " + str(test_num))
            print("[INFO]   - family: " + str(family_dir))
            print("[INFO]   - family_number: " + str(family_num))
            print("[INFO]   - name: " + str(test_dir))
            print("[INFO]   - path: " + str(test_path))
            _compile(test_path, compss_cfg)
        # Add the test for deployment (in any case)
        compiled_tests.append((test_dir, test_path, test_num))
    print("[INFO] Tests compiled")

    print()
    print("[INFO] Deploying tests...")
    target_base_dir = compss_cfg.get_target_base_dir()
    tests_exec_sandbox = os.path.join(target_base_dir, "apps")
    if __debug__:
        print("[DEBUG]   - target_dir : " + str(tests_exec_sandbox))
    for test_dir, test_path, test_global_num in compiled_tests:
        print("[INFO] Deploying test " + str(test_dir))
        _deploy(test_path, tests_exec_sandbox, test_global_num, cmd_args, compss_cfg)


def _compile(working_dir, compss_cfg):
    """
    Compiles the sources available in the given working directory

    :param working_dir: Path to source to compile
    :param compss_cfg:  Object representing the COMPSs test configuration options available in the given cfg file
        + type: COMPSsConfiguration
    :return:
    :raise TestCompilationError: If any compilation error has raised
    """

    pom_file = os.path.join(working_dir, "pom.xml")
    if not os.path.isfile(pom_file):
        print("[WARN] No pom.xml file found. Skipping compilation")
    else:
        import subprocess

        cmd = ["mvn", "-U", "clean", "install"]
        exec_env = os.environ.copy()
        exec_env["JAVA_HOME"] = compss_cfg.get_java_home()
        exec_env["COMPSS_HOME"] = compss_cfg.get_compss_home()
        p = subprocess.Popen(cmd, cwd=working_dir, env=exec_env)
        p.communicate()
        exit_value = p.returncode

        # Log command exit_value/output/error
        print("[INFO] Compilation command EXIT_VALUE: " + str(exit_value))

        # Raise an exception if command has failed
        if exit_value != 0:
            raise TestCompilationError("[ERROR] Compile command has failed with exit value: " + str(exit_value))
        print("[INFO] Compilation of " + working_dir + " successful")


def _deploy(source_path, test_exec_sandbox_global, test_num, cmd_args, compss_cfg):
    """
    Executes the test deployment script

    :param source_path: Test source path
    :param test_exec_sandbox_global: Execution deployment path
    :param test_num: Global test number
    :param cmd_args: Object representing the command line arguments
        + type: argparse.Namespace
    :param compss_cfg:  Object representing the COMPSs test configuration options available in the given cfg file
        + type: COMPSsConfiguration
    :return:
    :raise TestCompilationError: If any compilation error has raised
    """

    test_exec_sandbox = os.path.join(test_exec_sandbox_global, "app" + "{:03d}".format(test_num))
    try:
        os.makedirs(test_exec_sandbox)
    except OSError:
        raise TestDeploymentError("[ERROR] Cannot create base execution sandbox directory: " + str(test_exec_sandbox))

    print("[INFO] Deploying " + str(source_path) + " to " + str(test_exec_sandbox))

    # Check if test will be skipped
    skip_file_path = os.path.join(source_path, "skip")
    if cmd_args.skip and os.path.isfile(skip_file_path):
        # Deploy only the skip file
        from shutil import copyfile
        copyfile(skip_file_path, os.path.join(test_exec_sandbox, "skip"))
    else:
        # Regular deploy

        # Search deploy script
        deploy_script_path = os.path.join(source_path, "deploy")
        if not os.path.isfile(deploy_script_path):
            raise TestDeploymentError("[ERROR] Cannot find deploy script " + str(deploy_script_path))

        # Invoke deploy script
        import subprocess
        cmd = [deploy_script_path, source_path, test_exec_sandbox]
        exec_env = os.environ.copy()
        exec_env["JAVA_HOME"] = compss_cfg.get_java_home()
        exec_env["COMPSS_HOME"] = compss_cfg.get_compss_home()
        p = subprocess.Popen(cmd, cwd=source_path, env=exec_env)
        p.communicate()
        exit_value = p.returncode

        # Log command exit_value/output/error
        print("[INFO] Deployment command EXIT_VALUE: " + str(exit_value))

        # Raise an exception if command has failed
        if exit_value != 0:
            raise TestDeploymentError("[ERROR] Deployment command has failed with exit value: " + str(exit_value))
        print("[INFO] Deployment of " + str(source_path) + " completed")
