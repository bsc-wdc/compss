#!/usr/bin/env python3

# Imports
import os
import subprocess
import sys
from enum import Enum
from tabulate import tabulate

from constants import RUNCOMPSS_REL_PATH
from constants import ENQUEUE_COMPSS_REL_PATH
from constants import REMOTE_SCRIPTS_REL_PATH
from constants import CLEAN_PROCS_REL_PATH
from constants import JACOCO_LIB_REL_PATH
from constants import SCRIPT_DIR
from constants import DEFAULT_REL_TARGET_TESTS_DIR
from constants import CONFIGURATIONS_DIR
from constants import PYCOMPSS_SRC_DIR

############################################
# ERROR CLASS
############################################


class TestExecutionError(Exception):
    """
    Class representing an error when executing the tests

    :attribute msg: Error message when executing the tests
        + type: String
    """

    def __init__(self, msg):
        """
        Initializes the TestExecutionError class with the given error message

        :param msg: Error message when executing the tests
        """
        self.msg = msg

    def __str__(self):
        return str(self.msg)


############################################
# HELPER CLASS
############################################


class ExitValue(Enum):
    OK = (0,)
    OK_RETRY = (1,)
    SKIP = (2,)
    UNSUPPORTED = (3,)
    FAIL = 4


def str_exit_value_coloured(exit_value):
    """
    Returns the coloured string representation of the exit_value object

    :param exit_value: ExitValue object
        + type: ExitValue
    :return: The coloured string representation of the exit_value object
        + type: String
    """
    colour_white = "\033[0m"
    colour_red = "\033[31m"
    colour_green = "\033[32m"
    colour_orange = "\033[33m"
    colour_blue = "\033[34m"
    colour_purple = "\033[35m"

    if exit_value == ExitValue.OK:
        return colour_green + exit_value.name + colour_white
    if exit_value == ExitValue.OK_RETRY:
        return colour_orange + exit_value.name + colour_white
    if exit_value == ExitValue.SKIP:
        return colour_blue + exit_value.name + colour_white
    if exit_value == ExitValue.UNSUPPORTED:
        return colour_purple + exit_value.name + colour_white
    # FAIL
    return colour_red + exit_value.name + colour_white


def str_exit_value(exit_value):
    """
    Returns the string representation of the exit_value object

    :param exit_value: ExitValue object
        + type: ExitValue
    :return: The string representation of the exit_value object
        + type: String
    """
    if exit_value == ExitValue.OK:
        return exit_value.name
    if exit_value == ExitValue.OK_RETRY:
        return exit_value.name
    if exit_value == ExitValue.SKIP:
        return exit_value.name
    if exit_value == ExitValue.UNSUPPORTED:
        return exit_value.name
    # FAIL
    return exit_value.name


def _merge_exit_values(ev1, ev2):
    """
    Merges the given two exit values preserving the worst result

    :param ev1: First ExitValue
        + type: ExitValue
    :param ev2: Second ExitValue
        + type: ExitValue
    :return: ExitValue representing the merge of the two given exit values preserving the worst result
        + type: ExitValue
    """
    if ev1 == ExitValue.FAIL or ev2 == ExitValue.FAIL:
        return ExitValue.FAIL
    if ev1 == ExitValue.OK_RETRY or ev2 == ExitValue.OK_RETRY:
        return ExitValue.OK_RETRY
    return ExitValue.OK


def get_exit_code(exit_value):
    if exit_value == ExitValue.OK or exit_value == ExitValue.OK_RETRY:
        return 0
    if exit_value == ExitValue.UNSUPPORTED or exit_value == ExitValue.SKIP:
        return 99
    # FAIL
    return 1


def generate_coverage_reports(jacoco_lib_path, coverage_report_path, compss_home_path):
    import subprocess

    print("[INFO] Generating Coverage reports (" + coverage_report_path + ")...")
    print("[INFO] Merging jacoco reports...")
    coverage_bash_command = (
        "java -jar "
        + jacoco_lib_path
        + "/jacococli.jar merge "
        + coverage_report_path
        + "/*.exec --destfile "
        + coverage_report_path
        + "/temp/jacocoreport.exec"
    )
    output = subprocess.check_output(["bash", "-c", coverage_bash_command])

    coverage_bash_command = "rm -r " + coverage_report_path + "/*.exec"
    output = subprocess.check_output(["bash", "-c", coverage_bash_command])

    coverage_bash_command = (
        "mv " + coverage_report_path + "/temp/jacocoreport.exec " + coverage_report_path
    )
    output = subprocess.check_output(["bash", "-c", coverage_bash_command])
    output = subprocess.check_output(
        ["bash", "-c", "rm -r " + coverage_report_path + "/temp"]
    )

    coverage_bash_command = (
        "coverage combine --rcfile=" + coverage_report_path + "/coverage_rc"
    )
    try:
        print(
            "[INFO] Merging combining python reports (" + coverage_bash_command + ")..."
        )
        subprocess.check_output(["bash", "-c", coverage_bash_command])

        coverage_bash_command = (
            "coverage xml --rcfile=" + coverage_report_path + "/coverage_rc"
        )
        print(
            "[INFO] Merging generating cobertura xml report ("
            + coverage_bash_command
            + ")..."
        )
        subprocess.check_output(["bash", "-c", coverage_bash_command])
        # Not required with [paths] tag in coverage_rc
        for i in ["2", "3"]:
            coverage_bash_command = (
                "sed -i 's#"
                + compss_home_path
                + "Bindings/python/"
                + i
                + "#src#g' "
                + coverage_report_path
                + "/coverage.xml"
            )
            print(
                "[INFO] Correcting path to source paths ("
                + coverage_bash_command
                + ")..."
            )
            subprocess.check_output(["bash", "-c", coverage_bash_command])
    except subprocess.CalledProcessError as e:
        print("Error generating coverage report")
        print(e)


def create_coverage_file(coverage_rc_path, tests_output_path):
    fin = open(CONFIGURATIONS_DIR + "/coverage_rc", "rt")
    fout = open(coverage_rc_path, "w")
    for line in fin:
        line = line.replace("@TEST_OUTPUT_PATH@", tests_output_path)
        line = line.replace("@PYCOMPSS_SRC_PATH@", PYCOMPSS_SRC_DIR)
        fout.write(line)
    fin.close()
    fout.close()


############################################
# PUBLIC METHODS
############################################


def execute_tests(cmd_args, compss_cfg):
    """
    Executes all the deployed tests and builds a result summary table.
    If failfast option is set, once a test fails the script exits.

    :param cmd_args: Object representing the command line arguments
        + type: argparse.Namespace
    :param compss_cfg:  Object representing the COMPSs test configuration options available
                        in the given cfg file
        + type: COMPSsConfiguration
    :return: An ExitValue object indicating the exit status of the WORST test execution
        + type: ExitValue
    :raise TestExecutionError: If an error is encountered when creating the necessary
                               structures to launch the test
    """
    # Load deployment structure folder paths
    compss_logs_root = compss_cfg.get_compss_log_dir()
    target_base_dir = compss_cfg.get_target_base_dir()
    execution_sanbdox = os.path.join(target_base_dir, "apps")
    coverage_path = os.path.join(target_base_dir, "coverage")
    jaccoco_lib_path = compss_cfg.get_compss_home() + JACOCO_LIB_REL_PATH

    if cmd_args.coverage:
        print("[INFO] Coverage mode enabled")
        try:
            os.makedirs(coverage_path)
        except OSError as exc:
            raise TestExecutionError(
                f"[ERROR] Cannot create coverage dir {coverage_path}"
            ) from exc

        coverage_expression = (
            "--coverage="
            + jaccoco_lib_path
            + "/jacocoagent.jar=destfile="
            + coverage_path
            + "/report_id.exec"
        )
        # coverage_paths[2] = coverage_paths[2].replace("#","@")
        # coverage_expression = "--coverage="+coverage_paths[0]+"/jacocoagent.jar=destfile="+coverage_paths[1]+"/report_id.exec"+"#"+coverage_paths[2]
        print("[INFO] Coverage expression: " + coverage_expression)
        create_coverage_file(coverage_path + "/coverage_rc", target_base_dir)
        print("[INFO] File coverage_rc generated")
    # Execute all the deployed tests
    results = []

    for test_dir in sorted(os.listdir(execution_sanbdox)):
        old_runcompss_opts = compss_cfg.runcompss_opts
        if cmd_args.coverage:
            old_runcompss_opts = compss_cfg.runcompss_opts
            coverage_expression = coverage_expression.replace("id", test_dir)
            if old_runcompss_opts is None:
                compss_cfg.runcompss_opts = coverage_expression
            else:
                compss_cfg.runcompss_opts = (
                    compss_cfg.runcompss_opts + " " + coverage_expression
                )
            print(
                "[INFO] Modified runcompss_opt with coverage: "
                + compss_cfg.runcompss_opts
            )
        test_path = os.path.join(execution_sanbdox, test_dir)
        ev, exec_time = _execute_test(
            test_dir, test_path, compss_logs_root, cmd_args, compss_cfg
        )
        results.append((test_dir, ev, exec_time))
        compss_cfg.runcompss_opts = old_runcompss_opts
        if cmd_args.fail_fast and ev == ExitValue.FAIL:
            print()
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            print("[ERROR] Test has failed and fail-fast option is set. Aborting...")
            break

    # Process test results
    headers = [
        "Test\nG. Id",
        " Test \nFamily",
        " Test  \nFam. Id",
        "Test Name",
        "Test Exec.\n  Folder",
        " Test\nResult",
        "Execution\n Time (s)",
    ]
    results_info = []
    global_ev = ExitValue.OK
    for test_dir, ev, test_time in results:
        # Update global exit value
        global_ev = _merge_exit_values(global_ev, ev)
        # Colour the test exit value
        ev_color_str = str_exit_value(ev)
        # Retrieve test information
        test_global_num = int("".join(x for x in test_dir if x.isdigit()))
        test_name, _, family_dir, num_family = cmd_args.test_numbers["global"][
            test_global_num
        ]
        # Append all information for rendering
        results_info.append(
            [
                test_global_num,
                family_dir,
                num_family,
                test_name,
                test_dir,
                ev_color_str,
                test_time,
            ]
        )

    # Print result summary table
    print()
    print("----------------------------------------")
    print("TEST RESULTS SUMMARY:")
    print()
    print(tabulate(results_info, headers=headers))
    print("----------------------------------------")

    if cmd_args.coverage:
        generate_coverage_reports(
            jaccoco_lib_path, coverage_path, compss_cfg.get_compss_home()
        )
    # Return if any test has failed
    return global_ev


def execute_tests_sc(cmd_args, compss_cfg):
    import polling

    username = compss_cfg.get_user()
    module = compss_cfg.get_compss_module()
    comm = compss_cfg.get_comm()
    remote_dir = os.path.join(
        compss_cfg.get_remote_working_dir(), DEFAULT_REL_TARGET_TESTS_DIR
    )
    exec_envs = compss_cfg.get_execution_envs_str()
    runcompss_opts = compss_cfg.get_runcompss_opts()
    queue = compss_cfg.get_queue()
    qos = compss_cfg.get_qos()
    project_name = compss_cfg.get_project_name()
    batch = compss_cfg.get_batch()

    # Initialize tests results vars
    headers = [
        "Test\nG. Id",
        " Test \nFamily",
        " Test  \nFam. Id",
        "Test Name",
        "Test Exec.\n  Folder",
        "Test \nJobID",
        " Test Exec.\nEnvironment",
        " Test\nResult",
    ]
    results_info = []
    global_ev = ExitValue.OK

    # Calculate max tests
    num_tests = len(os.listdir(os.path.join(compss_cfg.get_target_base_dir(), "apps")))
    if batch == 0:
        batch = num_tests
    start = 0
    while start <= num_tests:
        end = start + batch
        if end > num_tests:
            end = num_tests
        if runcompss_opts is None:
            runcompss_opts = "none"
        runcompss_bin = "enqueue_compss"
        enqueue_tests_script = os.path.join(
            remote_dir, REMOTE_SCRIPTS_REL_PATH, "enqueue_tests.py"
        )
        results_script = os.path.join(remote_dir, REMOTE_SCRIPTS_REL_PATH, "results.py")

        # Add more parameters before exec_env. Let exec_envs for the last argument
        remote_cmd = (
            "python "
            + enqueue_tests_script
            + " "
            + remote_dir
            + " "
            + runcompss_bin
            + " "
            + comm
            + " "
            + runcompss_opts
            + " "
            + module
            + " "
            + queue
            + " "
            + qos
            + " "
            + project_name
            + " "
            + str(start)
            + " "
            + str(end)
            + " "
            + exec_envs
        )
        cmd = f"ssh {username} '{remote_cmd}'"
        print(f"Executing command: {cmd}")
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
        )
        out, err = process.communicate()
        if process.returncode != 0:
            print(f"[ERROR] Executing command: {cmd}")
            print(f"[ERROR] OUT: {out}")
            print(f"[ERROR] ERR: {err}")
            sys.exit(1)
        print(f"out: {out}")
        print(f"err: {err}")
        out = str(out.decode())
        print("[INFO] Executing tests on Supercomputer:")
        jobs = out.split("\n")[:-1]
        print(f"[INFO] Jobs: {jobs}")
        if len(jobs) > 0:
            for job in jobs:
                if job != "0":
                    print(f"[INFO] Waiting for job {job}")
                    try:
                        polling.poll(
                            lambda: not subprocess.check_output(
                                'ssh {} "squeue -h -j {}"'.format(username, job),
                                shell=True,
                            ),
                            step=30,
                            poll_forever=True,
                        )
                    except Exception:
                        print(f"[WARN] Error getting status of job {job}")
            print("[INFO] All jobs finished")
            print("[INFO] Checking results")
            cmd = (
                "ssh "
                + username
                + " "
                + "'python "
                + results_script
                + " "
                + remote_dir
                + " "
                + str(start)
                + " "
                + str(end)
                + "'"
            )
            process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
            )
            out, err = process.communicate()
            if process.returncode != 0:
                print("[ERROR] Failure in tests results")
                print(f"[ERROR] OUT: {out}")
                print(f"[ERROR] ERR: {err}")
        else:
            print("[ERROR] All tests in family are skipped or family is empty")
        start = end + 1

    outs_file = os.path.join(remote_dir, "outs.csv")
    cmd = f"scp {username}:{outs_file} /tmp"
    results = True
    try:
        subprocess.check_output(cmd, shell=True)
    except subprocess.CalledProcessError as e:
        print("WARNING: outs.csv has not been generated.")
        print(e.output)
        results = False
    else:
        print("WARNING: Could not process outs.csv")

    if results:
        # Process test results
        with open("/tmp/outs.csv", "r", encoding="UTF-8") as res_file:
            for line in res_file:
                print(f"Checking line: {line}")
                test_dir, environment, job_id, exit_value = line.split(",")
                if int(exit_value) == 0:
                    ev = ExitValue.OK
                elif int(exit_value) == 2:
                    ev = ExitValue.SKIP
                else:
                    ev = ExitValue.FAIL
                # Update global exit value
                global_ev = _merge_exit_values(global_ev, ev)
                # Get the test exit value
                ev_color_str = str_exit_value(ev)
                # Retrieve test information
                test_global_num = int("".join(x for x in test_dir if x.isdigit()))
                test_name, _, family_dir, num_family = cmd_args.test_numbers["global"][
                    test_global_num
                ]
                # Append all information for rendering
                results_info.append(
                    [
                        test_global_num,
                        family_dir,
                        num_family,
                        test_name,
                        test_dir,
                        job_id,
                        environment,
                        ev_color_str,
                    ]
                )

    # Print result summary table
    print()
    print("----------------------------------------")
    print("TEST RESULTS SUMMARY:")
    print()
    print(tabulate(results_info, headers=headers))
    print("----------------------------------------")
    return global_ev


def execute_tests_cli(cmd_args, compss_cfg, compss_cfg_sc):
    """
    Executes all the deployed tests and builds a result summary table.
    If failfast option is set, once a test fails the script exits.

    :param cmd_args: Object representing the command line arguments
        + type: argparse.Namespace
    :param compss_cfg:  Object representing the COMPSs test configuration options available in the given cfg file
        + type: COMPSsConfiguration
    :return: An ExitValue object indicating the exit status of the WORST test execution
        + type: ExitValue
    :raise TestExecutionError: If an error is encountered when creating the necessary structures to launch the test
    """
    # Load deployment structure folder paths
    compss_logs_root = compss_cfg.get_compss_log_dir()
    target_base_dir = compss_cfg.get_target_base_dir()
    execution_sanbdox = os.path.join(target_base_dir, "apps")
    coverage_path = os.path.join(target_base_dir, "coverage")
    jaccoco_lib_path = compss_cfg.get_compss_home() + JACOCO_LIB_REL_PATH

    if cmd_args.coverage:
        print("[INFO] Coverage mode enabled")
        try:
            os.makedirs(coverage_path)
        except OSError as exc:
            raise TestExecutionError(
                f"[ERROR] Cannot create coverage dir {coverage_path}"
            ) from exc

        coverage_expression = (
            "--coverage="
            + jaccoco_lib_path
            + "/jacocoagent.jar=destfile="
            + coverage_path
            + "/report_id.exec"
        )
        # coverage_paths[2] = coverage_paths[2].replace("#","@")
        # coverage_expression = "--coverage="+coverage_paths[0]+"/jacocoagent.jar=destfile="+coverage_paths[1]+"/report_id.exec"+"#"+coverage_paths[2]
        print("[INFO] Coverage expression: " + coverage_expression)
        create_coverage_file(coverage_path + "/coverage_rc", target_base_dir)
        print("[INFO] File coverage_rc generated")
    # Execute all the deployed tests
    results = []

    for test_dir in sorted(os.listdir(execution_sanbdox)):
        old_runcompss_opts = compss_cfg.runcompss_opts
        if cmd_args.coverage:
            old_runcompss_opts = compss_cfg.runcompss_opts
            coverage_expression = coverage_expression.replace("id", test_dir)
            if old_runcompss_opts is None:
                compss_cfg.runcompss_opts = coverage_expression
            else:
                compss_cfg.runcompss_opts = (
                    compss_cfg.runcompss_opts + " " + coverage_expression
                )
            print(
                "[INFO] Modified runcompss_opt with coverage: "
                + compss_cfg.runcompss_opts
            )
        test_path = os.path.join(execution_sanbdox, test_dir)
        ev, exec_time = _execute_test(
            test_dir,
            test_path,
            compss_logs_root,
            cmd_args,
            compss_cfg,
            compss_cfg_sc=compss_cfg_sc,
        )
        results.append((test_dir, ev, exec_time))
        compss_cfg.runcompss_opts = old_runcompss_opts
        if cmd_args.fail_fast and ev == ExitValue.FAIL:
            print()
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            print("[ERROR] Test has failed and fail-fast option is set. Aborting...")
            break

    # Process test results
    headers = [
        "Test\nG. Id",
        " Test \nFamily",
        " Test  \nFam. Id",
        "Test Name",
        "Test Exec.\n  Folder",
        " Test\nResult",
        "Execution\n Time (s)",
    ]
    results_info = []
    global_ev = ExitValue.OK
    for test_dir, ev, test_time in results:
        # Update global exit value
        global_ev = _merge_exit_values(global_ev, ev)
        # Colour the test exit value
        ev_color_str = str_exit_value(ev)
        # Retrieve test information
        test_global_num = int("".join(x for x in test_dir if x.isdigit()))
        test_name, _, family_dir, num_family = cmd_args.test_numbers["global"][
            test_global_num
        ]
        # Append all information for rendering
        results_info.append(
            [
                test_global_num,
                family_dir,
                num_family,
                test_name,
                test_dir,
                ev_color_str,
                test_time,
            ]
        )

    # Print result summary table
    print()
    print("----------------------------------------")
    print("TEST RESULTS SUMMARY:")
    print()
    print(tabulate(results_info, headers=headers))
    print("----------------------------------------")

    if cmd_args.coverage:
        generate_coverage_reports(
            jaccoco_lib_path, coverage_path, compss_cfg.get_compss_home()
        )
    # Return if any test has failed
    return global_ev


############################################
# INTERNAL METHODS
############################################


def _execute_test(
    test_name, test_path, compss_logs_root, cmd_args, compss_cfg, compss_cfg_sc=None
):
    """
    Executes the given test with the given options and retrieves its exit value

    :param test_name: Name of the test (on deployment phase: #appXXX)
        + type: String
    :param test_path: Path of the test (on deployment phase)
        + type: String
    :param compss_logs_root: Root folder of the COMPSs logs (usually ~/.COMPSs)
        + type: String
    :param cmd_args: Object representing the command line arguments
        + type: argparse.Namespace
    :param compss_cfg: Object representing the COMPSs test configuration options available in the given cfg file
        + type: COMPSsConfiguration
    :return: An ExitValue object indicating the exit status of the test execution
        + type: ExitValue
    :return: Time spent on the test execution
        + type: Long
    :raise TestExecutionError: If an error is encountered when creating the necessary
                               structures to launch the test
    """
    import time

    skip_file = os.path.join(test_path, "skip")
    if os.path.isfile(skip_file):
        print("[INFO] Skipping test " + str(test_name))
        return ExitValue.SKIP, 0

    # Else, execute test normally
    print("[INFO] Executing test " + str(test_name))

    target_base_dir = compss_cfg.get_target_base_dir()
    logs_sanbdox = os.path.join(target_base_dir, "logs")

    max_retries = cmd_args.retry
    retry = 1
    test_ev = ExitValue.FAIL
    start_time = time.time()
    while test_ev == ExitValue.FAIL and retry <= max_retries:
        if __debug__:
            print(
                "[DEBUG] Executing test "
                + str(test_name)
                + " Retry: "
                + str(retry)
                + "/"
                + str(max_retries)
            )
        # Create logs folder for current retry
        test_logs_path = os.path.join(logs_sanbdox, test_name + "_" + str(retry))
        try:
            os.makedirs(test_logs_path)
        except OSError as exc:
            raise TestExecutionError(
                f"[ERROR] Cannot create application log dir {test_logs_path}"
            ) from exc
        # Execute test specific execution file
        test_ev = _execute_test_cmd(
            test_path,
            test_logs_path,
            compss_logs_root,
            retry,
            compss_cfg,
            compss_cfg_sc=compss_cfg_sc,
        )
        # Clean orphan processes (if any)
        _clean_procs(compss_cfg)
        # Sleep between executions
        time.sleep(4)
        # Increase retry counter
        retry = retry + 1
    end_time = time.time()
    # Return test exit value
    print("[INFO] Executed test " + str(test_name) + " with ExitValue " + test_ev.name)
    return test_ev, "%.3f" % (end_time - start_time)


def _execute_test_cmd(
    test_path, test_logs_path, compss_logs_root, retry, compss_cfg, compss_cfg_sc=None
):
    """
    Executes the execution script of a given test

    :param test_path: Path to the test deployment folder
        + type: String
    :param test_logs_path: Path to store the execution logs
        + type: String
    :param compss_logs_root: Path of the root COMPSs log folder
        + type: String
    :param retry: Retry number
        + type: int
    :param compss_cfg: Object representing the COMPSs test configuration options available in the given cfg file
        + type: COMPSsConfiguration
    :return: An ExitValue object indicating the exit status of the test execution
        + type: ExitValue
    :raise TestExecutionError: If an error is encountered when creating the necessary structures to launch the test
    """
    # Create command
    execution_script_path = os.path.join(test_path, "execution")
    if not os.path.isfile(execution_script_path):
        raise TestExecutionError(
            "[ERROR] Cannot find execution script " + str(execution_script_path)
        )

    runcompss_bin = compss_cfg.get_compss_home() + RUNCOMPSS_REL_PATH
    runcompss_user_opts = compss_cfg.get_runcompss_opts()
    if runcompss_user_opts is None:
        runcompss_user_opts = ""
    cmd = [
        str(execution_script_path),
        str(runcompss_bin),
        str(compss_cfg.get_comm()),
        str(runcompss_user_opts),
        str(test_path),
        str(compss_logs_root),
        str(test_logs_path),
        str(retry),
        str(compss_cfg.get_execution_envs_str()),
    ]

    if compss_cfg_sc:
        username = compss_cfg_sc.get_user()
        module = compss_cfg_sc.get_compss_module()
        cmd += [str(username), str(module)]

    if __debug__:
        print("[DEBUG] Test execution command: " + str(cmd))
    # Invoke execution script
    try:
        exec_env = os.environ.copy()
        exec_env["JAVA_HOME"] = compss_cfg.get_java_home()
        exec_env["COMPSS_HOME"] = compss_cfg.get_compss_home()
        p = subprocess.Popen(cmd, cwd=test_path, env=exec_env)
        p.communicate()
        exit_value = p.returncode
    except Exception:
        exit_value = -1

    # Log command exit_value/output/error
    print("[INFO] Text execution command EXIT_VALUE: " + str(exit_value))

    # Return exit status
    if exit_value == 0:
        if retry == 1:
            return ExitValue.OK
        return ExitValue.OK_RETRY
    if exit_value == 99:
        return ExitValue.UNSUPPORTED
    return ExitValue.FAIL


def _clean_procs(compss_cfg):
    """
    Cleans the remaining compss processes if any

    :param compss_cfg: Object representing the COMPSs test configuration options available in the given cfg file
        + type: COMPSsConfiguration
    :return:
    """
    clean_procs_bin = compss_cfg.get_compss_home() + CLEAN_PROCS_REL_PATH
    cmd = [clean_procs_bin]

    try:
        p = subprocess.Popen(cmd)
        p.communicate()
        exit_value = p.returncode
    except Exception:
        exit_value = -1

    if exit_value != 0:
        print(
            "[WARN] Captured error while executing clean_compss_procs between test executions. Proceeding anyways..."
        )
        print("[WARN] clean_compss_procs command EXIT_VALUE: " + str(exit_value))
