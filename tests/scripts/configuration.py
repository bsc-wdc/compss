#!/usr/bin/env python3

# Imports
import configparser
import os
import getpass

from constants import DEFAULT_CFG_EXTENSION
from constants import CONFIGURATIONS_DIR
from constants import DEFAULT_COMPSS_HOME
from constants import DEFAULT_COMM
from constants import DEFAULT_EXECUTION_ENVS
from constants import DEFAULT_SC_EXECUTION_ENVS
from constants import DEFAULT_COMPSS_MODULE
from constants import DEFAULT_REL_TARGET_TESTS_DIR
from constants import DEFAULT_REL_COMPSS_LOG_DIR


############################################
# ERROR CLASS
############################################


class ConfigurationError(Exception):
    """
    Class representing an error when loading the configuration file

    :attribute msg: Error message when loading the configuration file
        + type: String
    """

    def __init__(self, msg):
        """
        Initializes the ConfigurationError class with the given error message

        :param msg: Error message when loading the configuration file
        """
        self.msg = msg

    def __str__(self):
        return str(self.msg)


############################################
# HELPER CLASS
############################################


class COMPSsConfiguration:
    """
    Class containing the configuration options provided by the COMPSs test configuration file

    :attribute user: User executing the tests
        + type: String
    :attribute target_base_dir: Path to store the tests execution sandbox
        + type: String
    :attribute compss_log_dir: Path to the COMPSs base log directory
        + type: String
    :attribute java_home: JAVA_HOME environment variable
        + type: String or None
    :attribute compss_home: COMPSS_HOME environment variable
        + type: String or None
    :attribute comm: Runtime Adaptor class to be loaded on tests execution
        + type: String
    :attribute runcompss_opts: Extra options for the runcompss command
        + type: List<String> or None
    :attribute execution_envs: List of different execution environments for each test
        + type: List<String>
    """

    def __init__(
        self,
        user=None,
        java_home=None,
        compss_home=DEFAULT_COMPSS_HOME,
        target_base_dir=None,
        comm=DEFAULT_COMM,
        runcompss_opts=None,
        execution_envs=DEFAULT_EXECUTION_ENVS,
    ):
        """
        Initializes the COMPSsConfiguration class with the given options

        :param user: User executing the tests
        :param java_home: JAVA_HOME environment variable
        :param compss_home: COMPSS_HOME environment variable
        :param target_base_dir:  Path to store the tests execution sandbox
        :param comm: Adaptor class to be loaded on tests execution
        :param runcompss_opts: Extra options for the runcompss command
        :param execution_envs: List of different execution environments for each test
        :raise ConfigurationError: If some mandatory variable is undefined
        """

        # Either we receive a user from cfg or we load it from current user (always defined)
        if user is None:
            self.user = getpass.getuser()
        else:
            self.user = user

        # Either we receive a java_home or we load it from environment, if not defined we raise an exception
        if java_home is not None:
            self.java_home = java_home
        else:
            # Load from env
            self.java_home = os.getenv("JAVA_HOME", None)
            if self.java_home is None:
                raise ConfigurationError(
                    "[ERROR] Undefined variable JAVA_HOME in both the configuration file and the environment"
                )

        # Store COMPSs_HOME (always defined because it has a default value)
        self.compss_home = compss_home

        # Define compss_log_dir
        user_home = os.path.expanduser("~")
        self.compss_log_dir = os.path.join(user_home, DEFAULT_REL_COMPSS_LOG_DIR)

        # Either we receive the target_base_dir or we compute it from user home
        if target_base_dir is None:
            self.target_base_dir = os.path.join(user_home, DEFAULT_REL_TARGET_TESTS_DIR)
        else:
            self.target_base_dir = target_base_dir

        # Receive comm (always defined, has default value)
        self.comm = comm
        # Receive comm (can be None)
        self.runcompss_opts = runcompss_opts
        # Receive comm (always defined, has default value)
        self.execution_envs = execution_envs

    def get_user(self):
        """
        Returns the user executing the tests

        :return: The user executing the tests
            + type: String
        """
        return self.user

    def get_java_home(self):
        """
        Returns the JAVA_HOME environment variable

        :return: The JAVA_HOME environment variable
            + type: String or None
        """
        return self.java_home

    def get_compss_home(self):
        """
        Returns the COMPSS_HOME environment variable

        :return: The COMPSS_HOME environment variable
            + type: String or None
        """
        return self.compss_home

    def get_target_base_dir(self):
        """
        Returns the path to store the tests execution sandbox

        :return: The path to store the tests execution sandbox
            + type: String
        """
        return self.target_base_dir

    def get_compss_log_dir(self):
        """
        Returns the path to the COMPSs log base directory

        :return: The path to the COMPSs log base directory
            + type: String
        """
        return self.compss_log_dir

    def get_comm(self):
        """
        Returns the adaptor class to be loaded on tests execution

        :return: The adaptor class to be loaded on tests execution
            + type: String
        """
        return self.comm

    def get_runcompss_opts(self):
        """
        Returns the extra options for the runcompss command

        :return: The extra options for the runcompss command
            + type: List<String> or None
        """
        return self.runcompss_opts

    def get_execution_envs(self):
        """
        Returns the list of different execution environments for each test

        :return: The list of different execution environments for each test
            + type: List<String>
        """
        return self.execution_envs

    def get_execution_envs_str(self):
        """
        Returns the list of different execution environments for each test

        :return: A string representing the list of different execution environments for each test
            + type: String
        """

        return " ".join(str(x) for x in self.execution_envs)

    def print_vars(self):
        """Show the variables."""
        print(f"[INFO]   - user: {self.user}")
        print(f"[INFO]   - java_home: {self.java_home}")
        print(f"[INFO]   - compss_home: {self.compss_home}")
        print(f"[INFO]   - target_dir: {self.target_base_dir}")
        print(f"[INFO]   - comm: {self.comm}")
        print(f"[INFO]   - runcompss_opts: {self.runcompss_opts}")
        print(f"[INFO]   - execution_envs: {self.execution_envs}")


class COMPSsSCConfiguration(COMPSsConfiguration):
    """
    Class containing the configuration options for sc tests
    extends :class: `COMPSsConfiguration`
    :attribute remote_working_dir: working directory in remote supercomputer
        + type: String
    :attribute compss_module: COMPSs module used for tests
        + type: String
    :attribute qos: Quality of Service for the execution of tests
        + type: String
    :attribute queue: Queue used for the execution of tests
        + type: String
    :attribute project_name: Project name used for the execution of tests
        + type: String
    """

    def __init__(
        self,
        remote_working_dir=None,
        compss_module=DEFAULT_COMPSS_MODULE,
        queue="none",
        qos="none",
        project_name="none",
        user=None,
        java_home=None,
        compss_home=DEFAULT_COMPSS_HOME,
        target_base_dir=None,
        comm=DEFAULT_COMM,
        runcompss_opts=None,
        execution_envs=DEFAULT_SC_EXECUTION_ENVS,
        batch="0",
    ):
        COMPSsConfiguration.__init__(
            self,
            user,
            java_home,
            compss_home,
            target_base_dir,
            comm,
            runcompss_opts,
            execution_envs,
        )

        if remote_working_dir is None:
            raise ConfigurationError("[ERROR] Undefined variable remote_working_dir")
        self.remote_working_dir = remote_working_dir
        self.compss_module = os.getenv("TEST_COMPSS_MODULE", None)
        if self.compss_module is None:
            self.compss_module = compss_module
        else:
            print("[WARN] Overwriting COMPSs Module to test")
        self.qos = qos
        self.queue = queue
        self.project_name = project_name
        self.batch = int(batch)

    def get_remote_working_dir(self):
        """
        Returns the path to the COMPSs log base directory

        :return: The path to the COMPSs log base directory
            + type: String
        """
        return self.remote_working_dir

    def get_compss_module(self):
        """
        Returns the path to the COMPSs log base directory

        :return: The path to the COMPSs log base directory
            + type: String
        """
        return self.compss_module

    def get_qos(self):
        """
        Returns the Quality of Service used for the execution of tests

        :return: The quality of service
            + type: String
        """
        return self.qos

    def get_queue(self):
        """
        Returns the queue used for the execution of tests

        :return: The queue name
            + type: String
        """
        return self.queue

    def get_project_name(self):
        """
        Returns the project name used for the execution of tests

        :return: The project name name
            + type: String
        """
        return self.project_name

    def get_batch(self):
        """
        Returns the batch size used for the execution of tests

        :return: The batch size
            + type: int
        """
        return self.batch

    def print_vars(self):
        """
        Prints de configuration variables
        """
        COMPSsConfiguration.print_vars(self)
        print(f"[INFO]   - compss_modules: {self.compss_module}")
        print(f"[INFO]   - remote_dir: {self.remote_working_dir}")
        print(f"[INFO]   - queue: {self.queue}")
        print(f"[INFO]   - qos: {self.qos}")
        print(f"[INFO]   - project_name: {self.project_name}")
        print(f"[INFO]   - batch: {self.batch}")


############################################
# PUBLIC METHODS
############################################


def load_configuration_file(cfg_file):
    """
    Loads the configuration file provided in the cfg_file path

    :param cfg_file: Path to the configuration file
                     (relative or absolute, with or without extension)
    :return: An object representing the COMPSs test configuration
             options available in the given cfg file
        + type: COMPSsConfiguration
    :raise ConfigurationError: If the provided file path is invalid or there
                               is an error when loading the cfg content
    """
    print()
    print("[INFO] Loading configuration file...")

    cfg_file = _check_file(cfg_file)

    # Load cfg values
    print("[INFO] Loading values from " + str(cfg_file))
    config = configparser.ConfigParser()
    config.read(cfg_file)
    # Load default variables
    cfg_vars = {k: v for k, v in config.items("DEFAULT")}

    _check_common_vars(cfg_vars, config)

    if __debug__:
        print("[DEBUG] Retrieved CFG variables: " + str(cfg_vars))

    # Create COMPSs Configuration
    compss_cfg = COMPSsConfiguration(**cfg_vars)

    # Log and return configuration object
    print("[INFO] Configuration file loaded")
    compss_cfg.print_vars()

    return compss_cfg


def load_sc_configuration_file(cfg_file):
    """
    Loads the configuration file provided in the cfg_file path

    :param cfg_file: Path to the configuration file (relative or absolute, with or without extension)
    :return: An object representing the COMPSs test configuration options available in the given cfg file
        + type: COMPSsConfiguration
    :raise ConfigurationError: If the provided file path is invalid or there is an error when loading the cfg content
    """
    print()
    print("[INFO] Loading configuration file...")

    cfg_file = _check_file(cfg_file)

    # Load cfg values
    print(f"[INFO] Loading values from {cfg_file}")
    config = configparser.ConfigParser()
    config.read(cfg_file)
    # Load default variables
    cfg_vars = {k: v for k, v in config.items("DEFAULT")}

    _check_common_vars(cfg_vars, config)

    if __debug__:
        print(f"[DEBUG] Retrieved CFG variables: {cfg_vars}")

    # Create COMPSs Configuration
    compss_cfg = COMPSsSCConfiguration(**cfg_vars)

    # Log and return configuration object
    print("[INFO] Configuration file loaded")
    compss_cfg.print_vars()

    return compss_cfg


def _check_file(cfg_file):
    # Fix extension and absolute path
    if __debug__:
        print(f"[DEBUG] Checking cfg_file path: {cfg_file}")

    cfg_basename, cfg_extension = os.path.splitext(cfg_file)
    if cfg_extension is None or not cfg_extension:
        if __debug__:
            print(f"[DEBUG] Adding default extension {DEFAULT_CFG_EXTENSION}")
        cfg_extension = DEFAULT_CFG_EXTENSION
        cfg_file = cfg_basename + cfg_extension
    if not os.path.isabs(cfg_file):
        cfg_file = os.path.join(CONFIGURATIONS_DIR, cfg_file)

    if __debug__:
        print(f"[DEBUG] Complete cfg_file path: {cfg_file}")

    # Check file existence
    if not os.path.isfile(cfg_file):
        raise ConfigurationError(f"[ERROR] File {cfg_file} does not exist")
    return cfg_file


def _check_common_vars(cfg_vars, config):
    # Load comm variables
    if "comm" not in cfg_vars.keys():
        raise ConfigurationError(
            "[ERROR] CFG file does not define comm variable under DEFAULT scope"
        )
    comm_adaptor = cfg_vars["comm"]
    if comm_adaptor not in config.sections():
        raise ConfigurationError(
            f"[ERROR] CFG file does not define adaptor scope {comm_adaptor}"
        )
    comm_adaptor_vars = {k: v for k, v in config.items(comm_adaptor)}
    cfg_vars.update(comm_adaptor_vars)
    # Fix execution environments (if any)
    if "execution_envs" in cfg_vars.keys():
        cfg_vars["execution_envs"] = cfg_vars["execution_envs"].strip().split(",")
