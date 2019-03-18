#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import os

from constants import DEFAULT_CFG_EXTENSION
from constants import CONFIGURATIONS_DIR
from constants import DEFAULT_COMPSS_HOME
from constants import DEFAULT_COMM
from constants import DEFAULT_EXECUTION_ENVS


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
    :attribute compss_base_log_dir: Path to the COMPSs base log directory
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

    def __init__(self, user=None, java_home=None, compss_home=DEFAULT_COMPSS_HOME, target_base_dir=None,
                 comm=DEFAULT_COMM, runcompss_opts=None, execution_envs=DEFAULT_EXECUTION_ENVS):
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
            import getpass
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
                    "[ERROR] Undefined variable JAVA_HOME in both the configuration file and the environment")

        # Store COMPSs_HOME (always defined because it has a default value)
        self.compss_home = compss_home

        # Define compss_base_log_dir
        user_home = os.path.expanduser("~")
        self.compss_base_log_dir = os.path.join(user_home, ".COMPSs")

        # Either we receive the target_base_dir or we compute it from user home
        if target_base_dir is None:
            self.target_base_dir = os.path.join(user_home, "tests_execution_sandbox")
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

    def get_compss_base_log_dir(self):
        """
        Returns the path to the COMPSs log base directory

        :return: The path to the COMPSs log base directory
            + type: String
        """
        return self.compss_base_log_dir

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

        return ' '.join(str(x) for x in self.execution_envs)


############################################
# PUBLIC METHODS
############################################

def load_configuration_file(cfg_file):
    """
    Loads the configuration file provided in the cfg_file path

    :param cfg_file: Path to the configuration file (relative or absolute, with or without extension)
    :return: An object representing the COMPSs test configuration options available in the given cfg file
        + type: COMPSsConfiguration
    :raise ConfigurationError: If the provided file path is invalid or there is an error when loading the cfg content
    """
    print()
    print("[INFO] Loading configuration file...")

    # Fix extension and absolute path
    if __debug__:
        print("[DEBUG] Checking cfg_file path: " + str(cfg_file))

    cfg_basename, cfg_extension = os.path.splitext(cfg_file)
    if cfg_extension is None or not cfg_extension:
        if __debug__:
            print("[DEBUG] Adding default extension " + str(DEFAULT_CFG_EXTENSION))
        cfg_extension = DEFAULT_CFG_EXTENSION
        cfg_file = cfg_basename + cfg_extension
    if not os.path.isabs(cfg_file):
        cfg_file = os.path.join(CONFIGURATIONS_DIR, cfg_file)

    if __debug__:
        print("[DEBUG] Complete cfg_file path: " + str(cfg_file))

    # Check file existence
    if not os.path.isfile(cfg_file):
        raise ConfigurationError("[ERROR] File " + str(cfg_file) + " does not exist")

    # Load cfg values
    print("[INFO] Loading values from " + str(cfg_file))
    import configparser
    config = configparser.ConfigParser()
    config.read(cfg_file)
    # Load default variables
    cfg_vars = {k: v for k, v in config.items("DEFAULT")}
    # Load comm variables
    if "comm" not in cfg_vars.keys():
        raise ConfigurationError("[ERROR] CFG file does not define comm variable under DEFAULT scope")
    comm_adaptor = cfg_vars["comm"]
    if comm_adaptor not in config.sections():
        raise ConfigurationError("[ERROR] CFG file does not define adaptor scope " + str(comm_adaptor))
    comm_adaptor_vars = {k: v for k, v in config.items(comm_adaptor)}
    cfg_vars.update(comm_adaptor_vars)
    # Fix execution environments (if any)
    if "execution_envs" in cfg_vars.keys():
        cfg_vars["execution_envs"] = cfg_vars["execution_envs"].strip().split(",")

    if __debug__:
        print("[DEBUG] Retrieved CFG variables: " + str(cfg_vars))

    # Create COMPSs Configuration
    compss_cfg = COMPSsConfiguration(**cfg_vars)

    # Log and return configuration object
    print("[INFO] Configuration file loaded")
    print("[INFO]   - user: " + str(compss_cfg.get_user()))
    print("[INFO]   - java_home: " + str(compss_cfg.get_java_home()))
    print("[INFO]   - compss_home: " + str(compss_cfg.get_compss_home()))
    print("[INFO]   - target_dir: " + str(compss_cfg.get_target_base_dir()))
    print("[INFO]   - comm: " + str(compss_cfg.get_comm()))
    print("[INFO]   - runcompss_opts: " + str(compss_cfg.get_runcompss_opts()))
    print("[INFO]   - execution_envs: " + str(compss_cfg.get_execution_envs()))

    return compss_cfg
