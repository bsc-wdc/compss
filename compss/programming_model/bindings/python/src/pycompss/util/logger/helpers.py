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

# -*- coding: utf-8 -*-

"""
PyCOMPSs Util - Logger - Helpers.

This file contains all logging methods.
"""

import copy
import json
import logging
import os
import pathlib
from contextlib import contextmanager
from logging import config

from pycompss import PYCOMPSS_HOME
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.logger.level import check_log_level
from pycompss.util.logger.remittent import LOG_REMITTENT
from pycompss.util.typing_helper import typing


LOG_CFG_PATH = os.path.join(PYCOMPSS_HOME, "util", "logger", "cfg")
CONFIG_FUNC = config.dictConfig
# Keep configs to avoid read the cfg many times
CONFIGS = {}  # type: typing.Dict[str, dict]


def clean_log_configs() -> None:
    """Remove all stored log configurations.

    :return: None
    """
    CONFIGS.clear()


def __get_logging_cfg_file(remittent: str) -> str:
    """Retrieve the logging configuration file.

    :param remittent: Logging remittent.
    :return: Logging configuration file.
    :raise PyCOMPSsException: Unsupported log remittent.
    """
    log_cfg = os.path.join(LOG_CFG_PATH, f"logging_{remittent}.json")
    if os.path.isfile(log_cfg):
        return log_cfg
    raise PyCOMPSsException(
        f"Logging configuration file {log_cfg} does not exist!"
    )


def __read_log_config_file(
    remittent: str, log_level: str
) -> typing.Dict[str, dict]:
    """Read the required config file and update.

    If already read, retrieves from global dictionary.

    :param remittent: Logging remittent.
    :param log_level: Log level [ "trace"|"debug"|"info"|"api"|"off" ].
    :return: Configuration file content.
    """
    log_config_file = __get_logging_cfg_file(remittent)

    # Get base configuration
    if log_config_file in CONFIGS:
        conf = CONFIGS[log_config_file]
    else:
        with open(log_config_file, "rt") as lcf_fd:
            conf = json.loads(lcf_fd.read())
        CONFIGS[log_config_file] = conf

    # Check if the log level is supported
    check_log_level(log_level)
    # Adapt log level to configuration format and set off log level with error
    log_level = log_level.upper()
    if log_level == "OFF":
        log_level = "ERROR"
    # Add all loggers (from current python files)
    conf = __add_loggers(conf, remittent, log_level)
    # Update configuration with log level
    conf = __update_log_level(conf, log_level)

    return conf


def __find_source_files(path, extensions):
    """Find all binding source files.

    :param path: Folder to look recursively.
    :param extensions: List of extensions considered as source.
    :return: Generator with all source files found.
    """
    main_home = os.path.dirname(path)
    if not main_home.endswith("/"):
        main_home = f"{main_home}/"
    for root, _, files in os.walk(path):
        for file in files:
            if "." in file and file.split(".")[1] in extensions:
                relative = root.replace(main_home, "")
                file_path = os.path.join(relative, file)
                yield str(pathlib.Path(file_path).with_suffix(""))


def __add_loggers(
    conf: typing.Dict[str, dict], remittent: str, log_level: str
) -> typing.Dict[str, dict]:
    """Add all needed loggers to the configuration.

    Traverses the whole binding folder looking for python files and includes
    them in the base configuration.

    :para conf: Configuration file content.
    :param remittent: Logging remittent.
    :param log_level: Log level [ "trace"|"debug"|"info"|"api"|"off" ].
    :return: Updated configuration file content.
    """
    loggers = []
    for source_file in __find_source_files(PYCOMPSS_HOME, ["py"]):
        logger = source_file.replace("/", ".")
        loggers.append(logger)

    if remittent == LOG_REMITTENT.MASTER:
        if log_level == "DEBUG":
            handlers = [
                "debug_master_file_handler",
                "error_master_file_handler",
            ]
        elif log_level == "INFO":
            handlers = [
                "info_master_file_handler",
                "error_master_file_handler",
            ]
        else:
            # ERROR level
            # "debug_master_file_handler" as well?
            handlers = ["error_master_file_handler"]
    elif remittent == LOG_REMITTENT.WORKER:
        if log_level == "DEBUG":
            handlers = [
                "debug_worker_file_handler",
                "error_worker_file_handler",
            ]
        elif log_level == "INFO":
            handlers = [
                "info_worker_file_handler",
                "error_worker_file_handler",
            ]
        else:
            # ERROR level
            # "debug_worker_file_handler" as well?
            handlers = ["error_worker_file_handler"]
    elif remittent == LOG_REMITTENT.GAT_WORKER:
        handlers = ["console", "error_console"]
    elif remittent == LOG_REMITTENT.MPI_WORKER:
        handlers = ["console", "error_console"]
    elif remittent == LOG_REMITTENT.CONTAINER_WORKER:
        handlers = ["console", "error_console"]
    else:
        raise PyCOMPSsException(
            f"Unexpected remittent received updating loggers in config: "
            f"{remittent}"
        )
    for logger in loggers:
        conf["loggers"][logger] = {
            "level": log_level,
            "handlers": handlers,
            "propagate": "no",
        }
    return conf


def __update_log_level(
    conf: typing.Dict[str, dict], log_level: str
) -> typing.Dict[str, dict]:
    """Update the log level in the configuration.

    :para conf: Configuration file content.
    :param log_level: Log level [ "trace"|"debug"|"info"|"api"|"off" ].
    :return: Updated configuration file content.
    """
    conf["root"]["level"] = log_level
    conf["handlers"]["console"]["level"] = log_level
    return conf


def init_logging(remittent: str, log_level: str, log_path: str) -> None:
    """Initialize logging in master.

    :param remittent: Logging remittent.
    :param log_level: Log level [ "trace"|"debug"|"info"|"api"|"off" ].
    :param log_path: Json log files path.
    :return: None.
    """
    conf = __read_log_config_file(remittent, log_level)
    handler = "error_master_file_handler"
    if handler in conf["handlers"]:
        errors_file = conf["handlers"][handler].get("filename")
        conf["handlers"][handler]["filename"] = log_path + errors_file
    handler = "info_master_file_handler"
    if handler in conf["handlers"]:
        info_file = conf["handlers"][handler].get("filename")
        conf["handlers"][handler]["filename"] = log_path + info_file
    handler = "debug_master_file_handler"
    if handler in conf["handlers"]:
        debug_file = conf["handlers"][handler].get("filename")
        conf["handlers"][handler]["filename"] = log_path + debug_file
    CONFIG_FUNC(conf)


def init_logging_worker(
    remittent: str,
    log_level: str,
    tracing: bool,
    job_out: typing.Optional[str] = None,
    job_err: typing.Optional[str] = None,
) -> None:
    """Initialize logger in worker.

    :param remittent: Logging remittent.
    :param log_level: Log level [ "trace"|"debug"|"info"|"api"|"off" ].
    :param tracing: If tracing is enabled (the log dir changes).
    :param job_out: out file path.
    :param job_err: err file path.
    :return: None.
    """
    conf = __read_log_config_file(remittent, log_level)
    if tracing:
        # The workspace is within the folder "workspace/python"
        # Remove the last folder
        handler = "error_worker_file_handler"
        if handler in conf["handlers"]:
            errors_file = conf["handlers"][handler].get("filename")
            conf["handlers"][handler]["filename"] = "../" + errors_file
        handler = "info_worker_file_handler"
        if handler in conf["handlers"]:
            info_file = conf["handlers"][handler].get("filename")
            conf["handlers"][handler]["filename"] = "../" + info_file
        handler = "debug_worker_file_handler"
        if handler in conf["handlers"]:
            debug_file = conf["handlers"][handler].get("filename")
            conf["handlers"][handler]["filename"] = "../" + debug_file
    # If within task
    if job_err:
        handler = "error_worker_file_handler"
        if handler in conf["handlers"]:
            conf["handlers"][handler]["filename"] = job_err
    if job_out:
        handler = "info_worker_file_handler"
        if handler in conf["handlers"]:
            conf["handlers"][handler]["filename"] = job_out
        handler = "debug_worker_file_handler"
        if handler in conf["handlers"]:
            conf["handlers"][handler]["filename"] = job_out

    CONFIG_FUNC(conf)


def init_logging_worker_piper(
    remittent: str, log_level: str, log_dir: str
) -> None:
    """Initialize logger in piper worker.

    :param remittent: Logging remittent.
    :param log_level: Log level [ "trace"|"debug"|"info"|"api"|"off" ].
    :param log_dir: Log directory.
    :return: None.
    """
    conf = __read_log_config_file(remittent, log_level)
    handler = "error_worker_file_handler"
    if handler in conf["handlers"]:
        errors_file = conf["handlers"][handler].get("filename")
        conf["handlers"][handler]["filename"] = os.path.join(
            log_dir, errors_file
        )
    handler = "info_worker_file_handler"
    if handler in conf["handlers"]:
        info_file = conf["handlers"][handler].get("filename")
        conf["handlers"][handler]["filename"] = os.path.join(
            log_dir, info_file
        )
    handler = "debug_worker_file_handler"
    if handler in conf["handlers"]:
        debug_file = conf["handlers"][handler].get("filename")
        conf["handlers"][handler]["filename"] = os.path.join(
            log_dir, debug_file
        )
    CONFIG_FUNC(conf)


def add_new_logger(logger_name: str) -> None:
    """Add a new logger for the user in the master.

    Creates a copy of the "user" or "piper_worker" logger and renames it with
    the given logger_name.

    :param logger_name: New logger name.
    :returns: None
    """
    # Get "user" logger information (used as source)
    log_config_file = list(CONFIGS.keys())[0]
    users_logger = CONFIGS[log_config_file]["loggers"]["user"]
    # Copy "user" logger and set its new name
    new_logger = copy.deepcopy(users_logger)
    CONFIGS[log_config_file]["loggers"][logger_name] = new_logger
    # Update the logger with the new handler
    CONFIG_FUNC(CONFIGS[log_config_file])


@contextmanager
def swap_logger_name(
    logger: logging.Logger, new_name: str
) -> typing.Iterator[None]:
    """Swap the current logger with the new one.

    :param logger: Logger facility.
    :param new_name: Logger name.
    :return: None.
    """
    previous_name = logger.name
    logger.name = new_name
    yield  # here the code runs
    logger.name = previous_name


@contextmanager
def keep_logger() -> typing.Iterator[None]:
    """Do nothing with the logger.

    It is used when the swap_logger_name does not need to be applied.

    :return: None
    """
    yield  # here the code runs
