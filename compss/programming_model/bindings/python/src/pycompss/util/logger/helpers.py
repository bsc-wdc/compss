#!/usr/bin/python
#
#  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Util - logs
====================
    This file contains all logging methods.
"""

import os
import logging
import json
from pycompss.util.typing_helper import typing
from contextlib import contextmanager
from logging import config
from pycompss.util.exceptions import PyCOMPSsException


CONFIG_FUNC = config.dictConfig
# Keep configs to avoid read the cfg many times
CONFIGS = dict()  # type: typing.Dict[str, dict]


def get_logging_cfg_file(log_level):
    # type: (str) -> str
    """ Retrieves the logging configuration file.

    :param log_level: Log level [ "trace"|"debug"|"info"|"api"|"off" ].
    :return: Logging configuration file.
    :raise PyCOMPSsException: Unsupported log level.
    """
    cfg_files = {
        "trace": "logging_debug.json",  # trace level == debug level
        "debug": "logging_debug.json",
        "info": "logging_info.json",
        "api": "logging_off.json",      # api level == off level
        "off": "logging_off.json"
    }
    if log_level in cfg_files:
        logging_cfg_file = cfg_files[log_level]
        return logging_cfg_file
    else:
        raise PyCOMPSsException("Unsupported logging level.")


def clean_log_configs():
    # type: () -> None
    """ Removes all stored log configurations.

    :return: None
    """
    CONFIGS.clear()


def __read_log_config_file__(log_config_file):
    # type: (str) -> dict
    """ Reads the given config file.
    If already read, retrieves from global dictionary.

    :param log_config_file: Configuration file to read.
    :return: Configuration file content.
    """
    if log_config_file in CONFIGS:
        conf = CONFIGS[log_config_file]
    else:
        with open(log_config_file, 'rt') as lcf_fd:
            conf = json.loads(lcf_fd.read())
        CONFIGS[log_config_file] = conf
    return conf


def init_logging(log_config_file, log_path):
    # type: (str, str) -> None
    """ Master logging initialization.

    :param log_config_file: Log file name.
    :param log_path: Json log files path.
    :return: None
    """
    if os.path.exists(log_config_file):
        conf = __read_log_config_file__(log_config_file)
        handler = "error_file_handler"
        if handler in conf["handlers"]:
            errors_file = conf["handlers"][handler].get("filename")
            conf["handlers"][handler]["filename"] = log_path + errors_file
        handler = "info_file_handler"
        if handler in conf["handlers"]:
            info_file = conf["handlers"][handler].get("filename")
            conf["handlers"][handler]["filename"] = log_path + info_file
        handler = "debug_file_handler"
        if handler in conf["handlers"]:
            debug_file = conf["handlers"][handler].get("filename")
            conf["handlers"][handler]["filename"] = log_path + debug_file
        CONFIG_FUNC(conf)
    else:
        logging.basicConfig(level=logging.INFO)  # NOSONAR


def init_logging_worker(log_config_file, tracing):
    # type: (str, bool) -> None
    """ Worker logging initialization.

    :param log_config_file: Log file name.
    :param tracing: If tracing is enabled (the log dir changes).
    :return: None
    """
    if os.path.exists(log_config_file):
        conf = __read_log_config_file__(log_config_file)
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
        CONFIG_FUNC(conf)
    else:
        logging.basicConfig(level=logging.INFO)  # NOSONAR


def init_logging_worker_piper(log_config_file, log_dir):
    # type: (str, str) -> None
    """ Worker logging initialization.

    :param log_config_file: Log file name.
    :param log_dir: Log directory.
    :return: None
    """
    if os.path.exists(log_config_file):
        conf = __read_log_config_file__(log_config_file)
        handler = "error_worker_file_handler"
        if handler in conf["handlers"]:
            errors_file = conf["handlers"][handler].get("filename")
            conf["handlers"][handler]["filename"] = os.path.join(log_dir, errors_file)
        handler = "info_worker_file_handler"
        if handler in conf["handlers"]:
            info_file = conf["handlers"][handler].get("filename")
            conf["handlers"][handler]["filename"] = os.path.join(log_dir, info_file)
        handler = "debug_worker_file_handler"
        if handler in conf["handlers"]:
            debug_file = conf["handlers"][handler].get("filename")
            conf["handlers"][handler]["filename"] = os.path.join(log_dir, debug_file)
        CONFIG_FUNC(conf)
    else:
        logging.basicConfig(level=logging.INFO)  # NOSONAR


def update_logger_handlers(log_config_file, job_out=None, job_err=None):
    # type: (str, str, str) -> None
    """ Worker logging update.

    :param log_config_file: Log file name.
    :param job_out: out file path.
    :param job_err: err file path.
    :return: None
    """
    if os.path.exists(log_config_file):
        conf = __read_log_config_file__(log_config_file)
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
    else:
        logging.basicConfig(level=logging.INFO)  # NOSONAR


@contextmanager
def swap_logger_name(logger, new_name):
    # type: (typing.Any, str) -> typing.Iterator[None]
    """ Swaps the current logger with the new one

    :param logger: Logger facility.
    :param new_name: Logger name.
    :return: None
    """
    previous_name = logger.name
    logger.name = new_name
    yield  # here the code runs
    logger.name = previous_name


@contextmanager
def keep_logger():
    # type: () -> typing.Iterator[None]
    """ Do nothing with the logger.
    It is used when the swap_logger_name does not need to be applied.

    :return: None
    """
    yield  # here the code runs
