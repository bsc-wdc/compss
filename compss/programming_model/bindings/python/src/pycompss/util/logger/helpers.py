#!/usr/bin/env python3
#
#  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
import logging.config
import os
from pathlib import Path
from contextlib import contextmanager
from typing import Dict, Iterator, Optional

from pycompss import PYCOMPSS_HOME
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.logger.level import check_log_level
from pycompss.util.logger.level import get_log_level
from pycompss.util.logger.remittent import LOG_REMITTENT

LOG_CFG_PATH = os.path.join(PYCOMPSS_HOME, "util", "logger", "cfg")

# Configurations cache
CONFIGS: Dict[str, dict] = {}

# =========================================================
# CONFIG LOADING
# =========================================================


def clean_log_configs() -> None:
    """Clear cached logging configurations."""
    CONFIGS.clear()


def __get_logging_cfg_file(remittent: str) -> str:
    """
    Get path of logging configuration file.

    :param remittent: logging context (worker/master/etc.)
    :return: file path
    """
    path = os.path.join(LOG_CFG_PATH, f"logging_{remittent}.json")
    if not os.path.isfile(path):
        raise PyCOMPSsException(f"Missing logging config: {path}")
    return path


def __read_log_config_file(remittent: str, log_level: str) -> dict:
    """
    Load and prepare logging configuration.

    :param remittent: logging type
    :param log_level: log level string
    :return: dict config
    """
    cfg_file = __get_logging_cfg_file(remittent)

    if cfg_file not in CONFIGS:
        with open(cfg_file, "rt") as f:
            CONFIGS[cfg_file] = json.load(f)

    conf = CONFIGS[cfg_file]

    # Translate the user level (e.g. api, trace) to the real log level tag
    log_level = get_log_level(log_level)
    check_log_level(log_level)

    log_level = log_level.upper()
    if log_level == "OFF":
        log_level = "ERROR"

    conf = __add_loggers(conf, remittent, log_level)
    conf = __update_log_level(conf, log_level)

    return conf


# =========================================================
# INIT LOGGING (GLOBAL ONCE PER PROCESS)
# =========================================================


def init_logging(remittent: str, log_level: str, log_path: str) -> None:
    """
    Initialize logging for master/worker.

    :param remittent: logging context
    :param log_level: logging level
    :param log_path: base log directory
    """
    conf = __read_log_config_file(remittent, log_level)
    _prefix_handlers(conf, log_path)
    logging.config.dictConfig(conf)


def init_logging_worker(remittent: str, log_level: str, tracing: bool) -> None:
    """
    Initialize worker logging.

    :param remittent: worker type
    :param log_level: log level
    :param tracing: enable tracing mode
    """
    conf = __read_log_config_file(remittent, log_level)

    if tracing:
        _prefix_tracing(conf)

    logging.config.dictConfig(conf)


def init_logging_worker_piper(
    remittent: str, log_level: str, log_dir: str
) -> None:
    """
    Initialize piper worker logging.

    :param remittent: worker type
    :param log_level: log level
    :param log_dir: log directory
    """
    conf = __read_log_config_file(remittent, log_level)
    _prefix_handlers(conf, log_dir)
    logging.config.dictConfig(conf)


# def init_logging_agent(log_level: str, log_dir: str) -> None:
#     """
#     Initialize agent logging.
#
#     :param log_level: log level
#     :param log_dir: log directory
#     """
#     conf = __read_log_config_file("agent", log_level)
#     _prefix_handlers(conf, log_dir)
#     logging.config.dictConfig(conf)


# =========================================================
# CONFIG HELPERS
# =========================================================


def _prefix_handlers(conf: dict, log_dir: str) -> None:
    """
    Prefix all file handler filenames with log directory.

    :param conf: Configuration directory.
    :param log_dir: Log file directory.
    """
    for h in conf.get("handlers", {}).values():
        if "filename" in h:
            h["filename"] = os.path.join(log_dir, h["filename"])


def _prefix_tracing(conf: dict) -> None:
    """
    Adjust paths for tracing mode.

    :param conf: Configuration directory.
    """
    for h in conf.get("handlers", {}).values():
        if "filename" in h:
            h["filename"] = "../" + h["filename"]


# =========================================================
# LOG CONFIG BUILDING
# =========================================================


def __add_loggers(conf: dict, remittent: str, log_level: str) -> dict:
    """
    Inject loggers dynamically discovered from source tree.

    :param conf: Configuration directory.
    :param remittent: Logging message remittent.
    :param log_level: Logging level.
    """
    loggers = []

    for source_file in __find_source_files(PYCOMPSS_HOME, ["py"]):
        loggers.append(source_file.replace("/", "."))

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
                "dp_file_handler",
            ]
        else:
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
                "dp_file_handler",
            ]
        else:
            handlers = ["error_worker_file_handler"]

    elif remittent in (
        LOG_REMITTENT.MPI_WORKER,
        LOG_REMITTENT.CONTAINER_WORKER,
    ):
        handlers = ["console", "error_console"]

    else:
        raise PyCOMPSsException(f"Unexpected remittent: {remittent}")

    for logger in loggers:
        conf["loggers"][logger] = {
            "level": log_level,
            "handlers": handlers,
            "propagate": False,
        }

    return conf


def __update_log_level(conf: dict, log_level: str) -> dict:
    """
    Update root logging level.

    :param conf: Configuration directory.
    :param log_level: Logging level.
    """
    conf["root"]["level"] = log_level
    conf["handlers"]["console"]["level"] = log_level
    return conf


# =========================================================
# JOB LOGGING (No leaks, task safe)
# =========================================================

_JOB_HANDLERS: list[logging.Handler] = []


def swap_job_logs(
    job_out: Optional[str] = None, job_err: Optional[str] = None
) -> None:
    """
    Attach per-job file handlers safely.

    This DOES NOT modify existing handlers.
    It guarantees isolation between tasks.

    :param job_out: stdout log file
    :param job_err: stderr log file
    """
    logger = logging.getLogger()

    restore_job_logs()  # prevent leaks

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s - %(message)s"
    )

    def _add(path: str, level: int) -> logging.Handler:
        handler = logging.FileHandler(path)
        handler.setLevel(level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        _JOB_HANDLERS.append(handler)
        return handler

    if job_out:
        _add(job_out, logging.INFO)

    if job_err:
        _add(job_err, logging.ERROR)


def restore_job_logs() -> None:
    """
    Remove all job-specific handlers safely.

    Ensures no cross-task leakage.
    """
    global _JOB_HANDLERS

    logger = logging.getLogger()

    for h in _JOB_HANDLERS:
        logger.removeHandler(h)
        h.close()

    _JOB_HANDLERS.clear()


# =========================================================
# UTILS
# =========================================================


def __find_source_files(path: str, extensions: list[str]):
    """
    Recursively find source files.

    :param path: root path
    :param extensions: file extensions
    """
    main_home = os.path.dirname(path)

    if not main_home.endswith("/"):
        main_home += "/"

    for root, _, files in os.walk(path):
        for file in files:
            if "." in file and file.split(".")[1] in extensions:
                relative = root.replace(main_home, "")
                file_path = os.path.join(relative, file)
                yield str(Path(file_path).with_suffix(""))


# =========================================================
# LOG EXTENSIONS
# =========================================================


def add_new_logger(logger_name: str, remittent: str) -> None:
    """Add a new logger dynamically based on "user" template."""
    cfg_file = __get_logging_cfg_file(remittent)
    conf = CONFIGS.get(cfg_file)

    if not conf:
        raise RuntimeError("Logging not initialized")

    base = conf["loggers"]["user"]
    conf["loggers"][logger_name] = copy.deepcopy(base)

    logging.config.dictConfig(conf)


# =========================================================
# CONTEXT HELPERS
# =========================================================


@contextmanager
def swap_logger_name(logger: logging.Logger, new_name: str) -> Iterator[None]:
    """Temporarily rename a logger."""
    old = logger.name
    logger.name = new_name
    yield
    logger.name = old


@contextmanager
def keep_logger() -> Iterator[None]:
    """No-op context manager."""
    yield  # here the code runs


def rotate_log(log_path: Path):
    """Rename the log path adding an incremental numerical suffix each time.

    :param log_path: Path to the dir to be rotated.

    Ejemplo:
        >>> rotate_log(Path("/path/to/1234"))
        # If exists, move to /path/to/1234_1
        # If invoked again with /path/to/1234, it moves to /path/to/1234_2
        # And so on.
    """
    if log_path.exists():
        n = 1
        while True:
            new_path = log_path.parent / f"{log_path.name}_{n}"
            if not new_path.exists():
                break
            n += 1
        log_path.rename(new_path)
