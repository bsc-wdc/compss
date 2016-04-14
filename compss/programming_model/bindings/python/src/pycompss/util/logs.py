"""
@author: etejedor
@author: fconejer

PyCOMPSs Utils - logs
=====================
    This file contains all logging methods.
"""

import os
import sys
import logging
import json


if sys.version_info >= (2, 7):
    from logging import config
    config_func = config.dictConfig
else:
    import dictconfig
    config_func = dictconfig.dictConfig


def init_logging(log_config_file, storage_path):
    """
    Logging initialization.
    @param log_config_file: Log file name.
    """
    if os.path.exists(log_config_file):
        f = open(log_config_file, 'rt')
        conf = json.loads(f.read())
        errors_file = conf["handlers"]["error_file_handler"].get("filename")
        debug_file = conf["handlers"]["debug_file_handler"].get("filename")
        conf["handlers"]["error_file_handler"]["filename"] = storage_path + errors_file
        conf["handlers"]["debug_file_handler"]["filename"] = storage_path + debug_file
        config_func(conf)
    else:
        logging.basicConfig(level=logging.INFO)


def init_logging_worker(log_config_file):
    """
    Worker logging initialization.
    @param log_config_file: Log file name.
    """
    if os.path.exists(log_config_file):
        f = open(log_config_file, 'rt')
        conf = json.loads(f.read())
        config_func(conf)
    else:
        logging.basicConfig(level=logging.INFO)
