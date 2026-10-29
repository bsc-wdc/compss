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

import pytest


def test_get_log_level():
    from pycompss.util.logger.level import get_log_level

    expected = {
        "off": "off",
        "info": "info",
        "api": "off",
        "debug": "debug",
        "trace": "debug",
    }
    for level, tag in expected.items():
        assert get_log_level(level) == tag, "ERROR: Wrong tag for " + level
        assert get_log_level(level.upper()) == tag, (
            "ERROR: Wrong tag for " + level
        )


def test_read_log_config_file_all_levels():
    # All the levels accepted by runcompss --log_level must be supported
    from pycompss.util.logger.helpers import __read_log_config_file
    from pycompss.util.logger.remittent import LOG_REMITTENT

    for level in ["off", "info", "api", "debug", "trace"]:
        __read_log_config_file(LOG_REMITTENT.MASTER, level)


def test_read_log_config_file_unsupported_level():
    from pycompss.util.exceptions import PyCOMPSsException
    from pycompss.util.logger.helpers import __read_log_config_file
    from pycompss.util.logger.remittent import LOG_REMITTENT

    with pytest.raises(PyCOMPSsException):
        __read_log_config_file(LOG_REMITTENT.MASTER, "verbose")
