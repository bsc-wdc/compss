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

from pycompss.util.context import MASTER
from pycompss.util.context import OUT_OF_SCOPE
from pycompss.util.context import WORKER

PRE_CONTEXT_ERROR = "ERROR: The context was in pycompss (OUT_OF_SCOPE)."
MASTER_CONTEXT_ERROR = "ERROR: The context was not in pycompss (MASTER)."
WORKER_CONTEXT_ERROR = "ERROR: The context was not in pycompss (WORKER)."


def test_in_master_context():
    from pycompss.util.context import in_master
    from pycompss.util.context import set_pycompss_context

    set_pycompss_context(MASTER)
    master_context = in_master()
    assert master_context is True, MASTER_CONTEXT_ERROR
    set_pycompss_context(OUT_OF_SCOPE)


def test_in_worker_context():
    from pycompss.util.context import in_worker
    from pycompss.util.context import set_pycompss_context

    set_pycompss_context(WORKER)
    worker_context = in_worker()
    assert worker_context is True, WORKER_CONTEXT_ERROR
    set_pycompss_context(OUT_OF_SCOPE)


def test_in_pycompss_context():
    from pycompss.util.context import in_pycompss
    from pycompss.util.context import set_pycompss_context

    set_pycompss_context(MASTER)
    master_context = in_pycompss()
    set_pycompss_context(WORKER)
    worker_context = in_pycompss()
    assert master_context is True, MASTER_CONTEXT_ERROR
    assert worker_context is True, WORKER_CONTEXT_ERROR
    set_pycompss_context(OUT_OF_SCOPE)


def test_who_contextualized():
    from pycompss.util.context import set_pycompss_context
    from pycompss.util.context import get_who_contextualized

    set_pycompss_context(MASTER)
    who = get_who_contextualized()
    assert (
        __name__ in who or "nose.case" in who
    ), "ERROR: Wrong who (%s) contextualized." % str(who)
    set_pycompss_context(OUT_OF_SCOPE)


def test_get_context():
    from pycompss.util.context import set_pycompss_context
    from pycompss.util.context import get_pycompss_context

    set_pycompss_context(OUT_OF_SCOPE)
    pre_context = get_pycompss_context()
    set_pycompss_context(MASTER)
    master_context = get_pycompss_context()
    set_pycompss_context(WORKER)
    worker_context = get_pycompss_context()
    assert (
        pre_context == OUT_OF_SCOPE
    ), "ERROR: The context was not OUT_OF_SCOPE before setting"  # noqa: E501
    assert master_context == MASTER, "ERROR: The context was not in MASTER."
    assert worker_context == WORKER, "ERROR: The context was not in WORKER."
    set_pycompss_context(OUT_OF_SCOPE)


def test_enable_nesting():
    from pycompss.util.context import enable_nesting
    from pycompss.util.context import disable_nesting
    from pycompss.util.context import is_nesting_enabled

    not_enabled = is_nesting_enabled()
    enable_nesting()
    is_enabled = is_nesting_enabled()
    disable_nesting()
    is_disabled = is_nesting_enabled()
    assert (
        not_enabled is False
    ), "ERROR: Nesting must not be enabled by default."  # noqa: E501
    assert is_enabled is True, "ERROR: Nesting has not been enabled."
    assert is_disabled is False, "ERROR: Nesting has not been disabled."
