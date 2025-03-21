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

from pycompss.util.context import CONTEXT

PRE_CONTEXT_ERROR = "ERROR: The context was in pycompss (OUT_OF_SCOPE)."
MASTER_CONTEXT_ERROR = "ERROR: The context was not in pycompss (MASTER)."
WORKER_CONTEXT_ERROR = "ERROR: The context was not in pycompss (WORKER)."


def test_inmaster_context():
    CONTEXT.set_master()
    master_context = CONTEXT.in_master()
    assert master_context is True, MASTER_CONTEXT_ERROR
    CONTEXT.set_out_of_scope()


def test_inworker_context():
    CONTEXT.set_worker()
    worker_context = CONTEXT.in_worker()
    assert worker_context is True, WORKER_CONTEXT_ERROR
    CONTEXT.set_out_of_scope()


def test_in_pycompss_context():
    CONTEXT.set_master()
    master_context = CONTEXT.in_pycompss()
    CONTEXT.set_worker()
    worker_context = CONTEXT.in_pycompss()
    assert master_context is True, MASTER_CONTEXT_ERROR
    assert worker_context is True, WORKER_CONTEXT_ERROR
    CONTEXT.set_out_of_scope()


def test_who_contextualized():
    CONTEXT.set_master()
    who = CONTEXT.get_who_contextualized()
    assert (
        __name__ in who
        or "None" in who
        or "_callers"  # callers when using mypy
    ), "ERROR: Wrong who (%s) contextualized." % str(who)
    CONTEXT.set_out_of_scope()


def test_get_context():
    CONTEXT.set_out_of_scope()
    pre_context = CONTEXT.get_pycompss_context()
    CONTEXT.set_master()
    master_context = CONTEXT.get_pycompss_context()
    CONTEXT.set_worker()
    worker_context = CONTEXT.get_pycompss_context()
    assert (
        pre_context == CONTEXT.out_of_scope
    ), "ERROR: The context was not OUT_OF_SCOPE before setting"  # noqa: E501
    assert (
        master_context == CONTEXT.master
    ), "ERROR: The context was not in MASTER."
    assert (
        worker_context == CONTEXT.worker
    ), "ERROR: The context was not in WORKER."
    CONTEXT.set_out_of_scope()


def test_enable_nesting():
    not_enabled = CONTEXT.is_nesting_enabled()
    CONTEXT.enable_nesting()
    is_enabled = CONTEXT.is_nesting_enabled()
    CONTEXT.disable_nesting()
    is_disabled = CONTEXT.is_nesting_enabled()
    assert (
        not_enabled is False
    ), "ERROR: Nesting must not be enabled by default."
    assert is_enabled is True, "ERROR: Nesting has not been enabled."
    assert is_disabled is False, "ERROR: Nesting has not been disabled."
