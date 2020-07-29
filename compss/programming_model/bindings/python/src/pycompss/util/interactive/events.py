#!/usr/bin/python
#
#  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Util - Interactive Events
==================================
    Provides the event handlers for the cell execution and registers the
    callbacks.
"""

import sys
from pycompss.util.interactive.outwatcher import STDW
import pycompss.util.context as context


#######################################
#           EVENT CALLBACKS           #
#######################################

def __pre_execute__():
    # type: () -> None
    """Fires prior to interactive execution.

    :return: None
    """
    print("pre_execute")


def __pre_run_cell__():
    # type: () -> None
    """ Like pre_run_cell, but is triggered prior to any execution.

    Sometimes code can be executed by libraries, etc. which skipping the
    history/display mechanisms, in which cases pre_run_cell will not fire.

    :return: None
    """
    print("pre_run_cell")


def __post_execute__():
    # type: () -> None
    """ Runs after interactive execution (e.g. a cell in a notebook).

    :return: None
    """
    print("post_execute")


def __post_run_cell__():
    # type: () -> None
    """ The same as pre_execute, post_execute is like post_run_cell, but
    fires for all executions, not just interactive ones.

    Notifies if any exception or task has been cancelled to the user.

    :return: None
    """
    messages = STDW.get_messages()
    if messages:
        for message in messages:
            sys.stderr.write("".join((message, '\n')))
            if message == '[ERRMGR]  -  Shutting down COMPSs...':
                # A critical error occurred --> notify that COMPSs runtime
                # stopped working to avoid issues when running any PyCOMPSs
                # function.
                context.set_pycompss_context(context.OUT_OF_SCOPE)


#######################################
#     EVENT MANAGEMENT FUNCTIONS      #
#######################################

def setup_event_manager(ipython):
    # type: (...) -> None
    """ Instantiates an Ipython event manager and registers the event handlers.

    :param ipython: IPython instance where to register the event manager.
    :return: None
    """
    # ipython.events.register('pre_execute', __pre_execute__)    # Not used
    # ipython.events.register('pre_run_cell', __pre_run_cell__)  # Not used
    # ipython.events.register('post_execute', __post_execute__)  # Not used
    ipython.events.register('post_run_cell', __post_run_cell__)  # Used


def release_event_manager(ipython):
    # type: (...) -> None
    """ Releases the event manager in the given ipython instance.

    :param ipython: IPython instance where to release the event manager.
    :return: None
    """
    try:
        # ipython.events.unregister('pre_execute', __pre_execute__)    # Not used
        # ipython.events.unregister('pre_run_cell', __pre_run_cell__)  # Not used
        # ipython.events.unregister('post_execute', __post_execute__)  # Not used
        ipython.events.unregister('post_run_cell', __post_run_cell__)  # Used
    except ValueError:
        # The event was already unregistered
        pass
