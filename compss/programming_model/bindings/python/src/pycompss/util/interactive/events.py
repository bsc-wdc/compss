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

# IPython imports
from IPython.display import display
from IPython.display import Javascript


#######################################
#           EVENT CALLBACKS           #
#######################################

def __pre_execute__():
    # type: () -> None
    """ Fires prior to interactive execution.

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
    messages = STDW.get_messages()
    runtime_crashed = False
    if messages:
        for message in messages:
            if message == '[ERRMGR]  -  Shutting down COMPSs...':
                # A critical error occurred --> notify that COMPSs runtime
                # stopped working to avoid issues when running any PyCOMPSs
                # function.
                runtime_crashed = True
        if runtime_crashed:
            error_messages = "\n".join(messages)
            # Display popup with the error messages
            display((Javascript("""
            require(
                ["base/js/dialog"],
                function(dialog) {
                    dialog.modal({
                        title: '%s',
                        body: '%s',
                        buttons: {
                            'Ok': {}
                        }
                    });
                }
            );
            """ % ("COMPSs Runtime Crashed!",
                   error_messages)),))
            context.set_pycompss_context(context.OUT_OF_SCOPE)
    # if messages:
    #     for message in messages:
    #         sys.stderr.write("".join((message, '\n')))
    #         if message == '[ERRMGR]  -  Shutting down COMPSs...':
    #             # A critical error occurred --> notify that COMPSs runtime
    #             # stopped working to avoid issues when running any PyCOMPSs
    #             # function.
    #             context.set_pycompss_context(context.OUT_OF_SCOPE)


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
    print("post_run_cell")


#######################################
#     EVENT MANAGEMENT FUNCTIONS      #
#######################################

def setup_event_manager(ipython):
    # type: (...) -> None
    """ Instantiates an Ipython event manager and registers the event handlers.

    :param ipython: IPython instance where to register the event manager.
    :return: None
    """
    # ipython.events.register('pre_execute', __pre_execute__)
    ipython.events.register('pre_run_cell', __pre_run_cell__)
    # ipython.events.register('post_execute', __post_execute__)
    # ipython.events.register('post_run_cell', __post_run_cell__)


def release_event_manager(ipython):
    # type: (...) -> None
    """ Releases the event manager in the given ipython instance.

    :param ipython: IPython instance where to release the event manager.
    :return: None
    """
    try:
        # ipython.events.unregister('pre_execute', __pre_execute__)
        ipython.events.unregister('pre_run_cell', __pre_run_cell__)
        # ipython.events.unregister('post_execute', __post_execute__)
        # ipython.events.unregister('post_run_cell', __post_run_cell__)
    except ValueError:
        # The event was already unregistered
        pass
