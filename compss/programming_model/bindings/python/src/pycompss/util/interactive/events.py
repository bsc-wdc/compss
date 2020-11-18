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
    found_errors = False
    runtime_crashed = False
    if messages:
        for message in messages:
            if message.startswith("[ERRMGR]"):
                found_errors = True
                # Errors found, but maybe not critical, like for example
                # tasks that failed but recovered.
            if message == "[ERRMGR]  -  Shutting down COMPSs...":
                # A critical error occurred --> notify that COMPSs runtime
                # stopped working to avoid issues when running any PyCOMPSs
                # function.
                runtime_crashed = True
        if runtime_crashed:
            # Display popup with the error messages
            header = ["<h4><u><strong>OUTPUT MESSAGES:</u></strong></h4>"]
            footer = ["",
                      "<h4><u><strong>IMPORTANT!</strong></u></h4>",
                      "<strong>The execution will continue without COMPSs.</strong>"]  # noqa: E501
            popup_body = header + messages + footer
            error_messages_html = "<p>" + "<br>".join(popup_body) + "</p>"
            error_messages_html = error_messages_html.replace("'", "")
            popup_title_html = "ERROR: COMPSs Runtime Crashed!"
            popup_code = """require(["base/js/dialog"],
                                    function(dialog) OPENBRACKET
                                        dialog.modal(OPENBRACKET
                                            title: '{0}',
                                            body: $('{1}'),
                                            buttons: OPENBRACKET
                                                'Ok': OPENBRACKET CLOSEBRACKET
                                            CLOSEBRACKET
                                        CLOSEBRACKET);
                                    CLOSEBRACKET
                            );""".format(popup_title_html, error_messages_html)
            popup_js = popup_code.replace("OPENBRACKET", '{').replace("CLOSEBRACKET", '}')  # noqa: E501
            popup = Javascript(popup_js)
            display(popup)  # noqa
            context.set_pycompss_context(context.OUT_OF_SCOPE)
        elif found_errors:
            # Display popup with the warning messages
            header = ["<h4><u><strong>OUTPUT MESSAGES:</u></strong></h4>", ""]
            footer = ["",
                      "<h4><u><strong>IMPORTANT!</strong></u></h4>",
                      "<strong>This is just a warning. The tasks that failed have been recovered.</strong>"]  # noqa: E501
            popup_body = header + messages + footer
            error_messages_html = "<p>" + "<br>".join(popup_body) + "</p>"
            error_messages_html = error_messages_html.replace("'", "")
            popup_title_html = "WARNING: some tasks may have failed!"
            popup_code = """require(["base/js/dialog"],
                                    function(dialog) OPENBRACKET
                                        dialog.modal(OPENBRACKET
                                            title: '{0}',
                                            body: $('{1}'),
                                            buttons: OPENBRACKET
                                                'Ok': OPENBRACKET CLOSEBRACKET
                                            CLOSEBRACKET
                                        CLOSEBRACKET);
                                    CLOSEBRACKET
                            );""".format(popup_title_html, error_messages_html)
            popup_js = popup_code.replace("OPENBRACKET", '{').replace("CLOSEBRACKET", '}')  # noqa: E501
            popup = Javascript(popup_js)
            display(popup)  # noqa
        else:
            # No issue
            pass


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
