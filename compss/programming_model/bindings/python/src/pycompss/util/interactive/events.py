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
PyCOMPSs Util - Interactive - Events.

Provides the event handlers for the cell execution and registers the callbacks.
"""

import os

from pycompss.util.interactive.outwatcher import STDW
from pycompss.util.typing_helper import typing

try:
    # IPython imports
    from IPython.display import display
    from IPython.display import Javascript
except ImportError:
    display = None
    Javascript = None

# Placeholder for post run cell messages
POST_MESSAGE = None


#######################################
#           EVENT CALLBACKS           #
#######################################


def __pre_execute__() -> None:
    """Fire prior to interactive execution.

    :return: None.
    """
    print("pre_execute")


def __pre_run_cell__() -> None:
    """Like pre_run_cell, but is triggered prior to any execution.

    Sometimes code can be executed by libraries, etc. which skipping the
    history/display mechanisms, in which cases pre_run_cell will not fire.

    :return: None.
    """
    global POST_MESSAGE
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
            current_flags = str(os.environ["PYCOMPSS_CURRENT_FLAGS"])
            header = []  # type: typing.List[str]
            footer = []  # type: typing.List[str]
            popup_body = header + messages + footer
            error_messages_html = "<p>" + "<br>".join(popup_body) + "</p>"
            error_messages_html = error_messages_html.replace("'", "")
            popup_title_html = "COMPSs RUNTIME STOPPED"
            # fmt: off
            popup_code = """require(["base/js/dialog"],
                                    function(dialog) OPENBRACKET
                                        function restartCOMPSs()OPENBRACKET
                                            var kernel = IPython.notebook.kernel;
                                            kernel.execute("import base64; import json; from pycompss.interactive import stop, start; stop(_hard_stop=True); _COMPSS_START_FLAGS=json.loads(base64.b64decode('" + '{2}' + "'.encode())); start(**_COMPSS_START_FLAGS)");
                                        CLOSEBRACKET
                                        function continueWithoutCOMPSs()OPENBRACKET
                                            var kernel = IPython.notebook.kernel;
                                            kernel.execute("from pycompss.interactive import stop; stop(_hard_stop=True)");
                                        CLOSEBRACKET
                                        dialog.modal(OPENBRACKET
                                            title: '{0}',
                                            body: $('{1}'),
                                            buttons: OPENBRACKET
                                                'Continue without COMPSs': OPENBRACKET
                                                                             click: function() OPENBRACKET
                                                                                 continueWithoutCOMPSs();
                                                                             CLOSEBRACKET
                                                                           CLOSEBRACKET,
                                                'Restart COMPSs': OPENBRACKET
                                                                    class: 'btn-primary',
                                                                    click: function() OPENBRACKET
                                                                        restartCOMPSs();
                                                                    CLOSEBRACKET
                                                                  CLOSEBRACKET
                                            CLOSEBRACKET
                                        CLOSEBRACKET);
                                    CLOSEBRACKET
                            );""".format(popup_title_html, error_messages_html, current_flags)  # noqa: E501
            # fmt: on
            popup_js = popup_code.replace("OPENBRACKET", "{").replace(
                "CLOSEBRACKET", "}"
            )  # noqa: E501
            popup = Javascript(popup_js)
            display(popup)  # noqa
            warn_msg = "WARNING: Some objects may have not been synchronized and need to be recomputed."  # noqa: E501
            POST_MESSAGE = "\x1b[40;43m" + warn_msg + "\x1b[0m"
        elif found_errors:
            # Display popup with the warning messages
            header = []
            footer = []
            popup_body = header + messages + footer
            error_messages_html = "<p>" + "<br>".join(popup_body) + "</p>"
            error_messages_html = error_messages_html.replace("'", "")
            popup_title_html = "WARNING: Some tasks may have failed"
            popup_code = """require(["base/js/dialog"],
                                    function(dialog) OPENBRACKET
                                        dialog.modal(OPENBRACKET
                                            title: '{0}',
                                            body: $('{1}'),
                                            buttons: OPENBRACKET
                                                'Continue': OPENBRACKET CLOSEBRACKET,
                                            CLOSEBRACKET
                                        CLOSEBRACKET);
                                    CLOSEBRACKET
                            );""".format(
                popup_title_html, error_messages_html
            )
            popup_js = popup_code.replace("OPENBRACKET", "{").replace(
                "CLOSEBRACKET", "}"
            )  # noqa: E501
            popup = Javascript(popup_js)
            display(popup)  # noqa
            info_msg = "INFO: The runtime has recovered the failed tasks."
            POST_MESSAGE = "\x1b[40;46m" + info_msg + "\x1b[0m"
        else:
            # No issue
            pass


def __post_execute__() -> None:
    """Run after interactive execution (e.g. a cell in a notebook).

    :return: None.
    """
    print("post_execute")


def __post_run_cell__() -> None:
    """Run for all cells after execution.

    The same as pre_execute, post_execute is like post_run_cell, but
    fires for all executions, not just interactive ones.

    Notifies if any exception or task has been cancelled to the user.

    :return: None.
    """
    global POST_MESSAGE
    if POST_MESSAGE:
        print(POST_MESSAGE)
        POST_MESSAGE = None


#######################################
#     EVENT MANAGEMENT FUNCTIONS      #
#######################################


def setup_event_manager(ipython: typing.Any) -> None:
    """Instantiate an Ipython event manager and registers the event handlers.

    :param ipython: IPython instance where to register the event manager.
    :return: None.
    """
    # ipython.events.register("pre_execute", __pre_execute__)
    ipython.events.register("pre_run_cell", __pre_run_cell__)
    # ipython.events.register("post_execute", __post_execute__)
    ipython.events.register("post_run_cell", __post_run_cell__)


def release_event_manager(ipython: typing.Any) -> None:
    """Releases the event manager in the given ipython instance.

    :param ipython: IPython instance where to release the event manager.
    :return: None.
    """
    try:
        # ipython.events.unregister("pre_execute", __pre_execute__)
        ipython.events.unregister("pre_run_cell", __pre_run_cell__)
        # ipython.events.unregister("post_execute", __post_execute__)
        ipython.events.unregister("post_run_cell", __post_run_cell__)
    except ValueError:
        # The event was already unregistered
        pass
