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

"""
PyCOMPSs Util - Interactive - Output watcher.

Keeps track of the stdout/stderr and saves all ERRORs or ISSUEs into a
queue that can be checked with the events or on the runtime closing.
"""

import os
import threading
import time
from queue import Queue

from pycompss.runtime.management.COMPSs import COMPSs
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.typing_helper import typing


class StdWatcher:
    """Standard output and standard error watcher class.

    This class implements the stdout and stderr files watcher for the
    interactive executions.

    To this end, starts a new thread in charge of observing the files
    where the runtime is dumping the stdout and stderr. If detects
    an error or more, pushes those lines into a queue, that can be
    later consulted.
    """

    __slots__ = ["running", "messages"]

    def __init__(self) -> None:
        """Create a new StdWatcher object.

        :returns: None.
        """
        self.running = False
        self.messages = Queue()  # type: Queue[str]

    @staticmethod
    def __watcher__(
        fd_out: typing.TextIO, fd_err: typing.TextIO
    ) -> typing.Iterator[str]:  # pylint: disable=unused-argument
        """Look for new lines in fd_out and fd_err.

        Static method that checks the stderr file descriptor looking
        for new lines added at the end.
        It is enabled to also look into the stdout file descriptor, but
        currently not being used.

        :param fd_out: Standard output file descriptor.
        :param fd_err: Standard error file descriptor.
        :return: Yields each line found in the fd_err.
        """
        while True:
            line = fd_err.readline()
            if line:
                yield line.strip()
            else:
                time.sleep(0.1)

    def __std_follower__(self, out_file_name: str, err_file_name: str) -> None:
        """Look for errors within out_file_name and err_file_name.

        Opens the out and error files and looks inside them thanks to the
        __watcher__ generator. This function puts into the queue any line
        of the error file which starts with "[ERRMGR]".

        :param out_file_name: Output file name.
        :param err_file_name: Error file name.
        :return: None.
        """
        with open(out_file_name, "r") as fd_out:
            with open(err_file_name, "r") as fd_err:
                for line in self.__watcher__(fd_out, fd_err):
                    if self.running:
                        if line.startswith("[ERRMGR]"):
                            self.messages.put(str(line))
                    else:
                        # Stop following std
                        return

    def start_watching(self) -> None:
        """Start a new thread to monitor the stdout and stderr files provided.

        :return: None.
        """
        if COMPSs.is_redirected():
            self.running = True
            out_file_name, err_file_name = COMPSs.get_redirection_file_names()
            thread = threading.Thread(
                target=self.__std_follower__,
                args=(out_file_name, err_file_name),
            )
            thread.start()
        else:
            raise PyCOMPSsException("Can not find the stdout and stderr.")

    def stop_watching(self, clean: bool = True) -> None:
        """Stop the monitoring thread and cleans the redirection files.

        :param clean: Remove the redirection files.
        :return: None.
        """
        self.running = False
        if clean:
            out_file_name, err_file_name = COMPSs.get_redirection_file_names()
            if os.path.exists(out_file_name):
                os.remove(out_file_name)
            if os.path.exists(err_file_name):
                os.remove(err_file_name)

    def get_messages(self) -> list:
        """Get messages found in standard output and standard error.

        Retrieves the current messages stored in the queue as a list
        of strings (one per line reported by the stdout and stderr files).

        :return: A list with the reported messages.
        """
        current_messages = []
        while not self.messages.empty():
            current_messages.append(self.messages.get())
        return current_messages


# Global instance of the STD watcher
STDW = StdWatcher()
