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
PyCOMPSs Utils - Std - Redirects.

This file contains the methods required to redirect the standard output
and standard error grabbing all kind of output and error (e.g. from C
or child processes).
"""

import ctypes
import fcntl
import io
import os
import sys
from contextlib import contextmanager

from pycompss.util.exceptions import StandardOutputError
from pycompss.util.exceptions import StandardErrorError
from pycompss.util.typing_helper import typing

LIBC = ctypes.CDLL(None)  # noqa
if sys.platform == "darwin":
    C_STDOUT = ctypes.c_void_p.in_dll(LIBC, "__stdoutp")
    C_STDERR = ctypes.c_void_p.in_dll(LIBC, "__stderrp")
else:
    C_STDOUT = ctypes.c_void_p.in_dll(LIBC, "stdout")
    C_STDERR = ctypes.c_void_p.in_dll(LIBC, "stderr")


class FDLocker:
    """File descriptor locker.

    This class implements a context able to lock a file in order to ensure
    that the access to it is done in exclusion.
    """

    def __init__(self, file_name):
        """Initialize a FDLocker object (context).

        :param file_name: File path to lock.
        """
        self.file_name = file_name
        self.file_descriptor = open(file_name, "ab")

    def __enter__(self):
        """Lock the file."""
        if self.file_descriptor.closed:
            self.file_descriptor = open(self.file_name, "ab")
        fcntl.flock(self.file_descriptor.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, _type, value, tb):
        """Flush and unlock the file."""
        self.file_descriptor.flush()
        fcntl.flock(self.file_descriptor.fileno(), fcntl.LOCK_UN)
        self.file_descriptor.close()


def _dup2(to_fd: int, from_fd: int) -> None:
    """Wrap dup2 to do retries if fails.

    dup2 has a race condition in Linux with open, that can result in
    error 16 EBUSY.

    :param to_fd: Destination file descriptor.
    :param from_fd: Source file descriptor.
    :return: None.
    """
    retries = 5
    while retries > 0:
        try:
            os.dup2(to_fd, from_fd)
            return
        except OSError:
            retries = retries - 1


@contextmanager
def not_std_redirector() -> typing.Iterator[None]:
    """Context which does nothing.

    Use this context instead of the std_redirector context to avoid
    stdout and stderr redirection.

    :return: None.
    """
    yield


@contextmanager
def std_redirector(
    out_filename: str, err_filename: str
) -> typing.Iterator[None]:
    """Stdout and stderr redirector to the given out and err file names.

    :param out_filename: Output file filename (where to redirect stdout)
    :param err_filename: Error output file filename (where to redirect stderr)
    :return: Generator
    """
    stdout = sys.stdout
    stderr = sys.stderr
    try:
        stdout_fd = stdout.fileno()
    except ValueError:
        sys.stdout = os.fdopen(1, "w")  # , 0)
        stdout_fd = sys.stdout.fileno()
    try:
        stderr_fd = stderr.fileno()
    except ValueError:
        sys.stderr = os.fdopen(2, "w")  # , 0)
        stderr_fd = sys.stderr.fileno()

    def _redirect_stdout(to_fd: int) -> None:
        """Redirect stdout to the given file descriptor.

        :param to_fd: Destination file descriptor
        :return: None
        """
        # Flush the C-level buffer stdout
        LIBC.fflush(C_STDOUT)
        # Flush and close sys.stdout (also closes the file descriptor)
        sys.stdout.flush()
        sys.stdout.close()
        # Make stdout_fd point to_fd
        _dup2(to_fd, stdout_fd)
        # Create a new sys.stdout that points to the redirected fd
        sys.stdout = io.TextIOWrapper(os.fdopen(stdout_fd, "wb"))

    def _redirect_stderr(to_fd: int) -> None:
        """Redirect stderr to the given file descriptor.

        :param to_fd: Destination file descriptor
        :return: None
        """
        # Flush the C-level buffer stderr
        LIBC.fflush(C_STDERR)
        # Flush and close sys.stderr (also closes the file descriptor)
        sys.stderr.flush()
        sys.stderr.close()
        # Make stderr_fd point to_fd
        _dup2(to_fd, stderr_fd)
        # Create a new sys.stderr that points to the redirected fd
        sys.stderr = io.TextIOWrapper(os.fdopen(stderr_fd, "wb"))

    # Save a copy of the original stdout and stderr
    stdout_fd_backup = os.dup(stdout_fd)
    stderr_fd_backup = os.dup(stderr_fd)

    with FDLocker(out_filename) as f_out:
        _redirect_stdout(f_out.file_descriptor.fileno())
    with FDLocker(err_filename) as f_err:
        _redirect_stderr(f_err.file_descriptor.fileno())

    # Yield to caller
    yield

    # Then redirect stdout and stderr back to the backup file descriptors
    with FDLocker(out_filename) as f_out:
        _redirect_stdout(stdout_fd_backup)
    with FDLocker(err_filename) as f_err:
        _redirect_stderr(stderr_fd_backup)
    # Close file descriptors
    os.close(stdout_fd_backup)
    os.close(stderr_fd_backup)


@contextmanager
def ipython_std_redirector(
    out_filename: str, err_filename: str
) -> typing.Iterator[None]:
    """Redirects stdout and stderr to the given files within ipython envs.

    :param out_filename: Output file filename (where to redirect stdout)
    :param err_filename: Error output file filename (where to redirect stderr)
    :return: Generator
    """
    if sys.__stdout__ is None:
        raise StandardOutputError()
    stdout = sys.__stdout__
    if sys.__stderr__ is None:
        raise StandardErrorError()
    stderr = sys.__stderr__
    try:
        stdout_fd = stdout.fileno()
    except ValueError:
        sys.stdout = os.fdopen(1, "w")  # , 0)
        stdout_fd = sys.stdout.fileno()
    try:
        stderr_fd = stderr.fileno()
    except ValueError:
        sys.stderr = os.fdopen(2, "w")  # , 0)
        stderr_fd = sys.stderr.fileno()

    def _redirect_stdout(to_fd: int) -> None:
        """Redirect stdout to the given file descriptor.

        :param to_fd: Destination file descriptor
        :return: None
        """
        # Flush the C-level buffer stdout
        LIBC.fflush(C_STDOUT)
        # Flush and close sys.__stdout__ (also closes the file descriptor)
        if sys.__stdout__ is None:
            raise StandardOutputError()
        sys.__stdout__.flush()
        sys.__stdout__.close()
        sys.stdout.flush()
        sys.stdout.close()
        # Make stdout_fd point to_fd
        _dup2(to_fd, stdout_fd)
        # Create a new sys.__stdout__ that points to the redirected fd
        new_out = io.TextIOWrapper(os.fdopen(stdout_fd, "wb"))
        sys.__stdout__ = new_out  # type: ignore
        sys.stdout = sys.__stdout__

    def _redirect_stderr(to_fd: int) -> None:
        """Redirect stderr to the given file descriptor.

        :param to_fd: Destination file descriptor
        :return: None
        """
        # Flush the C-level buffer stderr
        LIBC.fflush(C_STDERR)
        # Flush and close sys.__stderr__ (also closes the file descriptor)
        if sys.__stderr__ is None:
            raise StandardErrorError()
        sys.__stderr__.flush()
        sys.__stderr__.close()
        sys.stderr.flush()
        sys.stderr.close()
        # Make stderr_fd point to_fd
        _dup2(to_fd, stderr_fd)
        # Create a new sys.__stderr__ that points to the redirected fd
        new_err = io.TextIOWrapper(os.fdopen(stderr_fd, "wb"))
        sys.__stderr__ = new_err  # type: ignore
        sys.stderr = sys.__stderr__

    # Save a copy of the original stdout and stderr
    stdout_fd_backup = os.dup(stdout_fd)
    stderr_fd_backup = os.dup(stderr_fd)

    with FDLocker(out_filename) as f_out:
        _redirect_stdout(f_out.file_descriptor.fileno())
    with FDLocker(err_filename) as f_err:
        _redirect_stderr(f_err.file_descriptor.fileno())

    # Yield to caller
    yield

    # Then redirect stdout and stderr back to the backup file descriptors
    with FDLocker(out_filename) as f_out:
        _redirect_stdout(stdout_fd_backup)
    with FDLocker(err_filename) as f_err:
        _redirect_stderr(stderr_fd_backup)
    # Close file descriptors
    os.close(stdout_fd_backup)
    os.close(stderr_fd_backup)
