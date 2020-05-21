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
PyCOMPSs Utils - Std redirects
================================
    This file contains the methods required to redirect the standard output
    and standard error grabbing all kind of output and error (e.g. from C
    or child processes).
"""

from contextlib import contextmanager
import ctypes
import io
import os
import sys

from pycompss.runtime.commons import IS_PYTHON3

libc = ctypes.CDLL(None)
c_stdout = ctypes.c_void_p.in_dll(libc, 'stdout')
c_stderr = ctypes.c_void_p.in_dll(libc, 'stderr')


@contextmanager
def std_redirector(out_filename, err_filename):
    """
    Stdout and stderr redirector to the given out and err filenames.

    :param out_filename: Output file filename (where to redirect stdout)
    :param err_filename: Error output file filename (where to redirect stderr)
    :return: Generator
    """

    stdout_fd = sys.stdout.fileno()
    stderr_fd = sys.stderr.fileno()

    def _redirect_stdout(to_fd):
        """
        Redirect stdout to the given file descriptor.

        :param to_fd: Destination file descriptor
        :return: None
        """
        # Flush the C-level buffer stdout
        libc.fflush(c_stdout)
        # Flush and close sys.stdout (also closes the file descriptor)
        sys.stdout.close()
        # Make stdout_fd point to_fd
        os.dup2(to_fd, stdout_fd)
        # Create a new sys.stdout that points to the redirected fd
        if IS_PYTHON3:
            sys.stdout = io.TextIOWrapper(os.fdopen(stdout_fd, 'wb'))
        else:
            sys.stdout = os.fdopen(stdout_fd, 'w')

    def _redirect_stderr(to_fd):
        """
        Redirect stderr to the given file descriptor.

        :param to_fd: Destination file descriptor
        :return: None
        """
        # Flush the C-level buffer stderr
        libc.fflush(c_stderr)
        # Flush and close sys.stderr (also closes the file descriptor)
        sys.stderr.close()
        # Make stderr_fd point to_fd
        os.dup2(to_fd, stderr_fd)
        # Create a new sys.stderr that points to the redirected fd
        if IS_PYTHON3:
            sys.stderr = io.TextIOWrapper(os.fdopen(stderr_fd, 'wb'))
        else:
            sys.stderr = os.fdopen(stderr_fd, 'w')

    # Save a copy of the original stdout and stderr
    stdout_fd_backup = os.dup(stdout_fd)
    stderr_fd_backup = os.dup(stderr_fd)
    try:
        f_out = open(out_filename, 'ab')
        _redirect_stdout(f_out.fileno())
        f_err = open(err_filename, 'ab')
        _redirect_stderr(f_err.fileno())
    except Exception as e:
        raise e
    # Yield to caller
    yield
    try:
        # Then redirect stdout and stderr back to the backup file descriptors
        _redirect_stdout(stdout_fd_backup)
        f_out.flush()
        _redirect_stderr(stderr_fd_backup)
        f_err.flush()
    except Exception as e:
        raise e
    finally:
        f_out.close()
        os.close(stdout_fd_backup)
        f_err.close()
        os.close(stderr_fd_backup)
