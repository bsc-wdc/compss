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

import os
import sys
from pycompss.__main__ import main


def check_output(stdout, stderr, error_expected=False):
    if (os.path.exists(stderr) and
            os.path.getsize(stderr) > 0 and
            not error_expected):
        # Non empty file exists
        raise Exception("An error happened. Please check " + stderr)
    else:
        os.remove(stdout)
        os.remove(stderr)


def call_main(main_py, stdout, stderr):
    backup_out = sys.stdout
    backup_err = sys.stderr
    f_out = open(stdout, "a")
    f_err = open(stderr, "a")
    sys.stdout = f_out
    sys.stderr = f_err
    sys.argv = [main_py + "__main__.py"]
    main()
    f_out.close()
    f_err.close()
    sys.stdout = backup_out
    sys.stderr = backup_err


def call_main_run(stdout, stderr):
    backup_out = sys.stdout
    backup_err = sys.stderr
    f_out = open(stdout, "a")
    f_err = open(stderr, "a")
    sys.stdout = f_out
    sys.stderr = f_err
    sys.argv = ["__main__.py", "run"]
    main()
    f_out.close()
    f_err.close()
    sys.stdout = backup_out
    sys.stderr = backup_err


def call_main_enqueue(stdout, stderr):
    backup_out = sys.stdout
    backup_err = sys.stderr
    f_out = open(stdout, "a")
    f_err = open(stderr, "a")
    sys.stdout = f_out
    sys.stderr = f_err
    sys.argv = ["__main__.py", "enqueue"]
    main()
    f_out.close()
    f_err.close()
    sys.stdout = backup_out
    sys.stderr = backup_err


def test_main_script():
    current_path = os.path.dirname(os.path.abspath(__file__))
    stdout = current_path + "/../../../../std.out"
    stderr = current_path + "/../../../../std.err"
    main_py = current_path + "/../../../../src/pycompss/"
    call_main(main_py, stdout, stderr)
    check_output(stdout, stderr)
    # Check that can call runcompss and enqueue_compss, but they will fail
    # since they are not supposed to be available during unit testing.
    call_main_run(stdout, stderr)
    check_output(stdout, stderr, error_expected=True)
    call_main_enqueue(stdout, stderr)
    check_output(stdout, stderr, error_expected=True)
