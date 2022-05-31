#!/usr/bin/python
#
#  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
import sys
import logging
import subprocess

DECODING_FORMAT = 'utf-8'


def command_runner(cmd):
    """ Run the command defined in the cmd list.

    :param cmd: Command to execute as list (list[str]).
    :returns: Exit code
    :raises Exception: Exit code != 0
    """
    print("Executing: %s" % " ".join(cmd))
    p = subprocess.Popen(cmd,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()   # blocks until cmd is done
    stdout = stdout.decode(DECODING_FORMAT)
    stderr = stderr.decode(DECODING_FORMAT)
    return_code = p.returncode
    print("Exit code: %s" % str(return_code))
    print("------------ STDOUT ------------",
          flush=True)
    print(stdout, flush=True)
    if stderr:
        print("------------ STDERR ------------",
              file=sys.stderr, flush=True)
        print(stderr, file=sys.stderr, flush=True)
    if return_code != 0:
        print("Exit code: %s != 0" % str(return_code),
              file=sys.stderr, flush=True)
    exit(return_code)
