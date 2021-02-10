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
import os

from .common import *  # noqa

exacute_backend = os.environ.get("EXAQUTE_BACKEND")
print("EXAQUTE_BACKEND=", exacute_backend)

if exacute_backend:
    exacute_backend = exacute_backend.lower()

if not exacute_backend:
    print("ExaQUte backend: Local")
    from .local import *  # noqa
elif exacute_backend == "hyperloom":
    from .hyperloom import *  # noqa
elif exacute_backend == "pycompss":
    from .pycompss import *
else:
    raise Exception("Unknown exaqute backend: {}".format(exacute_backend))
