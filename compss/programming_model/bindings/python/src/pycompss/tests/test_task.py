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

import pycompss.interactive as ipycompss


def test_increment():
    ipycompss.start(debug=True)
    from pycompss.api.api import compss_wait_on
    from pycompss.tests.resources.tasks import increment
    initial = 1
    result = increment(initial)
    result = compss_wait_on(result)
    ipycompss.stop(sync=False)
    assert result == 2, "Wrong increment result obtained."

    # TODO: terminar este test permitiendo que se pueda hacer start y stop sin interactivo
