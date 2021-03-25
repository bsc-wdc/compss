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
Dummy SCO class
---------------

WARNING! Only for testing purposes.
         Considers the persistence within the /tmp folder of the localhost.
"""

from pycompss.tests.resources.storage import api


class SCO(object):

    id = None
    alias = None

    def __init__(self):
        pass

    # Functionality Not Supported! use getByName instead.
    # def __init__(self, alias):
    #     self.alias = alias

    def get_id(self):
        return self.id

    def set_id(self, id):  # noqa
        self.id = id

    def make_persistent(self, *args):
        api.makePersistent(self, *args)

    def delete_persistent(self):
        api.removeById(self)  # noqa

    def update_persistent(self):
        api.updatePersistent(self)

    # Renaming
    getID = get_id                        # NOSONAR
    setID = set_id                        # NOSONAR
    makePersistent = make_persistent      # NOSONAR
    deletePersistent = delete_persistent  # NOSONAR
    updatePersistent = update_persistent  # NOSONAR
