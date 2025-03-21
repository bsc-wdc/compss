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
Dummy SCO class.

WARNING! Only for testing purposes.
         Considers the persistence within the /tmp folder of the localhost.
"""

from pycompss.tests.integration.launch.resources.storage import api


class SCO(object):
    """Self-contained object representative."""

    id = None
    alias = None

    def __init__(self):
        """Do nothing constructor."""
        pass

    # Functionality Not Supported! use getByName instead.
    # def __init__(self, alias):
    #     self.alias = alias

    def get_id(self):
        """Retrieve the object identifier.

        :returns: Object identifier.
        """
        return self.id

    def set_id(self, id):  # noqa
        """Set the object identifier.

        :param id: Object identifier.
        :returns: None.
        """
        self.id = id

    def make_persistent(self, *args):
        """Persist the object.

        :param args: Arguments redirected to api.makePersistent.
        :returns: None.
        """
        api.makePersistent(self, *args)

    def delete_persistent(self):
        """Delete persistent object.

        :returns: None.
        """
        api.removeById(self)  # noqa

    def update_persistent(self):
        """Update persistent object.

        :returns: None.
        """
        api.updatePersistent(self)

    # Renaming
    getID = get_id  # noqa: N815
    setID = set_id  # noqa: N815
    makePersistent = make_persistent  # noqa: N815
    deletePersistent = delete_persistent  # noqa: N815
    updatePersistent = update_persistent  # noqa: N815
