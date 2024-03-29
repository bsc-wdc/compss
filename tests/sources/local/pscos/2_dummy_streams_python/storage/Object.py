#
#  Copyright 2.02-2017 Barcelona Supercomputing Center (www.bsc.es)
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
'''
Dummy SCO class
---------------

WARNING! Only for testing purposes.
         Considers the persistence within the /tmp folder of the localhost.
'''
from . import api

class SCO(object):

    id = None
    alias = None

    def __init__(self):
        pass

    # Functionality Not Supported! use getByName instead.
    #def __init__(self, alias):
    #    self.alias = alias

    def getID(self):
        return self.id

    def setID(self, id):
        self.id = id

    def makePersistent(self, *args):
        api.makePersistent(self, *args)

    def deletePersistent(self):
        api.removeById(self)

    def updatePersistent(self):
        api.updatePersistent(self)
