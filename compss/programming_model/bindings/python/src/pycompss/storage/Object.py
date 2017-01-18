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
         Considers the persitence within the /tmp folder of the localhost.
'''
import os
import uuid
from pycompss.util.serializer import serialize_to_file

storage_path = '/tmp/'

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

    def makePersistent(self):
        if self.id is None:
            uid = uuid.uuid4()
            self.id = str(uid)
            file_name = str(uid) + '.PSCO'
            file_path = storage_path + file_name

            # Serialize object and write to disk
            serialize_to_file(self, file_path)

    def delete(self):
        if self.id is None:
            # Remove file from /tmp
            file_name = str(self.id) + '.PSCO'
            file_path = storage_path + file_name
            try:
                os.remove(file_path)
            except:
                print "PSCO: " + file_path + " Does not exist!"

