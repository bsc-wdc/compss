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
'''Redis Storage Object implementation for the PyCOMPSs Python Binding
@author: srodrig1
'''
import uuid
#import pycompss.storage.api
import api

class storage_object(object):
    '''Storage Object
    '''

    def __init__(self):
        '''Constructor method
        '''
        # Id will be None until persisted
        self.pycompss_psco_identifier = None

    def makePersistent(self):
        '''Stores the object in the Redis database
        '''
        api.makePersistent(self)

    def make_persistent(self):
        '''Support for underscore notation
        '''
        self.makePersistent()

    def delete(self):
        '''Deletes the object from the Redis database
        '''
        api.deletePersistent(self)

    def getID(self):
        '''Gets the ID of the object
        '''
        return self.pycompss_psco_identifier


'''Add support for camelCase
'''
StorageObject = storage_object
