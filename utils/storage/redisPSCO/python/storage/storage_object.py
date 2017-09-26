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
