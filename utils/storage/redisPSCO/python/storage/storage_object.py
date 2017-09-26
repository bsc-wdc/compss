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
    # Id will be None until
    self.pycompss_psco_identifier = None

  #TODO: Why this method has no ID as parameter?
  def makePersistent(self):
    '''Stores the object in the Redis database
    '''
    api.makePersistent(self)

  def make_persistent(self):
    '''Lowercase wrapper for makePersistent
    '''
    self.makePersistent()

  def delete(self):
    '''Deletes the object from the Redis database
    '''
    pass

  def getID(self):
    '''Gets the ID of the object
    '''
    return self.pycompss_psco_identifier
