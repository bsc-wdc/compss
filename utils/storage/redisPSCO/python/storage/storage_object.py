'''Redis Storage Object implementation for the PyCOMPSs Python Binding
@author: srodrig1
'''

#TODO: Consider to change the API in such a way that inherits from here
#TODO: The presentation does not define the full signature of the Storage Object. What to do?
class storage_object(object):
  '''Storage Object
  '''

  def __init__(self):
    '''Constructor method
    '''
    pass

  #TODO: Why this method has no ID as parameter?
  def makePersistent(self):
    '''Stores the object in the Redis database
    '''
    pass

  def delete(self):
    '''Deletes the object from the Redis database
    '''
    pass
  
  def getID(self):
    '''Gets the ID of the object
    '''
    pass

