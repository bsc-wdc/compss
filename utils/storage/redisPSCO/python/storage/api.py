'''API for Redis PSCO handling
@author: srodrig1
'''

def init(config_file_path=None, **kwargs):
  '''Inits the storage. It basically sets the Redis client and connects it
  to the instance/cluster
  '''
  pass


def finish(**kwargs):
  '''Finish the storage: Simply close the Redis connection
  '''
  pass

#I strongly disagree to locally override a global function name as id
def getByID(identifier):
  '''Retrieves the object that has the given identifier from the Redis database
  '''
  pass

class TaskContext(object):
  '''TaskContext PENDING TO DOCUMENT
  '''
  def __init__(self, logger, values, config_file_path=None, **kwargs):
    '''CONSTRUCTOR PENDING TO DOCUMENT
    '''
    self.logger = logger
    self.values = values
    self.config_file_path = config_file_path

  def __enter__(self):
    '''PENDING TO DOCUMENT
    '''
    pass
  
  # Same, i disagree to locally override the function "type"
  def __exit__(self, type_cntx, value, traceback):
    '''PENDING TO DOCUMENT
    '''
    pass
