'''API for Redis PSCO handling
This class is responsible to establish and to mantain a Redis connection to
the backend, also it is responible of retrieving objects from it.
As a reminder, objects are stored as a serialized byte array.
@author: srodrig1 < sergio dot rodriguez at bsc dot es >
'''
import uuid
import redis
import rediscluster
from pycompss.util.serializer import serialize_to_string, deserialize_from_string


'''Constants
'''
REDIS_PORT = 6379

'''Global variables
They are declared only for visibility purposes
'''

redis_connection = None
hosts = None


class StorageException(Exception):
    '''StorageException class
    '''
    pass


def init(config_file_path=None, **kwargs):
  '''Init the storage client. Basically, we set the Redis client and connects it
  to the instance/cluster
  '''
  global redis_connection
  # If config_file_path is None we will assume that we only have localhost
  # as storage node
  if config_file_path is None:
      import StringIO as sio
      config_file_handler = sio.StringIO('localhost\n')
  else:
      config_file_handler = open(config_file_path)
  # As accorded in the API standar, this file must contain all the hosts names
  # with no port, one per line
  hosts = [x.strip() for x in config_file_handler.readlines()]
  config_file_handler.close()
  # We do not know if the current backend is a standalone redis server
  # or a redis server. However, the Redis Cluster protocol is different from
  # the standalone Redis one, so if we try to establish a connection to a
  # cluster when there is a standalone instance, a exception will be thrown
  try:
    # Given that cluster clients are capable to perform master
    # slave hierarchy discovery, we will simply connect to the first
    # node we got
    redis_connection = \
      rediscluster.StrictRedisCluster(host=hosts[0], port=REDIS_PORT)
  except:
    # We are in standalone mode
    redis_connection = \
      redis.StrictRedis(host=hosts[0], port=REDIS_PORT)
    # SrictRedis is not capable to know if we had success when connecting by
    # simply calling the constructor. We need to perform an actual query to
    # the backend
    # If we had no success this first line should crash
    redis_connection.set('PYCOMPSS_TEST', 'OK')
    assert redis_connection.get('PYCOMPSS_TEST') == 'OK'
    redis_connection.delete('PYCOMPSS_TEST')

def finish(**kwargs):
  '''Finish the storage: Nothing to do, as Python redis clients have no
  close method.
  '''
  pass

def getByID(identifier):
  '''Retrieves the object that has the given identifier from the Redis database.
  That is, given an identifier, retrieves the contents from the backend
  that correspond to this key, deserializes it and returns the reconstructed
  object.
  '''
  serialized_contents = redis_connection.get(identifier)
  if serialized_contents is None:
    error_message = 'ERROR: Redis backend has no object with id %s'%identifier
    raise StorageException(error_message)
  return deserialize_from_string(serialized_contents)

def makePersistent(obj):
  '''Persists an object to the Redis backend. Does nothing if the object
  is already persisted.
  '''
  if obj.pycompss_psco_identifier is not None:
    # Non null identifier -> object is already persisted
    return
  # The object has no identifier, we need to assign it one
  obj.pycompss_psco_identifier = str(uuid.uuid4())
  # Serialize the object and store the pair (id, serialized_object)
  serialized_object = serialize_to_string(obj)
  redis_connection.set(obj.pycompss_psco_identifier, serialized_object)
