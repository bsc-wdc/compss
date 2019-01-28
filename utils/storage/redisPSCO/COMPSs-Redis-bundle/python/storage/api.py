#
#  Copyright 2017 Barcelona Supercomputing Center (www.bsc.es)
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
'''API for Redis PSCO handling
This class is responsible to establish and to mantain a Redis connection to
the backend, also it is responible of retrieving objects from it.
As a reminder, objects are stored as a serialized byte array.
@author: srodrig1 < sergio dot rodriguez at bsc dot es >
'''
import uuid
import redis
import rediscluster
import logging
from pycompss.util.serializer import serialize_to_string, deserialize_from_string, deserialize_from_handler
__name__ = "redispycompss"

'''Constants
'''
REDIS_PORT = 6379
MAX_BLOCK_SIZE = 510 * 1024 * 1024
#MAX_BLOCK_SIZE = 16

'''Global variables
They are declared only for visibility purposes
'''
redis_connection = None
hosts = None
logger = logging.getLogger('storage')


class StorageException(Exception):
    '''StorageException class
    '''
    pass


def init(config_file_path=None, **kwargs):
    '''Init the storage client. Basically, we set the Redis client and connects it
    to the instance/cluster
    '''
    if __debug__:
        logger.debug("[REDIS-PyCOMPSs API] Initializing the storage client.")
    global redis_connection
    global hosts
    # If config_file_path is None we will assume that we only have localhost
    # as storage node
    if config_file_path is None:
        try:
            import StringIO as sio
        except ImportError:
            from io import StringIO as sio
        config_file_handler = sio.StringIO('localhost\n')
    else:
        config_file_handler = open(config_file_path)
    # As accorded in the API standar, this file must contain all the hosts names
    # with no port, one per line
    hosts = [x.strip() for x in config_file_handler.readlines()]
    config_file_handler.close()
    # If we have more than one host then we will assume that our backend is a Redis
    # cluster. If not, we will assume that we are dealing with a Redis standalone
    # instance
    if len(hosts) > 1:
        # Given that cluster clients are capable to perform master
        # slave hierarchy discovery, we will simply connect to the first
        # node we got
        redis_connection = \
            rediscluster.StrictRedisCluster(host=hosts[0], port=REDIS_PORT)
    else:
        # We are in standalone mode
        redis_connection = \
            redis.StrictRedis(host=hosts[0], port=REDIS_PORT)
    # StrictRedis is not capable to know if we had success when connecting by
    # simply calling the constructor. We need to perform an actual query to
    # the backend
    # If we had no success this first line should crash
    redis_connection.set('PYCOMPSS_TEST', 'OK')
    # Beware py2 vs py3 - b'string' works for both.
    assert redis_connection.get('PYCOMPSS_TEST') == b'OK'
    redis_connection.delete('PYCOMPSS_TEST')
    if __debug__:
        logger.debug("[REDIS-PyCOMPSs API] Initialization finished successfully.")

def initWorker(config_file_path=None):
    '''Per-worker init function
    '''
    if __debug__:
        logger.debug("[REDIS-PyCOMPSs API] Initializing the storage client in worker process.")
    init(config_file_path)
    if __debug__:
        logger.debug("[REDIS-PyCOMPSs API] Initialization in worker process finished successfully.")


init_worker = initWorker

def finishWorker(*args, **kwargs):
    '''Same as finish. No additional actions are needed
    '''
    if __debug__:
        logger.debug("[REDIS-PyCOMPSs API] Finalization at worker process. Not needed.")
    pass

finish_worker = finishWorker

def finish(**kwargs):
    '''Finish the storage: Nothing to do, as Python redis clients have no
    close method.
    '''
    if __debug__:
        logger.debug("[REDIS-PyCOMPSs API] Finalization at worker. Not needed.")
    pass

def getByIDOld(identifier):
    '''Retrieves the object that has the given identifier from the Redis database.
    That is, given an identifier, retrieves the contents from the backend
    that correspond to this key, deserializes it and returns the reconstructed
    object.
    '''
    logger.warn("[REDIS-PyCOMPSs API] Call to getByIDOld.")
    global redis_connection
    import io
    with io.BytesIO() as bio:
        num_blocks = int(redis_connection.llen(identifier))
        for l in redis_connection.lrange(identifier, 0, num_blocks):
            bio.write(l)
        # In case that we have read a None then it means that the requested object
        # was not present in the Redis backend
        bio.seek(0)
        ret = deserialize_from_handler(bio)
    return ret

def getByID(*identifiers):
    '''Retrieves a set of objects from their identifiers by pipelining the get commands
    '''
    if __debug__:
        logger.debug("[REDIS-PyCOMPSs API] Call to getByID of: " + str(identifiers))
    global redis_connection
    p = redis_connection.pipeline()
    # Stack the pipe calls
    for identifier in identifiers:
        num_blocks = int(redis_connection.llen(identifier))
        p.lrange(identifier, 0, num_blocks)
    # Get all the objects
    ret = p.execute()
    # Deserialize and delete the serialized contents for each object
    for i in range(len(identifiers)):
        ret[i] = deserialize_from_string(
                b''.join(
                    ret[i]
                )
            )
        ret[i].pycompss_mark_as_unmodified()
    return ret[0] if len(ret) == 1 else ret

get_by_ID = getByID

def makePersistent(obj, identifier = None):
    '''Persists an object to the Redis backend. Does nothing if the object
    is already persisted.
    '''
    if __debug__:
        logger.debug("[REDIS-PyCOMPSs API] Make persistent of: " + str(obj))
    if obj.pycompss_psco_identifier is not None:
        # Non null identifier -> object is already persisted
        if __debug__:
            logger.warn("[REDIS-PyCOMPSs API] The object is already persistent.")
        return
    # The object has no identifier, we need to assign it one
    obj.pycompss_psco_identifier = str(uuid.uuid4()) if identifier is None \
    else identifier
    # Serialize the object and store the pair (id, serialized_object)
    serialized_object = serialize_to_string(obj)
    bytes_size = len(serialized_object)
    num_blocks = (bytes_size + MAX_BLOCK_SIZE - 1) // MAX_BLOCK_SIZE
    for block in range( num_blocks ):
        l = block * MAX_BLOCK_SIZE
        r = (block + 1) * MAX_BLOCK_SIZE
        redis_connection.rpush(
            obj.pycompss_psco_identifier, serialized_object[l : r]
        )
    # Object is now synced with backend
    obj.pycompss_mark_as_unmodified()
    if __debug__:
        logger.debug("[REDIS-PyCOMPSs API] Object persisted with id: " + str(obj.pycompss_psco_identifier))

make_persistent = makePersistent

def deletePersistent(obj):
    '''Deletes a persisted object. If the object was not persisted, then
    nothing will be done.
    '''
    if __debug__:
        logger.debug("[REDIS-PyCOMPSs API] Delete persistent of: " + str(obj))
    if obj.pycompss_psco_identifier is None:
        # The object was not persisted, there is nothing to do
        if __debug__:
            logger.warn("[REDIS-PyCOMPSs API] The object is not persistent.")
        return
    if __debug__:
        logger.debug("[REDIS-PyCOMPSs API] Persistent object to delete id: " + str(obj.pycompss_psco_identifier))
    # Delete the object from the backend
    redis_connection.delete(obj.pycompss_psco_identifier)
    # Set key to None
    obj.pycompss_psco_identifier = None
    # Mark as unmodified
    obj.pycompss_mark_as_unmodified()
    if __debug__:
        logger.debug("[REDIS-PyCOMPSs API] Object deleted")

delete_persistent = deletePersistent

class TaskContext(object):
    '''Here for compatibility purposes
    '''
    def __init__(self, logger, values, config_file_path=None):
        self.logger = logger
        self.values = values
        self.config_file_path = config_file_path

    def __enter__(self):
        # Do something prolog

        # Ready to start the task
        self.logger.info("Prolog finished")
        pass

    def __exit__(self, type, value, traceback):
        # Update all modified objects
        for obj in self.values:
            try:
                if obj.pycompss_is_modified():
                    print('Repersisting object %s' % obj)
                    old_id = obj.getID()
                    obj.delete_persistent()
                    obj.make_persistent(old_id)
            except:
                pass
        # Finished
        self.logger.info("Epilog finished")
        pass

task_context = TaskContext
