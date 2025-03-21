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
"""
API for Redis PSCO handling
This class is responsible to establish and to mantain a Redis connection to
the backend, also it is responible of retrieving objects from it.
As a reminder, objects are stored as a serialized byte array.
"""
import uuid
import redis
import rediscluster
import logging
# Use some existing PyCOMPSs functions to serialize/deserialize
from pycompss.util.serialization.serializer import serialize_to_bytes
from pycompss.util.serialization.serializer import deserialize_from_bytes
from pycompss.util.serialization.serializer import deserialize_from_handler
# Enable to do from storage.api import StorageObject
# WARN: It is import to avoid circular import error
import storage.storage_object as storage_object

__name__ = "redispycompss"

"""
Constants
"""
REDIS_PORT = 6379
MAX_BLOCK_SIZE = 510 * 1024 * 1024
# MAX_BLOCK_SIZE = 16
HEADER = "[REDIS-PyCOMPSs API] "

"""
Global variables
They are declared only for visibility purposes
"""
redis_connection = None
hosts = None
logger = logging.getLogger('redis')


class StorageException(Exception):
    """
    StorageException class
    """
    pass


def init(config_file_path=None, **kwargs):
    """
    Init the storage client.
    Basically, we set the Redis client and connects it to the instance/cluster.
    """
    if __debug__:
        logger.debug(HEADER + "Initializing the storage client.")
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
    # As accorded in the API standar, this file must contain all the hosts
    # names with no port, one per line
    hosts = [x.strip() for x in config_file_handler.readlines()]
    config_file_handler.close()
    # If we have more than one host then we will assume that our backend is a
    # Redis cluster. If not, we will assume that we are dealing with a Redis
    # standalone instance
    if len(hosts) > 1:
        # Given that cluster clients are capable to perform master
        # slave hierarchy discovery, we will simply connect to the first
        # node we got
        redis_connection = \
            rediscluster.RedisCluster(host=hosts[0], port=REDIS_PORT)
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
        logger.debug(HEADER + "Initialization finished successfully.")


def initWorker(config_file_path=None):
    """
    Per-worker init function
    """
    if __debug__:
        logger.debug(HEADER +
                     "Initializing the storage client in worker process.")
    init(config_file_path)
    if __debug__:
        logger.debug(HEADER +
                     "Initialization in worker process finished successfully.")

init_worker = initWorker


def finishWorker(*args, **kwargs):
    """
    Same as finish.
    No additional actions are needed
    """
    if __debug__:
        logger.debug(HEADER + "Finalization at worker process. Not needed.")
    pass

finish_worker = finishWorker


def finish(**kwargs):
    """
    Finish the storage.
    Nothing to do, as Python redis clients have no close method.
    """
    if __debug__:
        logger.debug(HEADER + "Finalization at worker. Not needed.")
    pass


def getByIDOld(identifier):
    """
    Retrieves the object that has the given identifier from the Redis database.
    That is, given an identifier, retrieves the contents from the backend
    that correspond to this key, deserializes it and returns the reconstructed
    object.
    """
    logger.warn(HEADER + "Call to getByIDOld.")
    global redis_connection
    import io
    with io.BytesIO() as bio:
        num_blocks = int(redis_connection.llen(identifier))
        for l in redis_connection.lrange(identifier, 0, num_blocks):
            bio.write(l)
        # In case that we have read a None then it means that the requested
        # object was not present in the Redis backend
        bio.seek(0)
        ret = deserialize_from_handler(bio, True, logger)
    return ret


def getByID(*identifiers):
    """
    Retrieves a set of objects from their identifiers by pipelining the get
    commands
    """
    if __debug__:
        logger.debug(HEADER + "Call to getByID of: " + str(identifiers))
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
        ret[i] = deserialize_from_bytes(
                b''.join(
                    ret[i]
                ),
                True,
                logger
            )
        ret[i].pycompss_mark_as_unmodified()
    return ret[0] if len(ret) == 1 else ret

get_by_ID = getByID


def makePersistent(obj, identifier=None):
    """
    Persists an object to the Redis backend. Does nothing if the object is
    already persisted.
    """
    if __debug__:
        logger.debug(HEADER + "Make persistent of: " + str(obj))
    if obj.pycompss_psco_identifier is not None:
        # Non null identifier -> object is already persisted
        if __debug__:
            logger.warn(HEADER + "The object is already persistent.")
        return
    # The object has no identifier, we need to assign it one
    if identifier is None:
        obj.pycompss_psco_identifier = str(uuid.uuid4())
    else:
        obj.pycompss_psco_identifier = identifier
    # Serialize the object and store the pair (id, serialized_object)
    serialized_object = serialize_to_bytes(obj, logger)
    bytes_size = len(serialized_object)
    num_blocks = (bytes_size + MAX_BLOCK_SIZE - 1) // MAX_BLOCK_SIZE
    for block in range(num_blocks):
        l = block * MAX_BLOCK_SIZE
        r = (block + 1) * MAX_BLOCK_SIZE
        redis_connection.rpush(
            obj.pycompss_psco_identifier, serialized_object[l:r]
        )
    # Object is now synced with backend
    obj.pycompss_mark_as_unmodified()
    if __debug__:
        logger.debug(HEADER +
                     "Object persisted with id: " +
                     str(obj.pycompss_psco_identifier))

make_persistent = makePersistent


def deletePersistent(obj):
    """
    Deletes a persisted object. If the object was not persisted, then
    nothing will be done.
    """
    if __debug__:
        logger.debug(HEADER + "Delete persistent of: " + str(obj))
    if obj.pycompss_psco_identifier is None:
        # The object was not persisted, there is nothing to do
        if __debug__:
            logger.warn(HEADER + "The object is not persistent.")
        return
    if __debug__:
        logger.debug(HEADER +
                     "Persistent object to delete id: " +
                     str(obj.pycompss_psco_identifier))
    # Delete the object from the backend
    redis_connection.delete(obj.pycompss_psco_identifier)
    # Set key to None
    obj.pycompss_psco_identifier = None
    # Mark as unmodified
    obj.pycompss_mark_as_unmodified()
    if __debug__:
        logger.debug(HEADER + "Object deleted")

delete_persistent = deletePersistent


class TaskContext(object):
    """
    Task context which us used as wrapper for the task execution.
    Enables to perform prior and later actions for each task.
    """
    def __init__(self, logger, values, config_file_path=None):
        self.logger = logger
        self.values = values
        self.config_file_path = config_file_path

    def __enter__(self):
        # Do something prolog
        pass
        # Ready to start the task
        self.logger.info(HEADER + "Prolog finished")

    def __exit__(self, type, value, traceback):
        # Update all modified objects
        for obj in self.values:
            try:
                if obj.pycompss_is_modified():
                    print(HEADER + "Repersisting object %s" % obj)
                    old_id = obj.getID()
                    obj.delete_persistent()
                    obj.make_persistent(old_id)
            except:
                pass
        # Finished
        self.logger.info(HEADER + "Epilog finished")

task_context = TaskContext

StorageObject = storage_object.StorageObject
storage_object = storage_object.StorageObject
