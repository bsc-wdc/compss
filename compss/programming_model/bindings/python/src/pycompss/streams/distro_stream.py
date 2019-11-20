#!/usr/bin/python
#
#  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import unittest
import logging
from abc import abstractmethod

# Project imports
from pycompss.streams.types.requests import RegisterStreamRequest
from pycompss.streams.types.requests import CloseStreamRequest
from pycompss.streams.types.requests import StreamStatusRequest
from pycompss.streams.types.requests import BootstrapServerRequest
from pycompss.streams.types.requests import PollRequest
from pycompss.streams.types.requests import PublishRequest
from pycompss.streams.components.distro_stream_client import DistroStreamClientHandler

#
# Logger definition
#

logger = logging.getLogger("pycompss.streams.distro_stream")

#
# Type Enums
#
# class StreamType(Enum):
FILE = "FILE"
OBJECT = "OBJECT"
PSCO = "PSCO"

# class ConsumerMode(Enum):
AT_MOST_ONCE = "AT_MOST_ONCE"
AT_LEAST_ONCE = "AT_LEAST_ONCE"


def str2bool(val):
    return val.lower() in ("yes", "true", "t", "1")


#
# Interface definition
#

class DistroStream(object):
    """
    Interface for File and Object Distributed Streams.

    Attributes:
    """

    def __init__(self):
        """
        Creates a new DistroStream instance.
        """
        pass

    @abstractmethod
    def get_stream_id(self):
        """
        Returns the internal stream id.

        :return: The internal stream id
            + type: string
        """
        pass

    @abstractmethod
    def get_stream_alias(self):
        """
        Returns the internal stream alias.

        :return: The internal stream alias
            + type: string
        """
        pass

    @abstractmethod
    def get_stream_type(self):
        """
        Returns the internal stream type.

        :return: The internal stream type
            + type: StreamType
        """
        pass

    @abstractmethod
    def publish(self, message):
        """
        Publishes the given message on the stream.

        :param message: Message to publish.
        :return: None
        """
        pass

    @abstractmethod
    def publish_list(self, messages):
        """
        Publishes the given list of messages on the stream.

        :param messages: List of messages to publish.
        :return: None
        """
        pass

    @abstractmethod
    def poll(self, timeout=None):
        """
        Polls the produced messages. If there are registered messages, returns immediately. Otherwise,
         waits until a record is produced or the timeout is exceeded.

        :param timeout: Maximum request time to poll new messages.
        :return: List of polled messages.
            + type: List<T>
        """
        pass

    @abstractmethod
    def close(self):
        """
        Closes the current stream.

        :return: None
        """
        pass

    @abstractmethod
    def is_closed(self):
        """
        Returns whether the stream is closed or not.

        :return: True if the stream is closed, False otherwise.
            + type: boolean
        """
        pass


#
# Common Implementation
#

class DistroStreamImpl(DistroStream):
    """
    Implementation of the common methods of the DistroStream Interface.

    Attributes:
        - alias: Stream Alias.
            + type: string
        - id: Stream Id.
            + type: string containing UUID
        - stream_type: Internal stream type.
            + type: StreamType
        - access_mode: Stream consumer access mode.
            + type: ConsumerMode
    """

    def __init__(self, alias=None, stream_type=None, access_mode=AT_MOST_ONCE, internal_stream_info=None):
        """
        Creates a new DistroStream instance.

        :param alias: Stream alias.
            + type: string
        :param stream_type: Internal stream type.
            + type: StreamType
        :param access_mode: Stream access mode.
            + type: ConsumerMode
        :param internal_stream_info: Implementation specific information.
            + type: List<T>
        :raise RegistrationException: When client cannot register the stream into the server.
        """
        super(DistroStreamImpl, self).__init__()

        logger.debug("Registering new stream...")

        self.alias = alias
        self.stream_type = stream_type
        self.access_mode = access_mode

        # Retrieve registration id
        req = RegisterStreamRequest(self.alias, self.stream_type, self.access_mode, internal_stream_info)
        DistroStreamClientHandler.request(req)

        req.wait_processed()
        error = req.get_error_code()
        if error != 0:
            raise RegistrationException(error, req.get_error_msg())
        self.id = req.get_response_msg()

    def get_stream_id(self):
        return self.id

    def get_stream_alias(self):
        return self.alias

    def get_stream_type(self):
        return self.stream_type

    @abstractmethod
    def publish(self, message):
        pass

    @abstractmethod
    def publish_list(self, messages):
        pass

    @abstractmethod
    def poll(self, timeout=None):
        pass

    def close(self):
        if __debug__:
            logger.debug("Closing stream " + str(self.id))

        # Ask for stream closure
        req = CloseStreamRequest(self.id)
        DistroStreamClientHandler.request(req)

        req.wait_processed()
        error = req.get_error_code()
        if error != 0:
            logger.error("ERROR: Cannot close stream")
            logger.error(" - Internal Error Code: " + str(error))
            logger.error(" - Internal Error Msg: " + str(req.get_error_msg()))

        # No need to process the answer message, checking the error is enough.

    def is_closed(self):
        if __debug__:
            logger.debug("Checking if stream " + str(self.id) + " is closed")

        # Ask for stream status
        req = StreamStatusRequest(self.id)
        DistroStreamClientHandler.request(req)

        req.wait_processed()
        error = req.get_error_code()
        if error != 0:
            logger.error("ERROR: Cannot retrieve stream status")
            logger.error(" - Internal Error Code: " + str(error))
            logger.error(" - Internal Error Msg: " + str(req.get_error_msg()))

        return str2bool(req.get_response_msg())


#
# FileDistroStream definition
#

class FileDistroStream(DistroStreamImpl):
    """
    File Distributed Stream implementation.

    Attributes:
        - base_dir: Base directory path for the streaming.
            + type: string

    """

    def __init__(self, alias=None, access_mode=AT_MOST_ONCE, base_dir=None):
        """
        Creates a new FileDistroStream instance.

        :param alias: Stream alias.
            + type: string
        :param access_mode: Stream access mode.
            + type: ConsumerMode
        :param base_dir: Base directory for the file stream.
            + type: string
        :raise RegistrationException: When client cannot register the stream into the server.
        """
        super(FileDistroStream, self).__init__(alias=alias,
                                               stream_type=FILE,
                                               access_mode=access_mode,
                                               internal_stream_info=[base_dir])
        self.base_dir = base_dir

    def publish(self, message):
        # Nothing to do since server automatically publishes the written files
        logger.warn("WARN: Unnecessary call on publish on FileDistroStream")

    def publish_list(self, messages):
        # Nothing to do since server automatically publishes the written files
        logger.warn("WARN: Unnecessary call on publish on FileDistroStream")

    def poll(self, timeout=None):
        logger.info("Polling new stream items...")

        # Send request to server
        req = PollRequest(self.id)
        DistroStreamClientHandler.request(req)

        # Retrieve answer
        req.wait_processed()
        error = req.get_error_code()
        if error != 0:
            raise BackendException(error, req.get_error_msg())

        # Parse answer
        info = req.get_response_msg()
        if __debug__:
            logger.debug("Retrieved stream items: " + str(info))
        if info is not None and info and info != "null":
            return info.split()
        else:
            return []


#
# ObjectDistroStream definition
#

class ObjectDistroStream(DistroStreamImpl):
    """
    Object Distributed Stream implementation.

    Attributes:
        - kafka_topic_name: Registered topic name on the Kafka backend
            + type: string
        - bootstrap_server: Bootstrap server information
            + type: string
        - publisher: Internal Kafka connector for publish
            + type: ODSPublisher
        - consumer: Internal Kafka connector for consume
            + type: ODSConsumer
    """

    TOPIC_REGULAR_MESSAGES_PREFIX = "regular-messages"
    TOPIC_SYSTEM_MESSAGES = "system-messages"

    DEFAULT_KAFKA_TIMEOUT = 200  # ms

    def __init__(self, alias=None, access_mode=AT_MOST_ONCE):
        """
        Creates a new ObjectDistroStream instance.

        :param alias: Stream alias.
            + type: string
        :param access_mode: Stream access mode.
            + type: ConsumerMode
        :raise RegistrationException: When client cannot register the stream into the server.
        """
        super(ObjectDistroStream, self).__init__(alias=alias,
                                                 stream_type=OBJECT,
                                                 access_mode=access_mode,
                                                 internal_stream_info=[])
        if alias is None:
            self.kafka_topic_name = ObjectDistroStream.TOPIC_REGULAR_MESSAGES_PREFIX + "-" + self.id
        else:
            self.kafka_topic_name = alias

        self.bootstrap_server = None
        self.publisher = None
        self.consumer = None

    def _register_publisher(self):
        from pycompss.streams.components.objects.kafka_connectors import ODSPublisher
        if self.publisher is None:
            if self.bootstrap_server is None:
                self.bootstrap_server = ObjectDistroStream._request_bootstrap_server_info()

            logger.info("Creating internal producer...")
            self.publisher = ODSPublisher(self.bootstrap_server)

    def _register_consumer(self):
        from pycompss.streams.components.objects.kafka_connectors import ODSConsumer
        if self.consumer is None:
            if self.bootstrap_server is None:
                self.bootstrap_server = ObjectDistroStream._request_bootstrap_server_info()

            logger.info("Creating internal consumer...")
            self.consumer = ODSConsumer(self.bootstrap_server, self.kafka_topic_name, self.access_mode)

    @staticmethod
    def _request_bootstrap_server_info():
        logger.info("Requesting bootstrap server...")
        req = BootstrapServerRequest()
        DistroStreamClientHandler.request(req)

        # Retrieve answer
        req.wait_processed()
        error = req.get_error_code()
        if error != 0:
            raise BackendException(error, req.get_error_msg())

        # Parse answer
        answer = req.get_response_msg()
        if __debug__:
            logger.debug("Retrieved bootstrap server information: " + answer)

        return answer

    def publish(self, message):
        logger.info("Publishing new object...")
        self._register_publisher()
        self.publisher.publish(self.kafka_topic_name, message)
        logger.info("Publishing new object")

    def publish_list(self, messages):
        logger.info("Publishing new List of objects...")
        self._register_publisher()
        for msg in messages:
            self.publisher.publish(self.kafka_topic_name, msg)
        logger.info("Published new List of objects")

    def poll(self, timeout=DEFAULT_KAFKA_TIMEOUT):
        logger.info("Polling new stream items...")

        self._register_consumer()
        return self.consumer.poll(timeout)


#
# PscoDistroStream definition
#

class PscoDistroStream(DistroStreamImpl):
    """
    PSCO Distributed Stream implementation.

    Attributes:

    """

    def __init__(self, alias=None, access_mode=AT_MOST_ONCE):
        """
        Creates a new PscoDistroStream instance.

        :param alias: Stream alias.
            + type: string
        :param access_mode: Stream access mode.
            + type: ConsumerMode
        :raise RegistrationException: When client cannot register the stream into the server.
        """
        super(PscoDistroStream, self).__init__(alias=alias,
                                               stream_type=PSCO,
                                               access_mode=access_mode,
                                               internal_stream_info=[])

    def publish(self, message):
        logger.info("Publishing new PSCO object...")
        self._psco_publish(message)
        logger.info("Publishing new PSCO object")

    def publish_list(self, messages):
        logger.info("Publishing new List of PSCOs...")
        for msg in messages:
            self._psco_publish(msg)
        logger.info("Published new List of PSCOs")

    def _psco_publish(self, psco):
        # Persist the psco if its not
        logger.debug("Persisting user PSCO...")
        if psco.getID() is None:
            import uuid
            alias = str(uuid.uuid4())
            psco.makePersistent(alias)
        psco_id = psco.getID()

        # Register the psco on the server
        logger.debug("Registering PSCO publish...")
        req = PublishRequest(self.id, psco_id)
        DistroStreamClientHandler.request(req)

        # Retrieve answer
        req.wait_processed()
        error = req.get_error_code()
        if error != 0:
            raise BackendException(error, req.get_error_msg())

        # Parse answer
        answer = req.get_response_msg()
        if __debug__:
            logger.debug("Publish stream answer: " + str(answer))

    def poll(self, timeout=None):
        logger.info("Polling new stream items...")

        # Send request to server
        req = PollRequest(self.id)
        DistroStreamClientHandler.request(req)

        # Retrieve answer
        req.wait_processed()
        error = req.get_error_code()
        if error != 0:
            raise BackendException(error, req.get_error_msg())

        # Parse answer
        info = req.get_response_msg()
        if __debug__:
            logger.debug("Retrieved stream items: " + str(info))

        from pycompss.util.storages.persistent import get_by_id
        retrieved_pscos = []
        if info is not None and info and info != "null":
            for psco_id in info.split():
                psco = get_by_id(psco_id)
                retrieved_pscos.append(psco)
        return retrieved_pscos


#
# Exception Class
#

class RegistrationException(Exception):

    def __init__(self, code=None, message=None):
        """
        Creates a new RegistrationException instance.

        :param code: Internal request error code.
            + type: int
        :param message: Internal request error message.
            + type: string
        """
        self.code = code
        self.message = message

    def __str__(self):
        s = "ERROR: Registration Exception.\n"
        s = s + " - Internal error code: " + str(self.code)
        s = s + " - Internal error message:" + str(self.message)

        return s


class BackendException(Exception):

    def __init__(self, code=None, message=None):
        """
        Creates a new BackendException instance.

        :param code: Internal request error code.
            + type: int
        :param message: Internal request error message.
            + type: string
        """
        self.code = code
        self.message = message

    def __str__(self):
        s = "ERROR: Backend Exception.\n"
        s = s + " - Internal error code: " + str(self.code)
        s = s + " - Internal error message:" + str(self.message)

        return s


#
# UNIT TEST CASES
#

class TestDistroStream(unittest.TestCase):

    def test_file_distro_stream(self):
        # TODO: Add test
        pass

    def test_object_distro_stream(self):
        # TODO: Add test
        pass


#
# MAIN FOR UNIT TEST
#

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s | %(levelname)s | %(name)s - %(message)s')
    unittest.main()
