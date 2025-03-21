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

# -*- coding: utf-8 -*-

"""
PyCOMPSs - Streams - Distro stream.

This file contains the distro stream code.
"""

import uuid
from abc import abstractmethod

from pycompss.streams.components.distro_stream_client import (
    DistroStreamClientHandler,
)
from pycompss.streams.components.objects.kafka_connectors import (
    ODSPublisher,
    ODSConsumer,
)
from pycompss.streams.types.requests import BootstrapServerRequest
from pycompss.streams.types.requests import CloseStreamRequest
from pycompss.streams.types.requests import PollRequest
from pycompss.streams.types.requests import PublishRequest

# Project imports
from pycompss.streams.types.requests import RegisterStreamRequest
from pycompss.streams.types.requests import StreamStatusRequest

# Imports
from pycompss.util.typing_helper import typing

#
# Logger definition
#
if __debug__:
    import logging

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

# Common messages
POLLING_MSG = "Polling new stream items..."


def str2bool(val: str) -> bool:
    """Convert string to boolean.

    :param val: String to analyse.
    :return: If val means true or false.
    """
    return val.lower() in ("yes", "true", "t", "1")


#
# Interface definition
#


class DistroStream:
    """Interface for File and Object Distributed Streams."""

    def __init__(self) -> None:
        """Create a new DistroStream instance."""

    @abstractmethod
    def get_stream_id(self) -> str:
        """Return the internal stream id.

        :return: The internal stream id (str)
        """

    @abstractmethod
    def get_stream_alias(self) -> str:
        """Return the internal stream alias.

        :return: The internal stream alias (str)
        """

    @abstractmethod
    def get_stream_type(self) -> str:
        """Return the internal stream type.

        :return: The internal stream type (StreamType)
        """

    @abstractmethod
    def publish(self, message: str) -> None:
        """Publish the given message on the stream.

        :param message: Message to publish.
        :return: None.
        """

    @abstractmethod
    def publish_list(self, messages: list) -> None:
        """Publish the given list of messages on the stream.

        :param messages: List of messages to publish.
        :return: None.
        """

    @abstractmethod
    def poll(self, timeout: int = 0) -> list:
        """Poll the produced messages.

        If there are registered messages, returns immediately. Otherwise,
        waits until a record is produced or the timeout is exceeded.

        :param timeout: Maximum request time to poll new messages.
        :return: List of polled messages (List<T>).
        """
        return []

    @abstractmethod
    def close(self) -> None:
        """Close the current stream.

        :return: None.
        """

    @abstractmethod
    def is_closed(self) -> bool:
        """Return whether the stream is closed or not.

        :return: True if the stream is closed, False otherwise.
        """


#
# Common Implementation
#


class DistroStreamImpl(DistroStream):
    """Implementation of the common methods of the DistroStream Interface.

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

    def __init__(
        self,
        alias: typing.Optional[str] = None,
        stream_type: typing.Optional[str] = None,
        internal_stream_info: typing.Optional[list] = None,
        access_mode: str = AT_MOST_ONCE,
    ) -> None:
        """Create a new DistroStream instance.

        :param alias: Stream alias.
        :param stream_type: Internal stream type (StreamType).
        :param internal_stream_info: Implementation specific information
                                     (List<T>).
        :param access_mode: Stream access mode (ConsumerMode).
        :raise RegistrationException: When client cannot register the stream
                                      into the server.
        """
        super().__init__()

        if __debug__:
            logger.debug("Registering new stream...")

        self.alias = alias
        self.stream_type = stream_type
        self.access_mode = access_mode

        # Retrieve registration id
        req = RegisterStreamRequest(
            self.alias,
            self.stream_type,
            self.access_mode,
            internal_stream_info,
        )
        DistroStreamClientHandler.request(req)

        req.wait_processed()
        error = req.get_error_code()
        if error != 0:
            raise RegistrationException(error, req.get_error_msg())
        self.id = req.get_response_msg()  # pylint: disable=invalid-name

    def get_stream_id(self) -> str:
        """Retrieve the stream identifier.

        :returns: The stream identifier.
        """
        return self.id

    def get_stream_alias(self) -> str:
        """Retrieve the stream alias.

        :returns: The stream alias.
        """
        return self.alias

    def get_stream_type(self) -> str:
        """Retrieve the stream alias.

        :returns: The stream type.
        """
        return self.stream_type

    @abstractmethod
    def publish(self, message: str) -> None:
        """Publish a message.

        :param message: Message to publish.
        :returns: None.
        """

    @abstractmethod
    def publish_list(self, messages: list) -> None:
        """Publish a list of messages.

        :param messages: List of message to publish.
        :returns: None.
        """

    @abstractmethod
    def poll(self, timeout: int = 0) -> list:
        """Poll the stream.

        :param timeout: Waiting time.
        :returns: None.
        """
        return []

    def close(self) -> None:
        """Close the stream.

        :returns: None.
        """
        if __debug__:
            logger.debug("Closing stream %s", str(self.id))

        # Ask for stream closure
        req = CloseStreamRequest(self.id)
        DistroStreamClientHandler.request(req)

        req.wait_processed()
        error = req.get_error_code()
        if error != 0 and __debug__:
            logger.error("ERROR: Cannot close stream")
            logger.error(" - Internal Error Code: %s", str(error))
            logger.error(" - Internal Error Msg: %s", str(req.get_error_msg()))

        # No need to process the answer message, checking the error is enough.

    def is_closed(self) -> bool:
        """Check if the stream is closed.

        :returns: If the stream is closed.
        """
        if __debug__:
            logger.debug("Checking if stream %s is closed", str(self.id))

        # Ask for stream status
        req = StreamStatusRequest(self.id)
        DistroStreamClientHandler.request(req)

        req.wait_processed()
        error = req.get_error_code()
        if error != 0 and __debug__:
            logger.error("ERROR: Cannot retrieve stream status")
            logger.error(" - Internal Error Code: %s", str(error))
            logger.error(" - Internal Error Msg: %s", str(req.get_error_msg()))

        return str2bool(req.get_response_msg())


#
# FileDistroStream definition
#


class FileDistroStream(DistroStreamImpl):
    """File Distributed Stream implementation.

    Attributes:
        - base_dir: Base directory path for the streaming.
            + type: string
    """

    def __init__(
        self,
        alias: typing.Optional[str] = None,
        base_dir: typing.Optional[str] = None,
        access_mode: str = AT_MOST_ONCE,
    ) -> None:
        """Create a new FileDistroStream instance.

        :param alias: Stream alias.
        :param base_dir: Base directory for the file stream.
        :param access_mode: Stream access mode (ConsumerMode)
        :raise RegistrationException: When client cannot register the stream
                                      into the server.
        """
        super().__init__(
            alias=alias,
            stream_type=FILE,
            internal_stream_info=[base_dir],
            access_mode=access_mode,
        )
        self.base_dir = base_dir

    def publish(self, message: str) -> None:
        """Publish message.

        Nothing to do since server automatically publishes the written files.

        :param message: Message to publish.
        :return: None.
        """
        if __debug__:
            logger.warning(
                "WARN: Unnecessary call on publish on FileDistroStream"
            )

    def publish_list(self, messages: list) -> None:
        """Publish a list of messages.

        Nothing to do since server automatically publishes the written files.

        :param messages: List of messages to publish.
        :return: None.
        """
        if __debug__:
            logger.warning(
                "WARN: Unnecessary call on publish on FileDistroStream"
            )

    def poll(self, timeout: int = 0) -> list:
        """Poll the stream.

        :param timeout: Waiting time.
        :return: List of messages.
        """
        if __debug__:
            logger.info(POLLING_MSG)

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
            logger.debug("Retrieved stream items: %s", str(info))
        if info is not None and info and info != "null":
            return info.split()
        return []


#
# ObjectDistroStream definition
#


class ObjectDistroStream(DistroStreamImpl):
    """Object Distributed Stream implementation.

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

    def __init__(
        self,
        alias: typing.Optional[str] = None,
        access_mode: str = AT_MOST_ONCE,
    ) -> None:
        """Create a new ObjectDistroStream instance.

        :param alias: Stream alias.
        :param access_mode: Stream access mode (ConsumerMode)
        :raise RegistrationException: When client cannot register the stream
                                      into the server.
        """
        super().__init__(
            alias=alias,
            stream_type=OBJECT,
            internal_stream_info=[],
            access_mode=access_mode,
        )
        self.kafka_topic_name = alias
        if alias != "":
            self.kafka_topic_name = (
                ObjectDistroStream.TOPIC_REGULAR_MESSAGES_PREFIX
                + "-"
                + self.id
            )  # noqa: E501

        self.bootstrap_server = "None"  # type: str
        self.publisher = None  # type: typing.Any
        self.consumer = None  # type: typing.Any

    def _register_publisher(self) -> None:
        """Register publisher.

        :return: None.
        """
        if self.publisher is None:
            if self.bootstrap_server == "None":
                self.bootstrap_server = (
                    ObjectDistroStream._request_bootstrap_server_info()
                )  # noqa: E501
            if __debug__:
                logger.info("Creating internal producer...")
            self.publisher = ODSPublisher(self.bootstrap_server)

    def _register_consumer(self) -> None:
        """Register consumer.

        :return: None.
        """
        if self.consumer is None:
            if self.bootstrap_server == "None":
                self.bootstrap_server = (
                    ObjectDistroStream._request_bootstrap_server_info()
                )  # noqa: E501
            if __debug__:
                logger.info("Creating internal consumer...")
            self.consumer = ODSConsumer(
                self.bootstrap_server, self.kafka_topic_name, self.access_mode
            )

    @staticmethod
    def _request_bootstrap_server_info() -> str:
        """Request bootstrap server information.

        :return: String with the retrieved information.
        """
        if __debug__:
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
            logger.debug("Retrieved bootstrap server information: %s", answer)

        return answer

    def publish(self, message: str) -> None:
        """Publish message.

        :param message: Message to publish.
        :return: None.
        """
        if __debug__:
            logger.info("Publishing new object...")
        self._register_publisher()
        self.publisher.publish(self.kafka_topic_name, message)
        if __debug__:
            logger.info("Publishing new object")

    def publish_list(self, messages: list) -> None:
        """Publish message.

        :param messages: List of messages to publish.
        :return: None.
        """
        if __debug__:
            logger.info("Publishing new List of objects...")
        self._register_publisher()
        for msg in messages:
            self.publisher.publish(self.kafka_topic_name, msg)
        if __debug__:
            logger.info("Published new List of objects")

    def poll(self, timeout: int = DEFAULT_KAFKA_TIMEOUT) -> list:
        """Poll server.

        :param timeout: Maximum waiting time.
        :return: None.
        """
        if __debug__:
            logger.info(POLLING_MSG)

        self._register_consumer()
        return self.consumer.poll(timeout)


#
# PscoDistroStream definition
#


class PscoDistroStream(DistroStreamImpl):
    """PSCO Distributed Stream implementation."""

    def __init__(self, alias: str, access_mode: str = AT_MOST_ONCE) -> None:
        """Create a new PscoDistroStream instance.

        :param alias: Stream alias.
        :param access_mode: Stream access mode (ConsumerMode).
        :raise RegistrationException: When client cannot register the stream
                                      into the server.
        """
        super().__init__(
            alias=alias,
            stream_type=PSCO,
            internal_stream_info=[],
            access_mode=access_mode,
        )

    def publish(self, message: str) -> None:
        """Publish message.

        :param message: Message to publish.
        :return: None.
        """
        if __debug__:
            logger.info("Publishing new PSCO object...")
        self._psco_publish(message)
        if __debug__:
            logger.info("Publishing new PSCO object")

    def publish_list(self, messages: list) -> None:
        """Publish message.

        :param messages: List of messages to publish.
        :return: None.
        """
        if __debug__:
            logger.info("Publishing new List of PSCOs...")
        for msg in messages:
            self._psco_publish(msg)
        if __debug__:
            logger.info("Published new List of PSCOs")

    def _psco_publish(self, psco: typing.Any) -> None:
        """Publish message.

        :param psco: Persistent object to publish.
        :return: None.
        """
        # Persist the psco if its not
        if __debug__:
            logger.debug("Persisting user PSCO...")
        if psco.getID() is None:
            alias = str(uuid.uuid4())
            psco.makePersistent(alias)
        psco_id = psco.getID()

        # Register the psco on the server
        if __debug__:
            logger.debug("Registering PSCO publish...")
        req = PublishRequest(self.id, psco_id)
        DistroStreamClientHandler.request(req)

        # Retrieve answer
        req.wait_processed()
        error = req.get_error_code()
        if error != 0:
            raise BackendException(error, req.get_error_msg())

        # Parse answer
        answer = req.get_response_msg()  # noqa
        if __debug__:
            logger.debug("Publish stream answer: %s", str(answer))

    def poll(self, timeout: int = 0) -> list:
        """Poll server.

        :param timeout: Maximum waiting time.
        :return: None.
        """
        if __debug__:
            logger.info(POLLING_MSG)

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
            logger.debug("Retrieved stream items: %s", str(info))

        from pycompss.util.storages.persistent import (
            get_by_id,
        )  # pylint: disable=import-outside-toplevel

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
    """Registration exception class."""

    def __init__(
        self,
        code: typing.Optional[int] = None,
        message: typing.Optional[str] = None,
    ) -> None:
        """Create a new RegistrationException instance.

        :param code: Internal request error code.
        :param message: Internal request error message.
        """
        super().__init__()
        self.code = code
        self.message = message

    def __str__(self) -> str:
        """Retrieve the string repr. of the RegistrationException object.

        :return: The string representation.
        """
        message = (
            f"ERROR: Registration Exception.\n"
            f" - Internal error code: {str(self.code)}\n"
            f" - Internal error message: {str(self.message)}"
        )
        return message


class BackendException(Exception):
    """Backend exception class."""

    def __init__(
        self,
        code: typing.Optional[int] = None,
        message: typing.Optional[str] = None,
    ) -> None:
        """Create a new BackendException instance.

        :param code: Internal request error code.
        :param message: Internal request error message.
        """
        super().__init__()
        self.code = code
        self.message = message

    def __str__(self) -> str:
        """Retrieve the string representation of the BackendException object.

        :return: The string representation.
        """
        message = (
            f"ERROR: Backend Exception.\n"
            f" - Internal error code: {str(self.code)}\n"
            f" - Internal error message: {str(self.message)}"
        )
        return message
