#!/usr/bin/python
#
#  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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

# Imports
import logging
from abc import abstractmethod
from threading import Semaphore

#
# Logger definition
#

logger = logging.getLogger("pycompss.streams.distro_stream")

#
# Type Enums
#
# class RequestType(Enum):
REGISTER_CLIENT = "REGISTER_CLIENT"
UNREGISTER_CLIENT = "UNREGISTER_CLIENT"
BOOTSTRAP_SERVER = "BOOTSTRAP_SERVER"
REGISTER_STREAM = "REGISTER_STREAM"
STREAM_STATUS = "STREAM_STATUS"
CLOSE_STREAM = "CLOSE_STREAM"
POLL = "POLL"
PUBLISH = "PUBLISH"
STOP = "STOP"


#
# Interface definition
#
class Request(object):
    """
    Interface for Client-Server requests.

    Attributes:
        - rt: Request type.
            + type: RequestType
        - has_been_processed: Whether the request has been processed or not.
            + type: boolean
        - wait_sem: Internal waiting semaphore for request completion.
            + type: Semaphore
        - error_code: Request error code.
            + type: int
        - error_msg: Request error message.
            + type: string
        - response_msg: Client response message.
            + type: string
    """

    def __init__(self, rt):
        # type: (str) -> None
        """Creates a new Request instance.

        :param rt: Request type (RequestType)
        """
        self.rt = rt

        self.has_been_processed = False
        self.wait_sem = Semaphore(0)

        self.error_code = -1
        self.error_msg = "None"
        self.response_msg = "None"

    def get_type(self):
        # type: () -> str
        """Returns the request type.

        :return: The request type (RequestType).
        """
        return self.rt

    def is_processed(self):
        # type: () -> bool
        """Returns whether the request has been processed or not.

        :return: True if the request has been processed, False otherwise.
        """
        return self.has_been_processed

    def wait_processed(self):
        # type: () -> None
        """Locks the current thread until the request has been processed.

        :return: None.
        """
        self.wait_sem.acquire()

    def get_error_code(self):
        # type: () -> int
        """Returns the request error code.

        :return: The request error code.
        """
        return self.error_code

    def get_error_msg(self):
        # type: () -> str
        """Returns the request error message.

        :return: The request error message.
        """
        return self.error_msg

    def get_response_msg(self):
        # type: () -> str
        """Returns the request response message.

        :return: The request response message.
        """
        return self.response_msg

    @abstractmethod
    def get_request_msg(self):
        # type: () -> str
        """Returns the request message to send to the server.

        :return: The request message to send to the server.
        """
        pass

    def set_processed(self):
        # type: () -> None
        """Marks the request as processed.

        :return: None.
        """
        self.has_been_processed = True
        self.wait_sem.release()

    def set_error(self, error_code, error_msg):
        # type: (int, str) -> None
        """Sets a new error code and message to the current request.

        :param error_code: Error code.
        :param error_msg: Error message.
        :return: None.
        """
        self.error_code = error_code
        self.error_msg = error_msg

    def set_response(self, msg):
        # type: (str) -> None
        """Sets a new response message to the current request.

        :param msg: New response message.
        :return: None.
        """
        self.error_code = 0
        self.response_msg = msg


#
# Specific requests implementations
#


class RegisterStreamRequest(Request):
    """
    Request to register a new stream

    Attributes:
        - alias: Associated stream alias.
            + type: string
        - stream_type: Associated stream type.
            + type: StreamType
        - access_mode: Associated stream access mode.
            + type: ConsumerMode
        - internal_stream_info: Associated information about the internal
                                stream implementation.
            + type: List<string>
    """

    def __init__(self, alias, stream_type, access_mode, internal_stream_info):
        # type: (str, str, str, list) -> None
        """Creates a new RegisterStreamRequest instance.

        :param alias: Associated stream alias.
        :param stream_type: Associated stream type (StreamType)
        :param access_mode: Associated stream access mode (ConsumerMode)
        :param internal_stream_info: Associated information about the internal
                                     stream implementation (List<string>)
        """
        super(RegisterStreamRequest, self).__init__(rt=REGISTER_STREAM)
        self.alias = alias
        self.stream_type = stream_type
        self.access_mode = access_mode
        self.internal_stream_info = internal_stream_info

    def get_request_msg(self):
        # type: () -> str
        """Get request message.

        :return: Message.
        """
        s = " ".join(
            (
                str(self.rt),
                str(self.stream_type),
                str(self.access_mode),
                str(self.alias),
            )
        )
        if self.internal_stream_info is not None:
            for info in self.internal_stream_info:
                s = s + " " + str(info)
        return s


class StopRequest(Request):
    """
    Request to stop the client.
    """

    def __init__(self):
        # type: () -> None
        """Creates a new StopRequest instance."""
        super(StopRequest, self).__init__(rt=STOP)

    def get_request_msg(self):
        # type: () -> str
        s = str(self.rt)
        return s


class BootstrapServerRequest(Request):
    """
    Request to retrieve the bootstrap server information.
    """

    def __init__(self):
        # type: () -> None
        """Creates a new BootstrapServerRequest instance."""
        super(BootstrapServerRequest, self).__init__(rt=BOOTSTRAP_SERVER)

    def get_request_msg(self):
        # type: () -> str
        """Get request message.

        :return: Message.
        """
        s = str(self.rt)
        return s


class StreamStatusRequest(Request):
    """
    Request to retrieve the status of the given stream.

    Attributes:
        - stream_id : Stream Id.
            + type: UUID
    """

    def __init__(self, stream_id):
        # type: (str) -> None
        """Creates a new StreamStatusRequest instance.

        :param stream_id: Stream Id.
        """
        super(StreamStatusRequest, self).__init__(rt=STREAM_STATUS)
        self.stream_id = stream_id

    def get_request_msg(self):
        # type: () -> str
        """Get request message.

        :return: Message.
        """
        return "%s %s" % (str(self.rt), str(self.stream_id))


class CloseStreamRequest(Request):
    """
    Request to close the given stream.

    Attributes:
        - stream_id : Stream Id.
            + type: UUID
    """

    def __init__(self, stream_id):
        # type: (str) -> None
        """Creates a new CloseStreamRequest instance.

        :param stream_id: Stream Id.
        """
        super(CloseStreamRequest, self).__init__(rt=CLOSE_STREAM)
        self.stream_id = stream_id

    def get_request_msg(self):
        # type: () -> str
        """Get request message.

        :return: Message.
        """
        return "%s %s" % (str(self.rt), str(self.stream_id))


class PollRequest(Request):
    """
    Request to poll the given stream.

    Attributes:
        - stream_id : Stream Id.
            + type: UUID
    """

    def __init__(self, stream_id):
        # type: (str) -> None
        """Creates a new PollRequest instance.

        :param stream_id: Stream Id.
        """
        super(PollRequest, self).__init__(rt=POLL)
        self.stream_id = stream_id

    def get_request_msg(self):
        # type: () -> str
        """Get request message.

        :return: Message.
        """
        return "%s %s" % (str(self.rt), str(self.stream_id))


class PublishRequest(Request):
    """
    Request to publish a new element to the given stream.

    Attributes:
        - stream_id : Stream Id.
            + type: UUID
        - msg : Message to publish
            + type: str
    """

    def __init__(self, stream_id, msg):
        # type: (str, str) -> None
        """Creates a new PublishRequest instance.

        :param stream_id: Stream Id (UUID).
        :param msg: Message to publish.
        """
        super(PublishRequest, self).__init__(rt=PUBLISH)
        self.stream_id = stream_id
        self.msg = msg

    def get_request_msg(self):
        # type: () -> str
        """Get request message.

        :return: Message.
        """
        return " ".join((str(self.rt), str(self.stream_id), str(self.msg)))
