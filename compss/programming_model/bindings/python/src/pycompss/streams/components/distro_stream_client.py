#!/usr/bin/env python3
#
#  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs - Streams - Components.

This file contains the distro stream components code.
"""

# Imports
import queue
import socket
import time
from threading import Thread

# Project imports
from pycompss.streams.types.requests import STOP
from pycompss.streams.types.requests import StopRequest
from pycompss.util.typing_helper import typing

#
# Logger definition
#

if __debug__:
    import logging

    logger = logging.getLogger("pycompss.streams.distro_stream_client")


#
# Client Handler
#


class DistroStreamClientHandler:
    """Handler to use the DistroStreamClient.

    This is a static class.

    Attributes:
        - CLIENT: Distributed Stream Client to handle.
            + type: DistroStreamClient
    """

    CLIENT = None  # type: typing.Any

    def __init__(self) -> None:
        """Create a new handler instance.

        Should never be called directly since all attributes are static.
        """
        # Nothing to do since this is a static handler

    @staticmethod
    def init_and_start(
        master_ip: typing.Optional[str] = None,
        master_port: typing.Optional[str] = None,
    ) -> None:
        """Initialize and starts the client.

        :param master_ip: Master IP.
        :param master_port: Master port.
        :return: None.
        """
        DistroStreamClientHandler.CLIENT = DistroStreamClient(
            master_ip=master_ip, master_port=master_port
        )
        DistroStreamClientHandler.CLIENT.start()

    @staticmethod
    def set_stop() -> None:
        """Mark the client to stop.

        :return: None.
        """
        req = StopRequest()
        DistroStreamClientHandler.CLIENT.add_request(req)

    @staticmethod
    def request(req: typing.Any) -> None:
        """Add a new request to the client.

        :param req: Client request (Subclass of Request)
        :return: None.
        """
        DistroStreamClientHandler.CLIENT.add_request(req)


#
# Client definition
#
class DistroStreamClient(Thread):
    """Distro Stream Client definition.

    Attributes:
        - master_ip: Master IP address.
            + type: string
        - master_port: Master port.
            + type: int
        - running: Whether the client thread is running or not
            + type: boolean
        - requests: Queue of pending client requests
            + type: Queue.Queue
    """

    BUFFER_SIZE = 4096

    def __init__(
        self,
        master_ip: typing.Optional[str],
        master_port: typing.Optional[str],
    ) -> None:
        """Create a new Client associated to the given master properties.

        :param master_ip: Master IP address.
        :param master_port: Master port.
        """
        super().__init__()

        if __debug__:
            logger.info(
                "Initializing DS Client on %s:%s", master_ip, master_port
            )

        # Register information
        self.master_ip = master_ip
        self.master_port = int(str(master_port))

        # Initialize internal structures
        self.running = True
        self.requests = None  # type: typing.Any
        self.requests = queue.Queue()

    def run(self) -> None:
        """Run method of the internal thread.

        :return: None.
        """
        if __debug__:
            logger.info("DS Client started")

        while self.running:
            # Process requests
            req = self.requests.get(block=True)

            if __debug__:
                logger.debug("Processing request: %s", str(req.get_type()))

            if req.get_type() == STOP:
                if __debug__:
                    logger.info("DS Client asked to stop")
                self.running = False
                req.set_response("DONE")
            else:
                self._process_request(req)

            # Mark the request as processed
            req.set_processed()
            self.requests.task_done()

        if __debug__:
            logger.info("DS Client stopped")

    def _process_request(self, req: typing.Any) -> None:
        """Process requests to the server.

        :param req: Request.
        :return: None.
        """
        # Retry on connection refused: the DistroStream master server may not
        # be ready yet immediately after COMPSs starts
        # (timing race on restart).
        _MAX_RETRIES = 10
        _RETRY_DELAY = 3  # seconds

        last_exc: typing.Optional[Exception] = None
        for attempt in range(_MAX_RETRIES):
            comm_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                comm_socket.connect((self.master_ip, self.master_port))

                # Send request message
                req_msg = req.get_request_msg()
                if __debug__:
                    logger.debug("Sending request to server: %s", str(req_msg))
                req_msg = req_msg + "\n"
                req_msg = req_msg.encode()
                comm_socket.sendall(req_msg)
                if __debug__:
                    logger.debug("Sent request to server")

                # Receive answer
                chunk = comm_socket.recv(DistroStreamClient.BUFFER_SIZE)
                answer = chunk
                if __debug__:
                    logger.debug(
                        "Received answer from server: %s", str(answer)
                    )
                while (
                    chunk is not None
                    and chunk
                    and not chunk.endswith("\n".encode())
                ):
                    if __debug__:
                        logger.debug(
                            "Received chunk answer from server with size = %s",
                            str(len(chunk)),
                        )
                    chunk = comm_socket.recv(DistroStreamClient.BUFFER_SIZE)
                    if chunk is not None and chunk:
                        answer = answer + chunk
                answer_str = answer.decode(encoding="UTF-8").strip()
                if __debug__:
                    logger.debug(
                        "Received answer from server: %s", str(answer_str)
                    )
                req.set_response(answer_str)
                return
            except ConnectionRefusedError as exc:
                last_exc = exc
                if __debug__:
                    logger.warning(
                        "DS Server not ready (attempt %d/%d), retrying in %ds",
                        attempt + 1,
                        _MAX_RETRIES,
                        _RETRY_DELAY,
                    )
                if attempt < _MAX_RETRIES - 1:
                    time.sleep(_RETRY_DELAY)
            except Exception as general_exception:
                if __debug__:
                    logger.error(
                        "ERROR: Cannot process request \n %s",
                        str(general_exception),
                    )
                req.set_error(1, str(general_exception))
                return
            finally:
                comm_socket.close()

        # All retries exhausted
        if __debug__:
            logger.error(
                "ERROR: DS Server unreachable after %d attempts: %s",
                _MAX_RETRIES,
                str(last_exc),
            )
        req.set_error(1, str(last_exc))

    def add_request(self, req: typing.Any) -> None:
        """Add a new request to the client.

        :param req: Request to add (Request subclass).
        :return: None.
        """
        if __debug__:
            logger.debug(
                "Adding new request to client queue: %s", str(req.get_type())
            )
        self.requests.put(req, block=True)
