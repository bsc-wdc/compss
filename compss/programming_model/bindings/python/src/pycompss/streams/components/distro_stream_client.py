#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import unittest
import logging
import socket
from threading import Thread

try:
    # Python 3
    import queue
except ImportError:
    # Python 2
    import Queue as queue

# Project imports
from pycompss.streams.types.requests import STOP
from pycompss.streams.types.requests import StopRequest

#
# Logger definition
#

logger = logging.getLogger("pycompss.streams.distro_stream_client")


#
# Client Handler
#

class DistroStreamClientHandler:
    """
    Handler to use the DistroStreamClient. This is a static class.

    Attributes:
        - CLIENT: Distributed Stream Client to handle.
            + type: DistroStreamClient
    """

    CLIENT = None

    def __init__(self):
        """
        Creates a new handler instance. Should never be called directly since all attributes are static.
        """
        # Nothing to do since this is a static handler
        pass

    @staticmethod
    def init_and_start(master_ip=None, master_port=None):
        """
        Initializes and starts the client.

        :param master_ip: Master IP.
            + type: string
        :param master_port: Master port.
            + type: int
        :return: None.
        """
        DistroStreamClientHandler.CLIENT = DistroStreamClient(master_ip=master_ip, master_port=master_port)
        DistroStreamClientHandler.CLIENT.start()

    @staticmethod
    def set_stop():
        """
        Marks the client to stop.

        :return: None.
        """
        req = StopRequest()
        DistroStreamClientHandler.CLIENT.add_request(req)

    @staticmethod
    def request(req):
        """
        Adds a new request to the client.

        :param req: Client request.
            + type: Subclass of Request
        :return: None.
        """
        DistroStreamClientHandler.CLIENT.add_request(req)


#
# Client definition
#
class DistroStreamClient(Thread):
    """
    Distro Stream Client definition.

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

    def __init__(self, master_ip=None, master_port=None):
        """
        Creates a new Client associated to the given master properties.

        :param master_ip: Master IP address.
            + type: string
        :param master_port: Master port.
            + type: int
        """
        super(DistroStreamClient, self).__init__()

        logger.info("Initializing DS Client on " + str(master_ip) + ":" + str(master_port))

        # Register information
        self.master_ip = master_ip
        self.master_port = master_port

        # Initialize internal structures
        self.running = True
        self.requests = queue.Queue()

    def run(self):
        """
        Running method of the internal thread.

        :return: None.
        """
        logger.info("DS Client started")

        while self.running:
            # Process requests
            req = self.requests.get(block=True)

            if __debug__:
                logger.debug("Processing request: " + str(req.get_type()))

            if req.get_type() == STOP:
                logger.info("DS Client asked to stop")
                self.running = False
                req.set_response("DONE")
            else:
                self._process_request(req)

            # Mark the request as processed
            req.set_processed()
            self.requests.task_done()

        logger.info("DS Client stopped")

    def _process_request(self, req):
        # Process requests to the server

        # Open socket connection
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.master_ip, self.master_port))

            # Send request message
            req_msg = req.get_request_msg()
            if __debug__:
                logger.debug("Sending request to server: " + str(req_msg))
            req_msg = req_msg + "\n"
            req_msg = req_msg.encode()
            s.sendall(req_msg)

            # Receive answer
            chunk = s.recv(DistroStreamClient.BUFFER_SIZE)
            answer = chunk
            while chunk is not None and chunk and not chunk.endswith("\n".encode()):
                if __debug__:
                    logger.debug("Received chunk answer from server with size = " + str(len(chunk)))
                chunk = s.recv(DistroStreamClient.BUFFER_SIZE)
                if chunk is not None and chunk:
                    answer = answer + chunk
            answer = answer.decode(encoding='UTF-8').strip()
            if __debug__:
                logger.debug("Received answer from server: " + str(answer))
            req.set_response(answer)
        except Exception as e:
            logger.error("ERROR: Cannot process request" + str(e))
            # Some error occurred, mark request as failed and keep going
            req.set_error(1, str(e))

    def add_request(self, req):
        """
        Adds a new request to the client.

        :param req: Request to add.
            + type: Request subclass.
        :return: None.
        """
        if __debug__:
            logger.debug("Adding new request to client queue: " + str(req.get_type()))

        self.requests.put(req, block=True)


#
# UNIT TEST CASES
#

class TestDistroStreamClient(unittest.TestCase):

    def disabled_test_client(self):
        """
        Tests the client. Requires a valid server into the given master_port.

        :return: None.
        """
        # Master port
        master_port = 49049

        # Imports
        import uuid

        from pycompss.streams.types.requests import RegisterStreamRequest
        from pycompss.streams.types.requests import StreamStatusRequest
        from pycompss.streams.types.requests import CloseStreamRequest
        from pycompss.streams.distro_stream import FILE
        from pycompss.streams.distro_stream import AT_MOST_ONCE

        try:
            # Start
            print("Start")
            DistroStreamClientHandler.init_and_start(master_ip="localhost", master_port=master_port)

            # Send request
            print("Send request")
            req = RegisterStreamRequest(None, FILE, AT_MOST_ONCE, ["/tmp/file_stream_python/"])
            DistroStreamClientHandler.request(req)

            # Wait response
            print("Wait response")
            req.wait_processed()

            # Process response
            print("Received: ")
            print(" - Error Code    : " + str(req.get_error_code()))
            print(" - Error Message : " + str(req.get_error_msg()))
            print(" - Response      : " + str(req.get_response_msg()))
            stream_id = uuid.UUID(req.get_response_msg())

            # Send request
            print("Send request")
            req = StreamStatusRequest(stream_id)
            DistroStreamClientHandler.request(req)

            # Wait response
            print("Wait response")
            req.wait_processed()

            # Process response
            print("Received: ")
            print(" - Error Code    : " + str(req.get_error_code()))
            print(" - Error Message : " + str(req.get_error_msg()))
            print(" - Response      : " + str(req.get_response_msg()))

            # Send request
            print("Send request")
            req = CloseStreamRequest(stream_id)
            DistroStreamClientHandler.request(req)

            # Wait response
            print("Wait response")
            req.wait_processed()

            # Process response
            print("Received: ")
            print(" - Error Code    : " + str(req.get_error_code()))
            print(" - Error Message : " + str(req.get_error_msg()))
            print(" - Response      : " + str(req.get_response_msg()))
        finally:
            DistroStreamClientHandler.set_stop()

    def test_client_handler(self):
        """
        Tests the client handler with two different processes.

        :return:
        """

        def runner():
            print("Starting process")
            print("Init Client Handler")
            DistroStreamClientHandler.init_and_start()
            print("Stop Client Handler")
            DistroStreamClientHandler.set_stop()
            print("End process")

        from multiprocessing import Process
        p1 = Process(target=runner)
        p2 = Process(target=runner)

        p1.start()
        p2.start()

        p1.join()
        p2.join()


#
# MAIN FOR UNIT TEST
#

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s | %(levelname)s | %(name)s - %(message)s')
    unittest.main()
