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
import logging

#
# Logger definition
#

logger = logging.getLogger("pycompss.streams.distro_stream")


#
# ODSPublisher definition
#

class ODSPublisher(object):
    """
    ODS Publisher connector implementation.

    Attributes:
        - kafka_producer: KafkaProducer instance
            + type: KafkaProducer
    """

    def __init__(self, bootstrap_server):
        """
        Creates a new ODSPublisher instance

        :param bootstrap_server: Associated boostrap server
            + type: string
        """
        logger.debug("Creating Publisher...")

        # Parse configuration file

        # Create internal producer
        import socket
        from kafka import KafkaProducer

        bootstrap_server_info = str(bootstrap_server).split(":")
        bootstrap_server_ip = str(socket.gethostbyname(bootstrap_server_info[0]))  # noqa: E501
        bootstrap_server_port = str(bootstrap_server_info[1])
        self.kafka_producer = KafkaProducer(bootstrap_servers=bootstrap_server_ip + ":" + bootstrap_server_port,  # noqa: E501
                                            acks="all",
                                            retries=0,
                                            batch_size=16384,
                                            # auto_commit_interval_ms=2,
                                            linger_ms=0,
                                            # key_serializer="org.apache.kafka.common.serialization.StringSerializer",  # noqa: E501
                                            # value_serializer="org.apache.kafka.common.serialization.StringSerializer",  # noqa: E501
                                            # block_on_buffer_full=True
                                            )

        logger.debug("DONE Creating Publisher")

    def publish(self, topic, message):
        """
        Publishes the given message to the given topic.

        :param topic: Message topic.
            + type: string
        :param message: Message to publish.
            + type: string
        :return: None
        """
        if __debug__:
            logger.debug("Publishing Message to " + str(topic) + " ...")

        # Fix topic if required
        if type(topic) == bytes:
            topic = topic.decode('utf-8')
        else:
            topic = str(topic)

        # Serialize message
        import pickle
        serialized_message = pickle.dumps(message)

        # Send message
        self.kafka_producer.send(topic, value=serialized_message)
        self.kafka_producer.flush()
        logger.debug("DONE Publishing Message")


#
# ODSConsumer definition
#

class ODSConsumer(object):
    """
    ODS COnsumer connector implementation.

    Attributes:
        - topic: Registered topic name on the Kafka backend
            + type: string
        - access_mode: Consumer access mode
            + type: string
        - kafka_consumer: KafkaConsumer instance
            + type: KafkaConsumer
    """

    def __init__(self, bootstrap_server, topic, access_mode):
        """
        Creates a new ODSConsumer instance.

        :param bootstrap_server: Associated boostrap server.
            + type: string
        :param topic: Topic where to consume records.
            + type: string
        :param access_mode: Consumer access mode.
            + type: string
        """
        logger.debug("Creating Consumer...")

        if type(topic) == bytes:
            self.topic = topic.decode('utf-8')
        else:
            self.topic = str(topic)
        self.access_mode = access_mode

        # Parse configuration

        # Create internal consumer
        import socket
        from kafka import KafkaConsumer

        bootstrap_server_info = str(bootstrap_server).split(":")
        bootstrap_server_ip = str(socket.gethostbyname(bootstrap_server_info[0]))  # noqa: E501
        bootstrap_server_port = str(bootstrap_server_info[1])
        self.kafka_consumer = KafkaConsumer(bootstrap_servers=bootstrap_server_ip + ":" + bootstrap_server_port,  # noqa: E501
                                            enable_auto_commit=True,
                                            auto_commit_interval_ms=200,
                                            group_id=self.topic,
                                            # key_deserializer="org.apache.kafka.common.serialization.StringSerializer",  # noqa: E501
                                            # value_deserializer="org.apache.kafka.common.serialization.StringSerializer",  # noqa: E501
                                            auto_offset_reset="earliest",
                                            session_timeout_ms=10000,
                                            fetch_min_bytes=1,
                                            receive_buffer_bytes=262144,
                                            max_partition_fetch_bytes=2097152
                                            )

        # Subscribe consumer
        self.kafka_consumer.subscribe([self.topic])

        logger.debug("DONE Creating Consumer")

    def poll(self, timeout):
        """
        Polls messages from the subscribed topics.

        :param timeout: Poll timeout
            + type: int
        :return: List of polled messages (can be empty but not None).
            + type: List<String>
        """
        if __debug__:
            logger.debug("Polling Messages from " + str(self.topic) + " ...")

        import pickle
        new_messages = []
        for tp, records in self.kafka_consumer.poll(timeout_ms=timeout).items():  # noqa: E501
            for record in records:
                if record.topic == self.topic:
                    deserialized_message = pickle.loads(record.value)
                    new_messages.append(deserialized_message)
                else:
                    logger.warn("Ignoring received message on unregistered topic " + str(record.topic))  # noqa: E501

        if __debug__:
            logger.debug("DONE Polling Messages (" + str(len(new_messages)) + " elements)")  # noqa: E501

        return new_messages
