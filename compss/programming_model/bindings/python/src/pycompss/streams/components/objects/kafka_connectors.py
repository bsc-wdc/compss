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
PyCOMPSs - Streams - Components - Objects.

This file contains the distro stream components objects code.
"""

# Imports
import pickle
import socket

from pycompss.util.typing_helper import typing

#
# Logger definition
#

if __debug__:
    import logging

    logger = logging.getLogger("pycompss.streams.distro_stream")

#
# Globals
#

API_VERSION = (3, 8, 0)

#
# ODSPublisher definition
#


class ODSPublisher:
    """ODS Publisher connector implementation.

    Attributes:
        - kafka_producer: KafkaProducer instance
            + type: KafkaProducer
    """

    def __init__(self, bootstrap_server: str) -> None:
        """Create a new ODSPublisher instance.

        :param bootstrap_server: Associated boostrap server.
        """
        if __debug__:
            logger.debug("Creating Publisher...")

        # Create internal producer
        from kafka3 import KafkaProducer

        bootstrap_server_info = str(bootstrap_server).split(":")
        bootstrap_server_ip = str(
            socket.gethostbyname(bootstrap_server_info[0])
        )
        bootstrap_server_port = str(bootstrap_server_info[1])
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=f"{bootstrap_server_ip}:{bootstrap_server_port}",
            acks="all",
            retries=0,
            batch_size=16384,
            linger_ms=0,
            api_version=API_VERSION,
            max_block_ms=120000,
        )
        # Other flags:
        # auto_commit_interval_ms=2,
        # key_serializer="org.apache.kafka.common.serialization.StringSerializer",  # noqa: E501
        # value_serializer="org.apache.kafka.common.serialization.StringSerializer",  # noqa: E501
        # block_on_buffer_full=True

        if __debug__:
            logger.debug("DONE Creating Publisher")

    def publish(self, topic: typing.Union[bytes, str], message: str) -> None:
        """Publish the given message to the given topic.

        :param topic: Message topic.
        :param message: Message to publish.
        :return: None.
        """
        if __debug__:
            logger.debug("Publishing Message to %s ...", str(topic))

        # Fix topic if required
        if isinstance(topic, bytes):
            topic_fix = str(topic.decode("utf-8"))
        else:
            topic_fix = str(topic)

        # Serialize message
        serialized_message = pickle.dumps(message)

        # Send message
        self.kafka_producer.send(topic_fix, value=serialized_message)
        self.kafka_producer.flush()

        if __debug__:
            logger.debug("DONE Publishing Message")


#
# ODSConsumer definition
#


class ODSConsumer:
    """ODS Consumer connector implementation.

    Attributes:
        - topic: Registered topic name on the Kafka backend
            + type: string
        - access_mode: Consumer access mode
            + type: string
        - kafka_consumer: KafkaConsumer instance
            + type: KafkaConsumer
    """

    def __init__(
        self, bootstrap_server: str, topic: str, access_mode: str
    ) -> None:
        """Create a new ODSConsumer instance.

        :param bootstrap_server: Associated boostrap server.
        :param topic: Topic where to consume records.
        :param access_mode: Consumer access mode.
        """
        if __debug__:
            logger.debug("Creating Consumer...")

        if isinstance(topic, bytes):
            topic_fix = str(topic.decode("utf-8"))  # noqa
        else:
            topic_fix = str(topic)
        self.topic = topic_fix
        self.access_mode = access_mode

        # Parse configuration

        # Create internal consumer
        from kafka3 import KafkaConsumer

        bootstrap_server_info = str(bootstrap_server).split(":")
        bootstrap_server_ip = str(
            socket.gethostbyname(bootstrap_server_info[0])
        )  # noqa: E501
        bootstrap_server_port = str(bootstrap_server_info[1])
        if __debug__:
            logger.debug(
                "Bootstrap server: %s - info: %s - %s:%s",
                bootstrap_server,
                bootstrap_server_info,
                bootstrap_server_ip,
                bootstrap_server_port,
            )
        self.kafka_consumer = KafkaConsumer(
            bootstrap_servers=f"{bootstrap_server_ip}:{bootstrap_server_port}",
            enable_auto_commit=True,
            auto_commit_interval_ms=200,
            group_id=self.topic,
            auto_offset_reset="earliest",
            session_timeout_ms=10000,
            fetch_min_bytes=1,
            receive_buffer_bytes=262144,
            max_partition_fetch_bytes=2097152,
            api_version=API_VERSION,
        )
        # Other flags:
        # key_deserializer="org.apache.kafka.common.serialization.StringSerializer",  # noqa: E501
        # value_deserializer="org.apache.kafka.common.serialization.StringSerializer",  # noqa: E501

        # Subscribe consumer
        self.kafka_consumer.subscribe([self.topic])

        if __debug__:
            logger.debug("DONE Creating Consumer")

    def poll(self, timeout: int) -> list:
        """Poll messages from the subscribed topics.

        :param timeout: Poll timeout.
        :return: List of polled messages (strings - can be empty but not None).
        """
        if __debug__:
            logger.debug("Polling Messages from %s ...", str(self.topic))

        new_messages = []
        # First _ was tp.
        for _, records in self.kafka_consumer.poll(
            timeout_ms=timeout
        ).items():  # noqa: E501
            for record in records:
                if record.topic == self.topic:
                    deserialized_message = pickle.loads(record.value)
                    new_messages.append(deserialized_message)
                else:
                    logger.warning(
                        "Ignoring received message on unregistered topic %s",
                        str(record.topic),
                    )

        if __debug__:
            logger.debug(
                "DONE Polling Messages (%s elements)", str(len(new_messages))
            )

        return new_messages
