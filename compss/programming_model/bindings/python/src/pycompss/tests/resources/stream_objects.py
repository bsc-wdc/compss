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

import time
# Imports
import uuid

from pycompss.api.api import compss_wait_on
from pycompss.api.parameter import IN
from pycompss.api.parameter import STREAM_IN
from pycompss.api.parameter import STREAM_OUT
from pycompss.api.task import task
from pycompss.streams.distro_stream import ObjectDistroStream

# Constant values
NUM_OBJECTS = 10
PRODUCER_SLEEP = 0.2  # s
CONSUMER_SLEEP = 0.1  # s
CONSUMER_SLEEP2 = 0.3  # s
ALIAS = "py_objects_stream"
POLLING_OBJECTS = "Polling objects"


#########
# Model #
#########


class MyObject(object):
    def __init__(self, name=None, age=None):
        self.name = name
        self.age = age

    def get(self):
        return self.age

    def put(self, age):
        self.age = age


####################
# Task definitions #
####################


@task(ods=STREAM_OUT, sleep=IN)
def write_objects(ods, sleep):
    for i in range(NUM_OBJECTS):
        # Build object
        obj = MyObject(name=str(uuid.uuid4()), age=i)

        # Publish object
        ods.publish(obj)

        # Sleep between generated files
        time.sleep(sleep)

    # Mark the stream for closure
    ods.close()


@task(ods=STREAM_IN, sleep=IN, returns=int)
def read_objects(ods, sleep):
    num_total = 0
    while not ods.is_closed():
        # Poll new objects
        print(POLLING_OBJECTS)
        new_objects = ods.poll()

        # Process files
        for obj in new_objects:
            print("RECEIVED OBJECT: " + str(obj))

        # Accumulate read files
        num_total = num_total + len(new_objects)

        # Sleep between requests
        time.sleep(sleep)

    # Read one last time
    print(POLLING_OBJECTS)
    new_objects = ods.poll()
    for obj in new_objects:
        print("RECEIVED OBJECT: " + str(obj))
    num_total = num_total + len(new_objects)

    # Return the number of processed files
    return num_total


@task(returns=int)
def process_object(obj):
    print("RECEIVED OBJECT: " + str(obj))
    return 1


#########
# TESTS #
#########


def test_produce_consume(num_producers, producer_sleep, num_consumers, consumer_sleep):
    # Create stream
    ods = ObjectDistroStream()

    # Create producers
    for _ in range(num_producers):
        write_objects(ods, producer_sleep)

    # Create consumers
    total_objects = []
    for _ in range(num_consumers):
        num_objects = read_objects(ods, consumer_sleep)
        total_objects.append(num_objects)

    # Sync and print value
    print("Wait for total objects")
    total_objects = compss_wait_on(total_objects)
    num_total = sum(total_objects)
    print("[LOG] TOTAL NUMBER OF PROCESSED OBJECTS: " + str(num_total))


def test_produce_gen_tasks(num_producers, producer_sleep, consumer_sleep):
    import time

    # Create stream
    ods = ObjectDistroStream()

    # Create producers
    for _ in range(num_producers):
        write_objects(ods, producer_sleep)

    # Process stream
    processed_results = []
    while not ods.is_closed():
        # Poll new objects
        print(POLLING_OBJECTS)
        new_objects = ods.poll()

        # Process files
        for obj in new_objects:
            res = process_object(obj)
            processed_results.append(res)

        # Sleep between requests
        time.sleep(consumer_sleep)

    # Sync and accumulate read files
    print("Wait for processed objects")
    processed_results = compss_wait_on(processed_results)
    num_total = sum(processed_results)
    print("[LOG] TOTAL NUMBER OF PROCESSED OBJECTS: " + str(num_total))


def test_by_alias(num_producers, producer_sleep, num_consumers, consumer_sleep):
    # Create producers
    ods = ObjectDistroStream(alias=ALIAS)
    for _ in range(num_producers):
        write_objects(ods, producer_sleep)

    # Create consumers
    ods2 = ObjectDistroStream(alias=ALIAS)
    total_objects = []
    for _ in range(num_consumers):
        num_objects = read_objects(ods2, consumer_sleep)
        total_objects.append(num_objects)

    # Sync and print value
    total_objects = compss_wait_on(total_objects)
    num_total = sum(total_objects)
    print("[LOG] TOTAL NUMBER OF PROCESSED OBJECTS: " + str(num_total))


def main():
    # 1 producer, 1 consumer, consumerTime < producerTime
    print("TEST 1 PRODUCER 1 CONSUMER <")
    test_produce_consume(1, PRODUCER_SLEEP, 1, CONSUMER_SLEEP)

    # 1 producer, 1 consumer, consumerTime > producerTime
    print("TEST 1 PRODUCER 1 CONSUMER >")
    test_produce_consume(1, PRODUCER_SLEEP, 1, CONSUMER_SLEEP2)

    # TODO: Enhance test when python-kafka is stable

    # 1 producer, 2 consumer
    # print("TEST 1 PRODUCER 2 CONSUMER")
    # test_produce_consume(1, PRODUCER_SLEEP, 2, CONSUMER_SLEEP2)

    # 2 producer, 1 consumer
    # print("TEST 2 PRODUCER 1 CONSUMER")
    # test_produce_consume(2, PRODUCER_SLEEP, 1, CONSUMER_SLEEP)

    # 2 producer, 2 consumer
    # print("TEST 2 PRODUCER 2 CONSUMER")
    # test_produce_consume(2, PRODUCER_SLEEP, 2, CONSUMER_SLEEP)

    # 1 producer, 1 task per entry
    print("TEST CONSUME PER ENTRY")
    test_produce_gen_tasks(1, PRODUCER_SLEEP, CONSUMER_SLEEP)

    # By alias
    print("TEST BY ALIAS")
    test_by_alias(1, PRODUCER_SLEEP, 1, CONSUMER_SLEEP)


# Uncomment for command line check:
# if __name__ == '__main__':
#     main()
