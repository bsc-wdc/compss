#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
==================
    This file tests the Python OBJECT Streams implementation in PyCOMPSs.
"""

# Imports
from pycompss.api.api import compss_wait_on
from pycompss.streams.distro_stream import ObjectDistroStream

from modules.test_tasks import write_objects
from modules.test_tasks import read_objects
from modules.test_tasks import process_object

PRODUCER_SLEEP = 0.2  # s
CONSUMER_SLEEP = 0.1  # s
CONSUMER_SLEEP2 = 0.3  # s
ALIAS = "py_objects_stream"


def test_produce_consume(num_producers, producer_sleep, num_consumers, consumer_sleep):
    # Create stream
    ods = ObjectDistroStream()

    # Create producers
    for _ in range(num_producers):
        write_objects(ods, producer_sleep)

    # Create consumers
    total_objects = []
    for i in range(num_consumers):
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
        print("Polling objects")
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
    for i in range(num_consumers):
        num_objects = read_objects(ods2, consumer_sleep)
        total_objects.append(num_objects)

    # Sync and print value
    total_objects = compss_wait_on(total_objects)
    num_total = sum(total_objects)
    print("[LOG] TOTAL NUMBER OF PROCESSED OBJECTS: " + str(num_total))


def main_program():
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


if __name__ == "__main__":
    main_program()
