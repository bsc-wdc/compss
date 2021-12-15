#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
==================
    This file tests the Python Streams implementation in PyCOMPSs.
"""

# Imports
from pycompss.api.api import compss_wait_on
from pycompss.streams.distro_stream import PscoDistroStream

from modules.test_tasks import write_pscos
from modules.test_tasks import read_pscos
from modules.test_tasks import process_psco

PRODUCER_SLEEP = 0.2  # s
CONSUMER_SLEEP = 0.1  # s
CONSUMER_SLEEP2 = 0.3  # s
ALIAS = "py_psco_stream"


def test_produce_consume(num_producers, producer_sleep, num_consumers, consumer_sleep):
    # Create stream
    pds = PscoDistroStream(alias=None)

    # Create producers
    for _ in range(num_producers):
        write_pscos(pds, producer_sleep)

    # Create consumers
    total_pscos = []
    for i in range(num_consumers):
        num_pscos = read_pscos(pds, consumer_sleep)
        total_pscos.append(num_pscos)

    # Sync and print value
    total_pscos = compss_wait_on(total_pscos)
    num_total = sum(total_pscos)
    print("[LOG] TOTAL NUMBER OF PROCESSED PSCOS: " + str(num_total))


def test_produce_gen_tasks(num_producers, producer_sleep, consumer_sleep):
    import time

    # Create stream
    pds = PscoDistroStream(alias=None)

    # Create producers
    for _ in range(num_producers):
        write_pscos(pds, producer_sleep)

    # Process stream
    processed_results = []
    while not pds.is_closed():
        # Poll new pscos
        print("Polling pscos")
        new_pscos = pds.poll()

        # Process pscos
        for nf in new_pscos:
            res = process_psco(nf)
            processed_results.append(res)

        # Sleep between requests
        time.sleep(consumer_sleep)

    # Sync and accumulate read pscos
    processed_results = compss_wait_on(processed_results)
    num_total = sum(processed_results)
    print("[LOG] TOTAL NUMBER OF PROCESSED PSCOS: " + str(num_total))


def test_by_alias(num_producers, producer_sleep, num_consumers, consumer_sleep):
    # Create producers
    pds = PscoDistroStream(alias=ALIAS)
    for _ in range(num_producers):
        write_pscos(pds, producer_sleep)

    # Create consumers
    pds2 = PscoDistroStream(alias=ALIAS)
    total_pscos = []
    for i in range(num_consumers):
        num_files = read_pscos(pds2, consumer_sleep)
        total_pscos.append(num_files)

    # Sync and print value
    total_pscos = compss_wait_on(total_pscos)
    num_total = sum(total_pscos)
    print("[LOG] TOTAL NUMBER OF PROCESSED PSCOS: " + str(num_total))


def main_program():
    # 1 producer, 1 consumer, consumerTime < producerTime
    test_produce_consume(1, PRODUCER_SLEEP, 1, CONSUMER_SLEEP)

    # 1 producer, 1 consumer, consumerTime > producerTime
    test_produce_consume(1, PRODUCER_SLEEP, 1, CONSUMER_SLEEP2)

    # 1 producer, 2 consumer
    test_produce_consume(1, PRODUCER_SLEEP, 2, CONSUMER_SLEEP2)

    # 2 producer, 1 consumer
    test_produce_consume(2, PRODUCER_SLEEP, 1, CONSUMER_SLEEP)

    # 2 producer, 2 consumer
    test_produce_consume(2, PRODUCER_SLEEP, 2, CONSUMER_SLEEP)

    # 1 producer, 1 task per entry
    test_produce_gen_tasks(1, PRODUCER_SLEEP, CONSUMER_SLEEP)

    # By alias
    test_by_alias(1, PRODUCER_SLEEP, 1, CONSUMER_SLEEP)


if __name__ == "__main__":
    main_program()
