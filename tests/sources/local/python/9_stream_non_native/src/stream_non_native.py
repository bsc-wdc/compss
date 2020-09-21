#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
==================
    This file tests the Python Streams implementation in PyCOMPSs using non-native tasks.
"""

# Imports
from pycompss.api.api import compss_wait_on
from pycompss.streams.distro_stream import FileDistroStream

from modules.test_tasks import write_files
from modules.test_tasks import read_files
from modules.test_tasks import TEST_PATH

PRODUCER_SLEEP = 1  # s
CONSUMER_SLEEP = 0.5  # s


def create_folder(folder):
    import os
    os.mkdir(folder)


def clean_folder(folder):
    import shutil
    shutil.rmtree(folder, ignore_errors=True)


def test_produce_consume(num_producers, producer_sleep, num_consumers, consumer_sleep):
    # Clean and create test path
    clean_folder(TEST_PATH)
    create_folder(TEST_PATH)

    # Create stream
    fds = FileDistroStream(base_dir=TEST_PATH)

    # Create producers
    for _ in range(num_producers):
        write_files(fds, producer_sleep)

    # Create consumers
    total_files = []
    for i in range(num_consumers):
        num_files = read_files(fds, consumer_sleep)
        total_files.append(num_files)

    # Sync and print value
    total_files = compss_wait_on(total_files)
    num_total = sum(total_files)
    print("[LOG] TOTAL NUMBER OF PROCESSED FILES: " + str(num_total))


def main_program():
    # 1 producer, 1 consumer, consumerTime < producerTime
    test_produce_consume(1, PRODUCER_SLEEP, 1, CONSUMER_SLEEP)

    # Clean folder
    clean_folder(TEST_PATH)


if __name__ == "__main__":
    main_program()
