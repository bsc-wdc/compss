#!/usr/bin/python

# -*- coding: utf-8 -*-

# Imports
import uuid
import time

from pycompss.api.task import task
from pycompss.api.parameter import *

from modules.models import Person

# Constant values
NUM_PSCOS = 10


# Task definitions
@task(pds=STREAM_OUT, sleep=IN)
def write_pscos(pds, sleep):
    for i in range(NUM_PSCOS):
        # Build psco
        p = Person(name=str(uuid.uuid4()), age=i)
        p.makePersistent()

        # Publish element
        pds.publish(p)

        # Sleep between generated files
        time.sleep(sleep)

    # Mark the stream for closure
    pds.close()


@task(pds=STREAM_IN, sleep=IN, returns=int)
def read_pscos(pds, sleep):
    num_total = 0
    while not pds.is_closed():
        # Poll new pscos
        print("Polling pscos")
        new_pscos = pds.poll()

        # Process pscos
        for psco in new_pscos:
            print("RECEIVED PSCO: " + str(psco))

        # Accumulate read pscos
        num_total = num_total + len(new_pscos)

        # Sleep between requests
        time.sleep(sleep)

    # Perform a las poll
    print("Polling pscos")
    new_pscos = pds.poll()
    for psco in new_pscos:
        print("RECEIVED PSCO: " + str(psco))
    num_total = num_total + len(new_pscos)

    # Return the number of processed pscos
    return num_total


@task(psco=IN, returns=int)
def process_psco(psco):
    print("RECEIVED PSCO: " + str(psco))
    return 1
