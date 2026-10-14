#!/usr/bin/env python3

"""PyCOMPSs multi-node test: 8 tasks must run on both remote workers."""

import socket
import sys

from pycompss.api.api import compss_wait_on
from pycompss.api.task import task


@task(returns=1)
def whereami():
    return socket.gethostname()


def main():
    futures = [whereami() for _ in range(8)]
    hosts = compss_wait_on(futures)
    print("Task hostnames: %s" % sorted(set(hosts)))
    if "COMPSsRemote01" in hosts and "COMPSsRemote02" in hosts:
        print("MULTINODE HELLO OK")
        return 0
    print("MULTINODE HELLO FAIL")
    return 1


if __name__ == "__main__":
    sys.exit(main())
