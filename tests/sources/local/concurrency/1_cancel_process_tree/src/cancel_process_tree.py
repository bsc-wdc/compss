#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs cancel process tree test
=================================
    Checks that cancelling a task group kills the processes that the
    cancelled tasks started and that the tasks' computing units are
    released. The master and the workers share neither a pid namespace
    nor a temporary filesystem, so the spawned processes are observed
    over TCP.
    CHECK 1: each blocking task spawns a child process that connects to
    a listener on the master and sleeps; the app cancels the group; the
    children must be gone within a few seconds after the cancellation
    (their connections to the master close).
    CHECK 2: after the cancellation, short tasks that need whole
    workers (CU_PER_TASK computing units each) finish within a limit
    that a leaked computing unit would exceed, and at least two of them
    run at the same time (one per remote worker) on both remote hosts,
    which proves that the cancelled tasks released their computing
    units.
"""

import socket
import subprocess
import sys
import time

from pycompss.api.api import TaskGroup, compss_cancel_group, compss_wait_on
from pycompss.api.constraint import constraint
from pycompss.api.task import task

# Computing units per task: the whole worker. Each ComputeNode of the
# test's resources.xml declares 4 computing units, and this constant
# matches that declaration (it is not read from any environment
# variable).
CU_PER_TASK = 4
# The two remote workers that the docker_test harness provides
REMOTE_WORKER_1 = "COMPSsRemote01"
REMOTE_WORKER_2 = "COMPSsRemote02"
# One blocking task per worker fills all the computing units
BLOCKING_TASKS = 2
SHORT_TASKS = 6
SHORT_DURATION = 3
CHILD_SLEEP = 300
CONNECT_TIMEOUT = 60
DEATH_TIMEOUT = 10
SHORT_TASKS_LIMIT = 60
MAX_CONCURRENCY = 2
GROUP_NAME = "blockingGroup"
CHILD_PORT = 18091


@constraint(computing_units=str(CU_PER_TASK))
@task(returns=1)
def blocking_task(master_host, port):
    """Blocking task: spawn a child process and wait for it.

    The child connects to the master's listener and sleeps; its death
    closes the connection, which is how the master checks that the
    process tree kill reached it without sharing a pid namespace with
    the workers.

    :param master_host: Host of the master's listener.
    :param port: Port of the master's listener.
    :return: 'blocking' (never returned, the task gets cancelled).
    """
    child_code = (
        "import socket, time\n"
        "s = socket.create_connection(({!r}, {}))\n"
        "time.sleep({})\n".format(master_host, port, CHILD_SLEEP)
    )
    child = subprocess.Popen([sys.executable, "-c", child_code])
    child.wait()
    return "blocking"


@constraint(computing_units=str(CU_PER_TASK))
@task(returns=1)
def short_task():
    """Short task: sleep SHORT_DURATION and return its times and host.

    :return: ('short', start, end, hostname).
    """
    start = time.time()
    time.sleep(SHORT_DURATION)
    end = time.time()
    return ("short", start, end, socket.gethostname())


def max_concurrency(intervals):
    """
    Computes the maximum number of overlapping [start, end) intervals.

    :param intervals: List of (start, end) tuples
        + type: list<tuple<float, float>>
    :return: Maximum number of concurrent intervals
        + type: int
    """
    events = []
    for start, end in intervals:
        events.append((start, 1))
        events.append((end, -1))
    events.sort()
    current = 0
    maximum = 0
    for _, delta in events:
        current += delta
        if current > maximum:
            maximum = current
    return maximum


def accept_children(listener):
    """Accepts the connections of the blocking tasks' children.

    :param listener: Master's TCP listener.
    :return: Accepted connections
        + type: list<socket>
    """
    connections = []
    deadline = time.time() + CONNECT_TIMEOUT
    while len(connections) < BLOCKING_TASKS:
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        listener.settimeout(remaining)
        try:
            connection, _ = listener.accept()
        except (socket.timeout, OSError):
            break
        connections.append(connection)
    return connections


def check_killed_processes(connections):
    """
    CHECK 1: the processes spawned by the cancelled tasks are gone
    DEATH_TIMEOUT seconds after the cancellation at the latest. The
    master reads from each connection that a child opened when its task
    started; the read returns end of stream when the runtime has killed
    the child, and times out while the child is still alive.

    :param connections: Connections of the blocking tasks' children.
    :return: True if the check passes
        + type: bool
    """
    if len(connections) < BLOCKING_TASKS:
        print(
            "CHECK 1 FAIL: only {} of {} children connected to port {} "
            "in {} s".format(
                len(connections), BLOCKING_TASKS, CHILD_PORT, CONNECT_TIMEOUT
            )
        )
        return False
    for connection in connections:
        connection.settimeout(DEATH_TIMEOUT)
    deadline = time.time() + DEATH_TIMEOUT
    try:
        for connection in connections:
            remaining = deadline - time.time()
            if remaining > 0:
                connection.settimeout(remaining)
            try:
                data = connection.recv(1)
            except socket.timeout:
                print(
                    "CHECK 1 FAIL: a child of a cancelled task is still "
                    "alive {} s after cancellation "
                    "(connection still open)".format(DEATH_TIMEOUT)
                )
                return False
            if data != b"":
                print(
                    "CHECK 1 FAIL: a child of a cancelled task sent "
                    "unexpected data {!r}".format(data)
                )
                return False
    finally:
        for connection in connections:
            connection.close()
    print(
        "CHECK 1 OK: the {} children of the cancelled tasks are no "
        "longer alive (connections closed after cancellation)".format(
            len(connections)
        )
    )
    return True


def check_cores_freed():
    """
    CHECK 2: SHORT_TASKS short tasks that need whole workers
    (CU_PER_TASK computing units each) finish in less than
    SHORT_TASKS_LIMIT seconds, at least MAX_CONCURRENCY of them run at
    the same time, and they run on both remote workers
    (socket.gethostname() of the workers), which proves that the
    cancelled tasks released their computing units (a leaked computing
    unit would leave at most one worker able to run them).

    :return: True if the check passes
        + type: bool
    """
    start_time = time.time()
    futures = [short_task() for _ in range(SHORT_TASKS)]
    results = compss_wait_on(futures)
    elapsed = time.time() - start_time
    concurrency = max_concurrency([(r[1], r[2]) for r in results])
    values_ok = all(r[0] == "short" for r in results)
    hosts = sorted(set(r[3] for r in results))
    hosts_ok = hosts == [REMOTE_WORKER_1, REMOTE_WORKER_2]
    if (
        values_ok
        and hosts_ok
        and elapsed < SHORT_TASKS_LIMIT
        and concurrency >= MAX_CONCURRENCY
    ):
        print(
            "CHECK 2 OK: {} short tasks in {:.1f} s (limit {} s), {} at "
            "once, on hosts {}".format(SHORT_TASKS, elapsed,
                                       SHORT_TASKS_LIMIT, concurrency,
                                       hosts)
        )
        return True
    print(
        "CHECK 2 FAIL: values_ok {}, hosts_ok {} ({}), elapsed {:.1f} s "
        "(limit {} s), max concurrency {} (expected at least {})".format(
            values_ok, hosts_ok, hosts, elapsed, SHORT_TASKS_LIMIT,
            concurrency, MAX_CONCURRENCY
        )
    )
    return False


def main():
    master_host = socket.gethostname()
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("", CHILD_PORT))
    listener.listen(BLOCKING_TASKS)

    # Submit the blocking tasks inside a group without implicit barrier
    # so that the app can cancel them while they are running
    with TaskGroup(GROUP_NAME, False):
        for _ in range(BLOCKING_TASKS):
            blocking_task(master_host, CHILD_PORT)

    # Wait until every blocking task has started (its child has
    # connected to the master's listener)
    connections = accept_children(listener)
    listener.close()

    # Cancel the group: the running tasks are killed
    compss_cancel_group(GROUP_NAME)

    checks_ok = check_killed_processes(connections)
    checks_ok = check_cores_freed() and checks_ok

    if checks_ok:
        print("CANCEL_PROCESS_TREE RESULT OK")
    else:
        print("CANCEL_PROCESS_TREE RESULT FAIL")

    print("------------------------------------------------------------")

    if not checks_ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
