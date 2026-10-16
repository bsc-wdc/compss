#!/usr/bin/env python3
#
#  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
Process tree management.

This file defines the utilities to discover and terminate the processes
spawned below a given process. It is used by the workers to kill the
processes that a cancelled task has started.
"""

import logging
import os
import signal
import time

from pycompss.util.typing_helper import typing

try:
    import psutil
except ImportError:
    psutil = None

REAP_INTERVAL = 0.1
KILL_GRACE_PERIOD = 5.0
KILL_RECHECK_PERIOD = 0.5


def _get_children_proc(pid: int) -> list:
    """Get the direct children of a process through /proc.

    Only available on Linux.

    :param pid: Process identifier.
    :return: Direct children pids.
    """
    children: typing.List[int] = []
    try:
        task_ids = os.listdir("/proc/%d/task" % pid)
    except OSError:
        return children
    for task_id in task_ids:
        children_file = "/proc/%d/task/%s/children" % (pid, task_id)
        try:
            with open(children_file) as file_descriptor:
                for token in file_descriptor.read().split():
                    children.append(int(token))
        except (OSError, ValueError):
            continue
    return children


def _get_descendants_proc(pid: int) -> list:
    """Get all the descendants of a process through /proc.

    Only available on Linux.

    :param pid: Process identifier.
    :return: Descendant pids (the root pid is not included).
    """
    descendants = []
    stack = [pid]
    visited = set()
    while stack:
        current = stack.pop()
        for child in _get_children_proc(current):
            if child not in visited:
                visited.add(child)
                descendants.append(child)
                stack.append(child)
    return descendants


def get_descendants(pid: int) -> list:
    """Get all the descendants of a process.

    Uses psutil when available and falls back to /proc otherwise.

    :param pid: Process identifier.
    :return: Descendant pids (the root pid is not included).
    """
    if psutil is not None:
        try:
            process = psutil.Process(pid)
            return [child.pid for child in process.children(recursive=True)]
        except psutil.Error:
            return []
    return _get_descendants_proc(pid)


def _pid_alive(pid: int) -> bool:
    """Check whether a process exists and is not a zombie.

    A zombie process is considered not alive: it has already exited and
    only waits to be reaped by its parent.

    :param pid: Process identifier.
    :return: True if the process exists and is not a zombie.
    """
    if psutil is not None:
        try:
            return psutil.Process(pid).status() != psutil.STATUS_ZOMBIE
        except psutil.NoSuchProcess:
            return False
        except psutil.Error:
            return True
    try:
        with open("/proc/%d/stat" % pid) as stat_file:
            stat_content = stat_file.read()
        state = stat_content.rsplit(")", 1)[1].split()[0]
        return state != "Z"
    except (OSError, IndexError):
        return False


def _reap_direct_children(pids: list) -> list:
    """Reap the dead direct children and collect the alive pids.

    A process killed while its parent is inside a signal handler can not
    disappear from the system until the parent reaps it; without this
    step a dead child would still answer to os.kill(pid, 0). Only the
    direct children of the current process can be reaped; for the rest,
    os.waitpid raises ChildProcessError and they are only checked for
    liveness.

    :param pids: Candidate pids.
    :return: Pids that are still alive.
    """
    alive = []
    for pid in pids:
        reaped = False
        try:
            waited_pid, _ = os.waitpid(pid, os.WNOHANG)
            reaped = waited_pid == pid
        except (ChildProcessError, InterruptedError):
            pass
        if not reaped and _pid_alive(pid):
            alive.append(pid)
    return alive


def _signal_processes(
    pids: list, signum: int, own_pgid: typing.Optional[int]
) -> None:
    """Send a signal to each process, or to its process group.

    The signal is sent to the whole process group when the group leader
    is one of the processes to kill and the group is not the caller's
    one; this covers the children that changed their process group and
    all their descendants. Otherwise the signal is sent to the single
    process. The caller is never signaled.

    :param pids: Process identifiers.
    :param signum: Signal number.
    :param own_pgid: Process group of the caller.
    :return: None
    """
    pid_set = set(pids)
    signaled_groups = set()
    for pid in pids:
        try:
            pgid = os.getpgid(pid)
        except OSError:
            continue
        if (
            pgid != own_pgid
            and pgid in pid_set
            and pgid not in signaled_groups
        ):
            try:
                os.killpg(pgid, signum)
            except OSError:
                continue
            signaled_groups.add(pgid)
        else:
            try:
                os.kill(pid, signum)
            except OSError:
                continue


def kill_process_tree(
    root_pid: typing.Optional[int] = None,
    grace_period: float = KILL_GRACE_PERIOD,
    logger: typing.Optional[logging.Logger] = None,
) -> None:
    """Kill all the processes spawned below a process.

    The root process itself is never signaled, so the caller can be the
    root (an executor process killing the processes of its current task)
    and survive to run further tasks. The descendants receive SIGTERM
    first and SIGKILL if they do not exit within the grace period. The
    direct children of the caller are reaped so that they do not remain
    as zombies.

    :param root_pid: Root process identifier (default: current process).
    :param grace_period: Seconds to wait after SIGTERM before SIGKILL.
    :param logger: Logger.
    :return: None
    """
    if logger is None:
        logger = logging.getLogger("PYCOMPSs")
    if root_pid is None:
        root_pid = os.getpid()
    try:
        own_pgid = os.getpgid(0)
    except OSError:
        own_pgid = None
    pids = [pid for pid in get_descendants(root_pid) if pid != root_pid]
    if not pids:
        return
    logger.info(
        "Killing process tree below %d (pids: %s)",
        root_pid,
        ",".join(str(pid) for pid in pids),
    )
    _signal_processes(pids, signal.SIGTERM, own_pgid)
    deadline = time.time() + grace_period
    alive = pids
    while alive and time.time() < deadline:
        time.sleep(REAP_INTERVAL)
        alive = _reap_direct_children(alive)
    if alive:
        logger.info(
            "Processes %s did not exit, sending SIGKILL",
            ",".join(str(pid) for pid in alive),
        )
        _signal_processes(alive, signal.SIGKILL, own_pgid)
        deadline = time.time() + KILL_RECHECK_PERIOD
        while alive and time.time() < deadline:
            time.sleep(REAP_INTERVAL)
            alive = _reap_direct_children(alive)
    if alive:
        logger.warning(
            "Processes %s could not be killed",
            ",".join(str(pid) for pid in alive),
        )
