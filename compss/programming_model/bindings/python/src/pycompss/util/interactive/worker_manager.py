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

"""Utilities for discovering and restarting persistent worker processes."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json
import subprocess
import time

import psutil


@dataclass(frozen=True, slots=True)
class WorkerInfo:
    """Represents a discovered worker process.

    Attributes:
        pid: Process identifier of the worker when it was discovered.
        command: Command and arguments used to launch the worker.
    """

    pid: int
    command: tuple[str, ...]
    env: dict[str, str]
    cwd: str

    def to_dict(self) -> dict[str, Any]:
        """Serialize the worker information.

        Returns:
            JSON-serializable representation of the worker.
        """
        return {
            "pid": self.pid,
            "command": list(self.command),
            "env": self.env,
            "cwd": self.cwd,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkerInfo:
        """Create a :class:`WorkerInfo` from serialized data.

        Args:
            data: Dictionary previously produced by :meth:`to_dict`.

        Returns:
            A new :class:`WorkerInfo` instance.
        """
        return cls(
            pid=int(data["pid"]),
            command=tuple(data["command"]),
            env=dict(data.get("env", {})),
            cwd=str(data["cwd"]),
        )


class WorkerManager:
    """Discovers, persists and restarts worker processes."""

    def __init__(
        self,
        script_name: str,
        state_file: Path,
    ) -> None:
        """Initialize the manager.

        Args:
            script_name: Name or path of the worker launcher script.
            state_file: JSON file used to persist the discovered workers.
                If omitted, a file under the system temporary directory is
                used.
        """
        self._script_name = Path(script_name).name
        self._state_file = state_file
        self._workers: list[WorkerInfo] = []
        self._load()

    @property
    def workers(self) -> list[WorkerInfo]:
        """Returns the cached workers.

        Returns:
            A copy of the cached workers.
        """
        return self._workers.copy()

    def discover(self) -> list[WorkerInfo]:
        """Discover all matching worker processes.

        Every running process whose command line contains the configured
        script name is cached and persisted.

        The current Python process is ignored.

        Returns:
            List of discovered workers.
        """
        self._workers.clear()
        current_pid = psutil.Process().pid
        for proc in psutil.process_iter(["pid", "ppid", "cmdline"]):
            try:
                pid = proc.info["pid"]
                cmdline = proc.info["cmdline"]
                if pid == current_pid or not cmdline:
                    continue
                if Path(cmdline[0]).name != "srun":
                    continue
                parent = proc.parent()
                if parent:
                    try:
                        parent_cmdline = parent.cmdline()
                        if (
                            parent_cmdline
                            and Path(parent_cmdline[0]).name == "srun"
                        ):
                            continue
                    except psutil.Error:
                        pass
                if not any(
                    Path(argument).name == self._script_name
                    for argument in cmdline
                ):
                    continue
                try:
                    env = proc.environ()
                except psutil.AccessDenied:
                    env = {}
                cwd = proc.cwd()
                self._workers.append(
                    WorkerInfo(
                        pid=pid,
                        command=tuple(cmdline),
                        env=env,
                        cwd=cwd,
                    )
                )
            except (
                psutil.NoSuchProcess,
                psutil.AccessDenied,
                psutil.ZombieProcess,
            ):
                continue
        self._save()
        return self.workers

    def restart(self) -> list[subprocess.Popen[str]]:
        """Restart every cached worker.

        Workers are started asynchronously in their own session.

        Returns:
            The created ``subprocess.Popen`` instances.
        """
        processes: list[subprocess.Popen[str]] = []
        for worker in self._workers:
            print(worker.command)
            proc = subprocess.Popen(
                worker.command,
                start_new_session=True,
                env=worker.env,
                cwd=worker.cwd,
                text=True,
            )
            time.sleep(1)
            print("Return code:", proc.poll())
            if proc.poll() is not None:
                if proc.stdout is not None:
                    print(proc.stdout.read())
                if proc.stderr is not None:
                    print(proc.stderr.read())
            processes.append(proc)
        return processes

    def is_running(self, worker: WorkerInfo) -> bool:
        """Check whether a cached worker is still running.

        Args:
            worker: Worker to check.

        Returns:
            ``True`` if the worker PID currently exists.
        """
        return psutil.pid_exists(worker.pid)

    def clear(self) -> None:
        """Remove all cached workers and deletes the persisted state."""
        self._workers.clear()
        if self._state_file.exists():
            self._state_file.unlink()

    def __len__(self) -> int:
        """Return the number of cached workers."""
        return len(self._workers)

    def __bool__(self) -> bool:
        """Return whether any workers are currently cached."""
        return bool(self._workers)

    def _save(self) -> None:
        """Persist the cached workers to disk."""
        self._state_file.parent.mkdir(parents=True, exist_ok=True)
        data = [worker.to_dict() for worker in self._workers]
        self._state_file.write_text(
            json.dumps(data, indent=2),
            encoding="utf-8",
        )

    def _load(self) -> None:
        """Load cached workers from disk."""
        if not self._state_file.exists():
            return
        try:
            data = json.loads(self._state_file.read_text(encoding="utf-8"))
            self._workers = [WorkerInfo.from_dict(item) for item in data]
        except json.JSONDecodeError:
            self._workers = []
