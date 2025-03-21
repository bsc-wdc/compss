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
PyCOMPSs DDS - Partition generators.

Partitions should be loaded with data in the last step only. Thus, generator
objects are necessary. Inside 'task' functions we will call their 'generate'
method in order to retrieve the partition. These partitions can previously be
loaded on master and sent to workers, or read from files on worker nodes.
"""

import pickle
import sys
from pycompss.util.exceptions import DDSException


class IPartitionGenerator:  # pylint: disable=too-few-public-methods
    """Everyone implements this."""

    def retrieve_data(self):
        """Retrieve data.

        :raises NotImplementedError: Not implemented function.
        """
        raise NotImplementedError


class BasicDataLoader(IPartitionGenerator):  # pylint: disable=R0903
    """Basic data loader."""

    def __init__(self, data):
        """Create a new BasicDataLoader object.

        :param data: Data.
        :returns: None.
        """
        super().__init__()
        self.data = data

    def retrieve_data(self):
        """Retrieve data.

        :returns: Data.
        """
        ret = []
        if isinstance(self.data, list):
            ret.extend(self.data)
        else:
            ret.append(self.data)
        return ret


class IteratorLoader(IPartitionGenerator):  # pylint: disable=R0903
    """Iterator Loader."""

    def __init__(self, iterable, start, end):
        """Create new IteratorLoader object.

        :param iterable: Iterable object.
        :param start: Start position.
        :param end: End Position.
        :returns: None.
        """
        super().__init__()
        self.iterable = iterable
        self.start = start
        self.end = end

    def retrieve_data(self):
        """Divide and retrieve the next partition.

        :returns: Data.
        """
        ret = []
        # If it's a dict
        if isinstance(self.iterable, dict):
            sorted_keys = sorted(self.iterable.keys())
            for key in sorted_keys[self.start : self.end]:  # noqa: E203
                ret.append((key, self.iterable[key]))
        elif isinstance(self.iterable, list):
            for item in iter(
                self.iterable[self.start : self.end]  # noqa: E203
            ):
                ret.append(item)
        else:
            index = 0
            for item in iter(self.iterable):
                index += 1
                if index > self.end:
                    break
                if index > self.start:
                    ret.append(item)
        return ret


class WorkerFileLoader(IPartitionGenerator):  # pylint: disable=R0903
    """Worker file loader."""

    def __init__(
        self, file_paths, single_file=False, start=0, chunk_size=None
    ):
        """Create new WorkerFileLoader object.

        :param file_paths: List of file paths.
        :param single_file: Is a single file?
        :param start: Start position.
        :param chunk_size: Chunk size.
        :returns: None.
        """
        super().__init__()
        self.file_paths = file_paths
        self.single_file = single_file
        self.start = start
        self.chunk_size = chunk_size

        if self.single_file and not chunk_size:
            raise DDSException("Missing chunk_size argument...")

    def retrieve_data(self):
        """Retrieve data.

        :returns: Data.
        """
        if self.single_file:
            file_path = self.file_paths[0]
            with open(file_path) as file_paths_fd:  # pylint: disable=W1514
                file_paths_fd.seek(self.start)
                temp = file_paths_fd.read(self.chunk_size)
            return [temp]

        ret = []
        for file_path in self.file_paths:
            with open(file_path) as file_path_fd:  # pylint: disable=W1514
                content = file_path_fd.read()
            ret.append((file_path, content))

        return ret


class PickleLoader(IPartitionGenerator):  # pylint: disable=R0903
    """Pickle loader."""

    def __init__(self, pickle_path):
        """Create new WorkerFileLoader object.

        :param pickle_path: Pickled file path.
        :returns: None.
        """
        super().__init__()
        self.pickle_path = pickle_path

    def retrieve_data(self):
        """Retrieve data.

        :returns: Data.
        """
        with open(self.pickle_path, "rb") as pickle_path_fd:
            ret = pickle.load(pickle_path_fd)
        return ret


def read_in_chunks(file_name, chunk_size=1024, strip=True):
    """Lazy function (generator) to read a file piece by piece.

    :param file_name: File name to read.
    :param chunk_size: Chunk size (Default: 1k).
    :param strip: If it requires stripping.
    :returns: Next partition.
    """
    partition = []
    with open(file_name) as file_name_fd:  # pylint: disable=W1514
        collected = 0
        for line in file_name_fd:
            _line = line.rstrip("\n") if strip else line
            partition.append(_line)
            collected += sys.getsizeof(_line)
            if collected > chunk_size:
                yield partition
                partition = []
                collected = 0

        if partition:
            yield partition


def read_lines(file_name, num_of_lines=1024, strip=True):
    """Lazy function (generator) to read a file line by line.

    :param file_name: File to read.
    :param num_of_lines: Total number of lines in each partition.
    :param strip: If line separators should be stripped from lines.
    :returns: Next partition.
    """
    partition = []
    with open(file_name) as file_name_fd:  # pylint: disable=W1514
        collected = 0
        for line in file_name_fd:
            _line = line.rstrip("\n") if strip else line
            partition.append(_line)
            collected += 1
            if collected > num_of_lines:
                yield partition
                partition = []
                collected = 0

        if partition:
            yield partition
