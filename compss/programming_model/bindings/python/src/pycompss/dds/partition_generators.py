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

"""
Partitions should be loaded with data in the last step only. Thus, generator
objects are necessary. Inside 'task' functions we will call their 'generate'
method in order to retrieve the partition. These partitions can previously be
loaded on master and sent to workers, or read from files on worker nodes.
"""
import pickle
import sys
from pycompss.util.exceptions import DDSException


class IPartitionGenerator(object):
    """
    Everyone implements this.
    """
    def retrieve_data(self):
        raise NotImplementedError


class BasicDataLoader(IPartitionGenerator):

    def __init__(self, data):
        super(BasicDataLoader, self).__init__()
        self.data = data

    def retrieve_data(self):
        ret = list()
        if isinstance(self.data, list):
            ret.extend(self.data)
        else:
            ret.append(self.data)
        return ret


class IteratorLoader(IPartitionGenerator):

    def __init__(self, iterable, start, end):
        super(IteratorLoader, self).__init__()
        self.iterable = iterable
        self.start = start
        self.end = end

    def retrieve_data(self):
        """
        Divide and retrieve the next partition.
        :return:
        """
        ret = list()
        # If it's a dict
        if isinstance(self.iterable, dict):
            sorted_keys = sorted(self.iterable.keys())
            for key in sorted_keys[self.start:self.end]:
                ret.append((key, self.iterable[key]))
        elif isinstance(self.iterable, list):
            for item in iter(self.iterable[self.start:self.end]):
                ret.append(item)
        else:
            index = 0
            for item in iter(self.iterable):
                index += 1
                if index > self.end:
                    break
                elif index > self.start:
                    ret.append(item)
        return ret


class WorkerFileLoader(IPartitionGenerator):

    def __init__(self, file_paths, single_file=False, start=0, chunk_size=None):
        super(WorkerFileLoader, self).__init__()

        self.file_paths = file_paths
        self.single_file = single_file
        self.start = start
        self.chunk_size = chunk_size

        if self.single_file and not chunk_size:
            raise DDSException("Missing chunk_size argument...")

    def retrieve_data(self):

        if self.single_file:
            fp = open(self.file_paths[0])
            fp.seek(self.start)
            temp = fp.read(self.chunk_size)
            fp.close()
            return [temp]

        ret = list()
        for file_path in self.file_paths:
            content = open(file_path).read()
            ret.append((file_path, content))

        return ret


class PickleLoader(IPartitionGenerator):

    def __init__(self, pickle_path):
        super(PickleLoader, self).__init__()
        self.pickle_path = pickle_path

    def retrieve_data(self):
        ret = pickle.load(open(self.pickle_path, "rb"))
        return ret


def read_in_chunks(file_name, chunk_size=1024, strip=True):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    partition = list()
    f = open(file_name)
    collected = 0
    for line in f:
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
    """
    Lazy function (generator) to read a file line by line.
    :param file_name:
    :param num_of_lines: total number of lines in each partition
    :param strip: if line separators should be stripped from lines
    """
    partition = list()
    f = open(file_name)
    collected = 0
    for line in f:
        _line = line.rstrip("\n") if strip else line
        partition.append(_line)
        collected += 1
        if collected > num_of_lines:
            yield partition
            partition = []
            collected = 0

    if partition:
        yield partition
