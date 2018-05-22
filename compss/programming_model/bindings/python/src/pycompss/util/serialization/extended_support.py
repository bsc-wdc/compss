#!/usr/bin/python
#
#  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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


class GeneratorIndicator(object):
    """
    GeneratorIndicator Class
    """

    pass


def pickle_generator(f_gen, f, serializer):
    """
    Pickle a generator and store the serialization result in a file.

    :param f_gen: Generator object.
    :param f: Destination file for pickling generator.
    :param serializer: Serializer to use
    """

    # Convert generator to list and pickle (less efficient but more reliable)
    # The tuple will be useful to determine when to call unplickle generator.
    # Using a key is weak, but otherwise, How can we difference a list from a
    # generator when receiving it?
    # At least, the key is complicated.
    gen_snapshot = (GeneratorIndicator(), list(f_gen))
    serializer.dump(gen_snapshot, f)


def convert_to_generator(l):
    """
    Converts a list into a generator.

    :param l: List to be converted.
    :return: the generator from the list.
    """

    gen = __list2gen__(l)
    return gen


# never used
def unpickle_generator(f, serializer):
    """
    Unpickle a generator from a file.

    :param f: source file of pickled generator.
    :param serializer: Serializer
    :return: the generator from file.
    """

    snapshot = serializer.load(f)
    gen = __list2gen__(snapshot)
    return gen


def __list2gen__(x):
    return (n for n in x)
