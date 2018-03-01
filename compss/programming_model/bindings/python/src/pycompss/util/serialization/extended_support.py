#!/usr/bin/python

# -*- coding: utf-8 -*-

class GeneratorIndicator(object):
    pass


def pickle_generator(f_gen, f, serializer):
    '''
    Pickle a generator and store the serialization result in a file.
    :param f_gen: generator object.
    :param f: destination file for pickling generator.
    :param serializer: serializer to use
    '''
    # Convert generator to list and pickle (less efficient but more reliable)
    # The tuple will be useful to determine when to call unplickle generator.
    # Using a key is weak, but otherwise, How can we difference a list from a
    # generator when receiving it?
    # At least, the key is complicated.
    genSnapshot = (GeneratorIndicator(), list(f_gen))
    serializer.dump(genSnapshot, f)


def convert_to_generator(l):
    '''
    Converts a list into a generator.
    :param l: List to be converted.
    :return: the generator from the list.
    '''
    gen = __list2gen__(l)
    return gen


def __list2gen__(x):
    return (n for n in x)


# never used
def unpickle_generator(f, serializer):
    '''
    Unpickle a generator from a file.
    :param f: source file of pickled generator.
    :return: the generator from file.
    '''
    snapshot = serializer.load(f)
    gen = __list2gen__(snapshot)
    return gen
