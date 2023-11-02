#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs DDS test
=========================
    This file represents PyCOMPSs Testbench.
    Tests of DDS Library
"""

import os
import random
import shutil
import tempfile
from collections import defaultdict

from pycompss.dds import DDS
from tasks import gen_fragment
from tasks import gen_big_fragment


def test_loader_functions():

    # Iterator loader
    dds = DDS().load(range(10)).collect()
    assert len(dds) == 10

    # Single file
    content = "Holala World!"
    desc, filename = tempfile.mkstemp()
    with open(filename, "wb") as tmp_f:
        tmp_f.write(bytearray(content, "utf8"))
    dds = DDS().load_file(filename).collect()
    os.remove(filename)
    assert dds[0] == content

    # Multiple files
    _dir = os.path.join(tempfile.gettempdir(), "multi")
    if not os.path.exists(_dir):
        os.mkdir(_dir)

    for _i in range(10):
        desc, filename = tempfile.mkstemp(dir=_dir)
        file_path = os.path.join(_dir, filename)
        with open(file_path, "wb") as tmp_f:
            tmp_f.write(bytearray(str(_i), "utf8"))

    dds = DDS().load_files_from_dir(_dir).collect()
    shutil.rmtree(_dir)

    dds = sorted(dds, key=lambda x: x[1])
    for _i in range(10):
        assert int(dds[_i][1]) == _i


def test_word_count():
    # Word Count
    vocabulary = ["Holala", "World", "COMPSs", "Lorem", "Ipsum", "_filter_"]

    _dir = os.path.join(tempfile.gettempdir(), "wc")
    if not os.path.exists(_dir):
        os.mkdir(_dir)
    for _i in range(5):
        desc, filename = tempfile.mkstemp(dir=_dir, suffix=".txt")
        file_path = os.path.join(_dir, filename)
        tmp_f = open(file_path, "w")
        for word in vocabulary:
            tmp_f.write(word + " ")
        tmp_f.close()

    dds = DDS().load_files_from_dir(_dir) \
        .flat_map(lambda x: x[1].split())\
        .filter(lambda x: "_" not in x)\
        .count_by_value(as_dict=True)

    shutil.rmtree(_dir)

    for key, value in dds.items():
        assert "_" not in key
        assert value == 5


def test_terasort():

    dataset = [gen_fragment() for _ in range(10)]
    dds = DDS().load(dataset, -1).sort_by_key().collect()
    prev = 0

    for key, _ in dds:
        assert key > prev
        prev = key


def test_big_terasort():

    dataset = [gen_big_fragment() for _ in range(10)]
    dds = DDS().load(dataset, -1).sort_by_key().collect()
    prev = 0

    for key, _ in dds:
        assert key > prev
        prev = key


def inverted_indexing():

    def _invert_files(pair):
        res = dict()
        for word in pair[1].split():
            res[word] = [pair[0]]
        return res.items()

    vocabulary = ["Holala", "World", "COMPSs", "Lorem", "Ipsum"]
    files = list()
    pairs = defaultdict(list)

    _dir = os.path.join(tempfile.gettempdir(), "ii")
    if not os.path.exists(_dir):
        os.mkdir(_dir)

    # Create Files
    for _i in range(len(vocabulary)//2):
        desc, filename = tempfile.mkstemp(dir=_dir, suffix=".txt")
        files.append(filename)

    for word in vocabulary:
        _files = random.sample(files, 2)
        for _f in _files:
            file_path = os.path.join(_dir, _f)
            tmp_f = open(file_path, "a")
            tmp_f.write(word + " ")
            tmp_f.close()
            pairs[word].append(file_path)

    result = DDS().load_files_from_dir(_dir).flat_map(_invert_files)\
        .reduce_by_key(lambda a, b: a + b).collect()

    shutil.rmtree(_dir)

    for word, files in result:
        assert set(pairs[word]).issubset(set(files))


def main_program():

    print("____________TEST DDS______________")
    test_loader_functions()
    test_word_count()
    test_terasort()
    test_big_terasort()
    inverted_indexing()
    print("- Test DDS: OK")


if __name__ == "__main__":
    main_program()
