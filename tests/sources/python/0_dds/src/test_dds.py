#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs DDS test
=========================
    This file represents PyCOMPSs Testbench.
    Tests of DDS Library
"""

# Imports
import tempfile
import os, shutil

from pycompss.dds import DDS


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
    assert dds[0] == content
    os.remove(filename)

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
    dds = sorted(dds, key=lambda x: x[1])
    for _i in range(10):
        assert int(dds[_i][1]) == _i
    shutil.rmtree(_dir)

    return True


def main_program():

    print("____________TEST DDS______________")
    if not test_loader_functions():
        print("- Test DDS Loader Functions: FAILED")
        # If loader functions fail, quit the tests..
        return

    print("- Test DDS: OK")


if __name__ == "__main__":
    main_program()
