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



def main_program():

    print("____________TEST DDS______________")
    test_loader_functions()
    print("- Test DDS: OK")


if __name__ == "__main__":
    main_program()
