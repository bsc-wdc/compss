#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Simple
========================
"""
import sys

"""
GROMACS Tutorial with PyCOMPSs
Lysozyme in Water

This example will guide a new user through the process of setting up a
simulation system containing a set of proteins (lysozymes) in boxes of water,
with ions. Each step will contain an explanation of input and output,
using typical settings for general use.

Extracted from: http://www.mdtutorials.com/gmx/lysozyme/index.html
Originally done by: Justin A. Lemkul, Ph.D.
From: Virginia Tech Department of Biochemistry

This example reaches up to stage 4 (energy minimization) and includes resulting
images merge.
"""

import unittest

from src.lysozyme_in_water import *


class ContainerMPITest(unittest.TestCase):

    def test_gromacs(self):
        base_app_dir = sys.argv[0].replace("decorator_container_mpi.py", "../")
        config_path = os.path.join(base_app_dir, "config")
        dataset_path = "/gpfs/projects/bsc19/COMPSs_DATASETS/gromacs/2m"
        output_path = os.path.join(base_app_dir, "output")
        self.assertTrue(main(dataset_path, output_path, config_path))


if __name__ == '__main__':
    unittest.main()
