#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest
import random

from pycompss.functions.data import generator


class testGenerator(unittest.TestCase):

    def testGeneratorRandom(self):
        size = (4, 4)
        num_frag = 2
        frag_size = int(size[0] / num_frag)
        seed = 10
        def gen_random(size, frag_size):
            random.seed(seed)
            return [[random.random() for _ in range(size)] for _ in range(frag_size)]
        expected = [gen_random(size[1], frag_size) for i in range(num_frag)]
        result = generator(size, num_frag, seed, 'random', True)
        self.assertEqual(result, expected)

    def testGeneratorNormal(self):
        size = (4, 4)
        num_frag = 2
        frag_size = int(size[0] / num_frag)
        seed = 10
        def gen_normal(size, frag_size):
            random.seed(seed)
            return [[random.gauss(mu=0.0, sigma=1.0) for _ in range(size)] for _ in range(frag_size)]
        expected = [gen_normal(size[1], frag_size) for i in range(num_frag)]
        result = generator(size, num_frag, seed, 'normal', True)
        self.assertEqual(result, expected)

    def testGeneratorUniform(self):
        size = (4, 4)
        num_frag = 2
        frag_size = int(size[0] / num_frag)
        seed = 10
        def gen_uniform(size, frag_size):
            random.seed(seed)
            return [[random.uniform(-1.0, 1.0) for _ in range(size)] for _ in range(frag_size)]
        expected = [gen_uniform(size[1], frag_size) for i in range(num_frag)]
        result = generator(size, num_frag, seed, 'uniform', True)
        self.assertEqual(result, expected)
