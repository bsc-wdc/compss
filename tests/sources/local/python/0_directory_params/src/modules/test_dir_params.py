#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import os
import shutil
import unittest

from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import compss_wait_on as cwo
from pycompss.api.api import compss_wait_on_directory as cwod


class TestDirParams(unittest.TestCase):

    @task(dir_in=DIRECTORY_IN)
    def dir_in_task(self, dir_in):
        res = ""
        for _ in os.listdir(dir_in):
            _fp = dir_in + os.sep + _
            with(open(_fp, 'r')) as nm:
                res += nm.read() + "\n"

        return res

    @task(dir_out=DIRECTORY_OUT)
    def dir_out_task(self, dir_out, content):

        os.mkdir(dir_out)

        for i, word in enumerate(content):
            temp = "{}{}{}".format(dir_out, os.sep, str(i))
            with(open(temp, 'w')) as _f:
                _f.write(word + "\n")

    @task(dir_inout=DIRECTORY_INOUT)
    def dir_inout_task(self, dir_inout):

        for _ in os.listdir(dir_inout):
            _fp = dir_inout + os.sep + _
            with(open(_fp, 'a')) as nm:
                nm.write(" #inout_task# ")

    @task(dir_in=DIRECTORY_IN)
    def dir_in_task_i(self, dir_in):
        res = list()
        for _ in os.listdir(dir_in):
            _fp = dir_in + os.sep + _
            with(open(_fp, 'r')) as nm:
                res .append(nm.read())

        return res

    @task(dir_out=DIRECTORY_OUT)
    def dir_out_task_i(self, dir_out, i):

        if os.path.exists(dir_out):
            shutil.rmtree(dir_out)

        os.mkdir(dir_out)

        f_out = "{}{}{}".format(dir_out, os.sep, i)

        with(open(f_out, 'w')) as nm:
            nm.write("written in dir out #{}".format(i))

    @task(dir_inout=DIRECTORY_INOUT, returns=list)
    def dir_inout_task_i(self, dir_inout, i):

        res = list()
        for _ in os.listdir(dir_inout):
            with(open("{}{}{}".format(dir_inout, os.sep, _), 'r')) as nm:
                res.append(nm.read())

        f_inout = "{}{}{}".format(dir_inout, os.sep, i)

        with(open(f_inout, 'w')) as nm:
            nm.write("written by inout task #" + str(i))

        return res

    @task(returns=bool, first=DIRECTORY_IN, second=DIRECTORY_INOUT, third=DIRECTORY_OUT)
    def directory_None(self, first=None, second=None, third=None):
        if first is None and \
           second is None and \
           third is None:
            return True
        else:
            return False

    def test_workflow(self):
        """
        Test multiple tasks with directory in, out, and inout params.
        """
        cur_path = "{}{}".format(os.getcwd(), os.sep)
        dir_t = "{}{}".format(cur_path, "some_dir_t")
        os.mkdir(dir_t)

        # len(phase[i] = i)
        res_phase_0 = []
        for i in range(0, 5, 1):
            res_phase_0.append(self.dir_inout_task_i(dir_t, i))

        # len(phase[i] = 5)
        res_phase_1 = []
        for i in range(0, 5, 1):
            res_phase_1.append(self.dir_in_task_i(dir_t))

        # len(phase[i] = i + 5)
        res_phase_2 = []
        for i in range(5, 10, 1):
            res_phase_2.append(self.dir_inout_task_i(dir_t, i))

        # len(phase[i] = 10)
        res_phase_3 = []
        for i in range(0, 5, 1):
            res_phase_3.append(self.dir_in_task_i(dir_t))

        # dir out should contain only the last file
        for i in range(0, 15, 1):
            self.dir_out_task_i(dir_t, i)

        res_phase_0 = cwo(res_phase_0)
        res_phase_1 = cwo(res_phase_1)
        res_phase_2 = cwo(res_phase_2)
        res_phase_3 = cwo(res_phase_3)
        cwod(dir_t)

        for i, res in enumerate(res_phase_0):
            self.assertEqual(len(res), i,
                             "error in task #{} of phase 0: {} != {}"
                             .format(i, len(res), i))

        for i, res in enumerate(res_phase_1):
            self.assertEqual(len(res), 5,
                             "error in task #{} of phase 1: {} != 5"
                             .format(i, len(res)))

        for i, res in enumerate(res_phase_2):
            self.assertEqual(len(res), i + 5,
                             "error in task #{} of phase 2: {} != {}"
                             .format(i, len(res), i + 5))

        for i, res in enumerate(res_phase_3):
            self.assertEqual(len(res), 10,
                             "error in task #{} of phase 3: {} != 10"
                             .format(i, len(res)))

        self.assertEqual(1, len(os.listdir(dir_t)),
                         "directory has fewer or more files than 1: {}"
                         .format(len(os.listdir(dir_t))))

        shutil.rmtree(dir_t)

    def test_dir_inout(self):
        """
        Test DIRECTORY_INOUT
        """
        cur_path = "{}{}".format(os.getcwd(), os.sep)
        dir_inout = "{}{}".format(cur_path, "some_dir_inout")
        os.mkdir(dir_inout)

        _content = "this is some text to test directory_inout".split(" ")

        for i, word in enumerate(_content):
            temp = "{}{}{}".format(dir_inout, os.sep, str(i))
            with open(temp, 'w') as f:
                f.write(word)

        res = ""
        for _f in os.listdir(dir_inout):
            temp = "{}{}{}".format(dir_inout, os.sep, _f)
            with(open(temp, 'r')) as _:
                res += _.read() + " "

        self.dir_inout_task(dir_inout)
        cwod(dir_inout)

        res = ""
        for _f in os.listdir(dir_inout):
            temp = "{}{}{}".format(dir_inout, os.sep, _f)
            with(open(temp, 'r')) as _:
                res += _.read() + " "

        for word in _content:
            self.assertTrue(word in res,
                            "missing word in dir_inout files: {}".format(word))

        total = len(_content) + 1
        retrieved = len(res.split("#inout_task#"))
        self.assertEqual(total, retrieved,
                         "append failed for some file(s).")
        shutil.rmtree(dir_inout)

    def test_dir_out(self):
        """
        Test DIRECTORY_OUT
        """
        cur_path = "{}{}".format(os.getcwd(), os.sep)
        dir_out = "{}{}".format(cur_path, "some_dir_out")

        content = "this is some text to test directory_out".split(" ")
        self.dir_out_task(dir_out, content)
        cwod(dir_out)

        self.assertTrue(os.path.exists(dir_out),
                        "directory_out has not been created")

        res = ""
        for _f in os.listdir(dir_out):
            temp = "{}{}{}".format(dir_out, os.sep, _f)
            with(open(temp, 'r')) as _:
                res += _.read() + " "

        for word in content:
            self.assertTrue(word in res,
                            "missing word in dir_out files: {}".format(word))

        shutil.rmtree(dir_out)

    def test_dir_in(self):
        """
        Test DIRECTORY_IN
        """
        cur_path = "{}{}".format(os.getcwd(), os.sep)
        dir_in = "{}{}".format(cur_path, "some_dir_in")
        os.mkdir(dir_in)

        content = "this is some text to test directory_in".split(" ")

        for i, word in enumerate(content):
            temp = "{}{}{}".format(dir_in, os.sep, str(i))
            with open(temp, 'w') as f:
                f.write(word)

        res = self.dir_in_task(dir_in)
        cwod(dir_in)
        res = cwo(res)
        for word in content:
            self.assertTrue(word in res, "missing word: {}".format(word))

        shutil.rmtree(dir_in)

    def test_dir_with_none(self):
        """
        Test DIRECTORY parameters with None
        """
        from pycompss.api.api import compss_wait_on
        din = None
        dinout = None
        dout = None
        res = self.directory_None(din, dinout, dout)
        res = compss_wait_on(res)
        self.assertEqual(res, True, "A parameter was not None in the directory_None task.")
        res = self.directory_None()
        res = compss_wait_on(res)
        self.assertEqual(res, True, "A parameter was not None in the directory_None task using default parameters.")
        # makes sense to wait on a None directory? No
