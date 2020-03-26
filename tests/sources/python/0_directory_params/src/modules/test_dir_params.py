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
from pycompss.api.api import compss_wait_on as cwo, compss_barrier as cb
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
        cb(True)

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
