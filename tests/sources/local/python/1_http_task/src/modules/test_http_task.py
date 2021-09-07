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
from pycompss.api.http import http
from pycompss.api.parameter import *
from pycompss.api.api import compss_wait_on as cwo, compss_barrier as cb
from pycompss.api.api import compss_wait_on_directory as cwod


class TestHttpTask(unittest.TestCase):

    def test_post_methods(self):
        mes = cwo(dummy_post())
        self.assertEqual(mes, "post_works", "TEST FAILED: POST dummy")

        payload = "something"
        mes = cwo(post_with_param(payload))
        self.assertEqual(str(mes), payload, "TEST FAILED: POST param in payload")

        fayl, content = "payload_file", "payload_content"
        with(open(fayl, 'w')) as nm:
            nm.write(content)

        ret = cwo(post_with_file_param(content))
        self.assertEqual(str(ret), content, "TEST FAILED: POST file as payload")

    def _test_get_methods(self):
        dummy()
        print("GET: dummy works.")
        message = "holala"
        length = int(cwo(get_length(message)))
        self.assertEqual(length, len(message), "TEST FAILED: GET get_length")

        mes = cwo(return_message(message))
        self.assertEqual(mes, message,  "TEST FAILED: GET return_message")

        mes = cwo(get_nested_produces(message))
        self.assertEqual(str(mes), message,  "TEST FAILED: GET nested_produces")

        mes, length = cwo(multi_return(message))
        self.assertEqual(str(mes), message,  "TEST FAILED: GET multi return 0")
        self.assertEqual(int(length), len(message),
                         "TEST FAILED: GET multi return 1")


@http(service_name="service_1", request="POST", resource="post/")
@task(returns=str)
def dummy_post():
    """
    """
    pass


@http(service_name="service_1", request="POST", resource="post_json/",
      payload="{{payload}}", payload_type="application/json")
@task(returns=str)
def post_with_param(payload):
    """
    """
    pass


@http(service_name="service_1", request="POST", resource="post_json/",
      payload="{{payload}}")
@task(returns=str, message=FILE_IN)
def post_with_file_param(payload):
    """
    """
    pass


@http(service_name="service_1", request="GET", resource="dummy/")
@task(returns=str)
def dummy():
    """
    """
    pass


@http(service_name="service_1", request="GET",
      resource="get_length/{{message}}")
@task(returns=int)
def get_length(message):
    """
    """
    pass


@http(service_name="service_1", request="GET",
      resource="print_message/{{message}}")
@task(returns=str)
def return_message(message):
    """
    """
    pass


@http(service_name="service_1", request="GET",
      resource="produce_format/{{message}}",
      produces="{'child_json':{'depth_1':'one','message':'{{return_0}}'},"
               "'depth_0':'zero'}")
@task(returns=str)
def get_nested_produces(message):
    """
    """
    pass


@http(service_name="service_1", request="GET",
      resource="produce_format/{{message}}",
      produces="{'length':'{{return_1}}', 'child_json':{'depth_1':'one',"
               "'message':'{{return_0}}'}}")
@task(returns=2)
def multi_return(message):
    """
    """
    pass
