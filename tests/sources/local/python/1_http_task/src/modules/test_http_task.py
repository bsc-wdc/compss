#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import os
import unittest

from pycompss.api.task import task
from pycompss.api.http import http
from pycompss.api.software import software
from pycompss.api.parameter import *
from pycompss.api.api import compss_wait_on as cwo, compss_barrier as cb


class TestHttpTask(unittest.TestCase):

    def test_post_methods(self):
        mes = cwo(dummy_post())
        self.assertEqual(mes, "post_works", "TEST FAILED: POST dummy")

        http_mes = cwo(http_in_software())
        self.assertEqual(http_mes, "post_works", "TEST FAILED: POST dummy")

        payload = "something"
        mes = cwo(post_with_param(payload))
        self.assertEqual(str(mes), payload, "TEST FAILED: POST param in payload")

        inner_param = "hello"
        res = cwo(post_with_inner_param(inner_param))
        self.assertEqual(res.get("first", None), inner_param,
                         "TEST FAILED: POST inner param")

        fayl, content = "payload_file", "payload_content"
        with(open(fayl, 'w')) as nm:
            nm.write(content)

        ret = cwo(post_with_file_param(content))
        self.assertEqual(str(ret), content, "TEST FAILED: POST file as payload")

    def test_serialization(self):

        payload = "something"
        ret = post_with_param(payload)

        res = cwo(post_with_inner_param(ret))
        self.assertEqual(res.get("first", ""), payload,
                         "TEST FAILED: POST inner param")

        inout = post_with_inner_param(ret)
        update_inout_dict(inout)
        regular_task(inout)
        inout = cwo(inout)
        self.assertIn("third", inout,  "TEST FAILED: json serialization")
        self.assertIn("greetings_from", inout,  "TEST FAILED: json serialization")

        length = get_length("holalaa")
        inout = post_with_inner_param(length)
        regular_task(inout)
        inout = cwo(inout)
        self.assertIn("greetings_from", inout,  "TEST FAILED: json serialization")

    def test_get_methods(self):
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

    def test_dictionaries(self):

        payload = dict(first="a", second="b")
        res = cwo(post_with_dict_param(payload))
        self.assertDictEqual(res, payload, "TEST FAILED: dict payload/return")

        inout = dict(first="a", second="b")
        update_inout_dict(inout)
        inout = cwo(inout)
        self.assertIn("third", inout,  "TEST FAILED: inout dict")

    def test_with_regular_tasks(self):

        inout = dict(first="a", second="b")
        update_inout_dict(inout)
        regular_task(inout)
        inout = cwo(inout)
        self.assertIn("third", inout,  "TEST FAILED: http --> task")
        self.assertIn("greetings_from", inout,  "TEST FAILED: http --> task")


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
      payload='{ "first" : {{inner_param}} }', payload_type="application/json")
@task(returns=dict)
def post_with_inner_param(inner_param):
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


@http(service_name="service_1", request="POST", resource="post_json/",
      payload="{{dict_in_param}}", payload_type="application/json")
@task(returns=dict)
def post_with_dict_param(dict_in_param):
    """
    """
    pass


@http(service_name="service_1", request="GET",
      resource="produce_format/test",
      produces="{'length':'{{return_0}}', 'child_json':{'depth_1':'one',"
               "'message':'{{param}}'}}",
      updates='{{event}}.third = {{param}}')
@task(event=INOUT)
def update_inout_dict(event):
    """
    """
    pass


@task(test=INOUT)
def regular_task(test):
    """
    """
    test["greetings_from"] = "regular_task"


@software(config_file=os.getcwd() + "/src/http.json")
@task(returns=str)
def http_in_software():
    """
    """
    pass
