#!/usr/bin/python

# -*- coding: utf-8 -*-


class MyObject(object):

    def __init__(self, name=None, age=None):
        self.name = name
        self.age = age

    def get(self):
        return self.age

    def put(self, age):
        self.age = age
