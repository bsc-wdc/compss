#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports


class Resource:
    def __init__(self, name, timestamp):
        self.name = name
        self.hosts = []
        self.hosted = []
        self.history = [[timestamp, "CREATED"]]

    def get_name(self):
        return self.name
        
    def host(self, action, timestamp):
        self.hosts.append(action)
        self.history.append([timestamp, "HOSTS "+str(action)])

    def get_host_actions(self):
        return self.hosts

    def unhost(self, action, timestamp):
        self.history.append([timestamp, "UNHOSTS " + str(action)])
        self.hosts.remove(action)
        self.hosted.append(action)

    def __str__(self):
        string = self.name +": "
        string = string + "\n \t Hosting:"
        for action in self.hosts:
            string = string + "\n \t\t" + str(action)
        return string


class ResourceRegister:
    def __init__(self):
        self.resources = {}  # value=Resources

    def register_resource(self, resource_name, timestamp):
        resource = self.resources.get(resource_name)
        if resource is None:
            resource = Resource(resource_name, timestamp)
            self.resources[resource_name] = resource
        return resource

    def get_resource(self, resource_name):
        return self.resources.get(resource_name)

    def get_resource_names(self):
        return self.resources.keys()

    def get_resources(self):
        return self.resources.values()

    def __str__(self):
        string = ""
        for key, value in self.resources.items():
            string = string + "\n * " + (str(key) + " -> " + str(value))

        return string

