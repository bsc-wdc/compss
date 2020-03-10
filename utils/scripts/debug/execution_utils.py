#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
from enum import Enum

from data_utils import DataRegister
from task_utils import TaskRegister
from action_utils import JobRegister
from resource_utils import ResourceRegister

import sys


class DataAccessStatus(Enum):
    REQUESTED = 0
    EXISTENCE_AWARE = 1
    OBTAINED = 2


class DataAccess:
    def __init__(self, access, data, timestamp):
        self.access = access
        self.access.register_read(data, timestamp)
        self.state = DataAccessStatus.REQUESTED
        self.access.get_read_version(timestamp).main_access_progress("requested", timestamp)

    def exists(self, timestamp):
        self.state = DataAccessStatus.EXISTENCE_AWARE
        self.access.get_read_version(timestamp).main_access_progress("is aware of existence", timestamp)

    def obtained(self, timestamp):
        self.state = DataAccessStatus.OBTAINED
        self.access.get_read_version(timestamp).main_access_progress("has the value on the node", timestamp)

    def __str__(self):
        return  str(self.access) + " in state " + str(self.state)


class ExecutionState:
    """
    Class representing the current state of the execution
    """
    def __init__(self):
        self.tasks = TaskRegister()
        self.resources = ResourceRegister()
        self.jobs = JobRegister()
        self.data = DataRegister()
        self.main_access = None

    def main_accesses_data(self, data_id, timestamp):
        access = self.data.last_registered_access
        datum = self.data.get_datum(data_id)
        self.main_access = DataAccess(access, datum, timestamp)

    def clear(self):
        self.tasks = TaskRegister()
        self.resources = ResourceRegister()
        self.jobs = JobRegister()
        self.main_access = None

    def query_resource(self, query):
        if len(query) > 0:
            resource_name = query[0]
            resource = self.resources.get_resource(resource_name)
            if resource is None:
                print("Runtime has no information regarding resource " + resource_name + ".", file=sys.stderr)
            else:
                print("-------- Resource ------")
                print("Name: " + resource.name)
                print("Currently hosting :")
                for a in resource.hosts:
                    print("\t " + str(a))
                print("Task History:")
                for entry in resource.history:
                    print("\t " + entry[0] + " -> " + entry[1])
        else:
            print("-------- Resources ------")
            resources = self.resources.get_resources()
            resource_names = self.resources.get_resource_names()
            max_length = 4
            for name in resource_names:
                if len(name) > max_length:
                    max_length = len(name)
            print("Name" + (((max_length - 4) + 3) * " ") + "#Hosting actions")
            for resource in sorted(resources, key=lambda resource: resource.name):
                name = resource.name
                print(name + (((max_length - len(name)) + 3) * " ") + str(len(resource.hosts)))

    def query_job(self, query):
        """

        :param query:
        """
        if len(query) > 0:
            if all(char.isdigit() for char in query[0]):
                job_id = query[0]
                job = self.jobs.get_job(job_id)
                if job is None:
                    print("Runtime has no information regarding job " + job_id + ".", file=sys.stderr)
                else:
                    print("-------- Job ------")
                    print("Id: " + job.job_id)
                    print("Job Status " + str(job.status))
                    print("Resource: " + str(job.resource.name))
                    print("Action: " + str(job.action))
                    print("Job History:")
                    for entry in job.get_history():
                        print("\t "+entry[0]+" -> "+entry[1])
            else:
                print("Unknown job sub-command "+query[0], file=sys.stderr)
        else:
            print("-------- Jobs ------")
            print("Job ID\tStatus\t\t\tAction")
            jobs = self.jobs.get_jobs()
            for job in sorted(jobs, key=lambda job: int(job.job_id)):
                print(job.job_id+"\t"+str(job.status)+"\t"+str(job.action))

    def query_task(self, query):
        if len(query) > 0:
            if all(char.isdigit() for char in query[0]):
                task_id = query[0]
                task = self.tasks.get_task(task_id)
                if task is None:
                    print("Runtime has no information regarding job " + task_id + ".", file=sys.stderr)
                else:
                    print("-------- Task ------")
                    print("Id: " + task.task_id)
                    print("Method Name: " + task.method_name)
                    print("Parameters:")
                    for p in task.parameters:
                        direction = str(p.get_direction())
                        data = p.get_data()
                        data_id = str(data.get_id())
                        predecessor = p.get_detected_dependency()
                        confirmed_predecessor = p.get_confirmed_dependency()

                        producer = ""
                        if predecessor is not None:
                            predecessor_state = predecessor.state
                            if confirmed_predecessor is not None:
                                if predecessor == confirmed_predecessor:
                                    producer = " depends on task "+str(predecessor.task_id) +\
                                               " (detected with state " + str(predecessor_state)+")"
                                else:
                                    producer = " expected a dependency on task " + str(predecessor.task_id) + \
                                               " and dependency with " + str(confirmed_predecessor.task_id)
                            else:
                                producer = " depends on task "+str(predecessor.task_id) +\
                                               " (not detected with state " + str(predecessor_state)+")"
                        else:
                            if confirmed_predecessor is not None:
                                producer = "unexpected dependency with task " + str(confirmed_predecessor.task_id)
                            else:
                                producer == ""
                        print(" * " + direction + " data " + data_id + producer)

                    print("Status: " + str(task.state))
                    print("Actions: ")
                    for a in task.action:
                        print("\t "+str(a))
                    print("Task History:")
                    for entry in task.get_history():
                        print("\t "+entry[0]+" -> "+entry[1])
            else:
                print("Unknown job sub-command "+query[0], file=sys.stderr)
        else:
            print("-------- Tasks ------")
            name_max_length = 0
            for name in self.tasks.get_method_names():
                if len(name) > name_max_length:
                    name_max_length = len(name)
            print("Task ID" + "    " +
                  "Method name" + ((name_max_length - 11 + 4) * " ") +
                  "Status")
            tasks = self.tasks.get_tasks()
            for task in sorted(tasks, key=lambda task: int(task.task_id)):
                print(task.task_id+((11-len(task.task_id))*" ") +
                      task.method_name + (( name_max_length - len(task.method_name) + 4) * " ") +
                      str(task.state))

    def query_data(self, query):
        if len(query) > 0:
            if all(char.isdigit() for char in query[0]):
                data_id = query[0]
                data = self.data.get_datum(data_id)
                if data is None:
                    print("Runtime has no information regarding data " + data_id + ".", file=sys.stderr)
                else:
                    print("-------- Data ------")
                    print("Id: " + data.get_id())
                    last_writer = data.get_last_writer()
                    if last_writer is None:
                        last_writer = "Main Code"
                    else:
                        last_writer = last_writer.task.task_id
                    print("Last writer: " + last_writer)
                    print("Data history:")
                    for entry in data.get_history():
                        print("\t " + entry[0] + " -> " + entry[1])
            else:
                print("Unknown job sub-command " + query[0], file=sys.stderr)
        else:
            print("-------- Data ------")
            print("Data ID     Num Versions    Last Writer")
            data = self.data.get_data()
            for d in sorted(data, key=lambda d: int(d.get_id())):
                last_writer = d.get_last_writer()
                if last_writer is None:
                    last_writer = "Main Code"
                else:
                    last_writer = last_writer.task.task_id
                versions_count = str(len(d.get_all_versions()))
                print(d.get_id() + ((12 - len(d.get_id())) * " ") + versions_count + ((28 - 12 - len(versions_count)) * " ") + last_writer)

