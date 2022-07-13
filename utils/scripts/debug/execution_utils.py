#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
from enum import Enum

from data_utils import DataRegister
from data_utils import MainDataAccessRegister
from task_utils import TaskRegister
from task_utils import ApplicationRegister
from action_utils import JobRegister
from resource_utils import ResourceRegister
from connection_utils import ConnectionRegister
from ce_utils import CoreElementRegister

import sys

class ExecutionState:
    """
    Class representing the current state of the execution
    """
    def __init__(self):
        self.core_elements = CoreElementRegister()
        self.apps = ApplicationRegister()
        self.tasks = TaskRegister()
        self.resources = ResourceRegister()
        self.jobs = JobRegister()
        self.data = DataRegister()
        self.connections = ConnectionRegister()
        self.main_accesses = MainDataAccessRegister()

    def clear(self):
        self.tasks = TaskRegister()
        self.resources = ResourceRegister()
        self.jobs = JobRegister()
        self.main_accesses = MainDataAccessRegister()

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


    def _short_signature(self, signature):
        if len(signature) > 50 :
            method_name = signature.split(".")[-1]
            prev_len = 47 - len(method_name)
            return signature[0:prev_len]+".."+method_name
        else:
            return signature.ljust(50)
            
    def query_core_elements(self, query):
        """

        :param query:
        """
        if len(query) > 0:
            core_element = None
            if all(char.isdigit() for char in query[0]):
                core_id = int(query[0])
                core_element = self.core_elements.get_core_element_by_id(core_id)
            else:
                core_signature = query[0]
                core_element = self.core_elements.get_core_element_by_signature(core_signature)
            if core_element is None:
                print("Runtime has no information regarding such core element.", file=sys.stderr)
            else:
                print("------- Core Element ------")
                print("Id: " + str(core_element.core_id))
                print("Signature: " + core_element.signature)
                print("Implementations:")
                for impl in core_element.get_implementations():
                    print("\t* Signature:"+impl.signature)
                    print("\t  Constraints:"+impl.constraints)
                print("History:")
                for entry in core_element.get_history():
                    print("\t "+entry[0]+" -> "+entry[1])

        else:
            print("------ Core Elements ------")
            print("ID\tSignature\t\t\t\t\t\tCPUs\tMem")
            cores = self.core_elements.get_core_elements()
            for core in sorted(cores, key=lambda core: int(core.core_id)):
                print(str(core.core_id)+"\t"+self._short_signature(str(core.signature)))
                impls = core.get_implementations()
                impl_count=len(impls)
                idx = 0
                for impl in impls:
                    impl_desc=""
                    if impl_count == idx+1:
                        impl_desc = b'\xc0'.decode("cp437") # |_
                    else:
                        impl_desc = b'\xc3'.decode("cp437") # |-
                    impl_desc = impl_desc + (b'\xc4'.decode("cp437") * 7 ) # -
                    print(impl_desc+" "+self._short_signature(str(impl.signature))+"\t"+impl.get_constraints_cores()+"\t"+impl.get_constraints_memory())
            
    def query_connection(self, query):
        """

        :param query:
        """
        if len(query) > 0:
            if all(char.isdigit() for char in query[0]):
                connection_id = query[0]
                connection = self.connections.get_connection(connection_id)
                if connection is None:
                    print("Runtime has no information regarding connection " + connection_id + ".", file=sys.stderr)
                else:
                    print("-------- Connection ------")
                    print("Id: " + connection.connection_id)
                    print("Socket Id: " + str(connection.socket_id))
                    print("Current Stage: "+str(connection.current_stage.name))
                    
                    print("Enqueued Stages: ")
                    for stage in connection.get_stages():
                        print("\t "+stage.name)
                    print("Connection History:")
                    for entry in connection.get_history():
                        print("\t "+entry[0]+" -> "+entry[1])
            else:
                print("Unknown job sub-command "+query[0], file=sys.stderr)
        else:
            print("------- Connections -------")
            print("Connection ID\tStatus")
            connections = self.connections.get_connections()
            for connection in sorted(connections, key=lambda connection: int(connection.connection_id)):
                print(connection.connection_id+"\t"+str(connection.status))

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
            print("Job ID\tStatus\t\tAction\t\t\t\tResource\tExecution Period")
            jobs = self.jobs.get_jobs()
            for job in sorted(jobs, key=lambda job: int(job.job_id)):
                job_status_label = "{:<15}".format(str(job.status).split(".")[1])
                job_action_label= "{:<31}".format(str(job.action))
                job_resource_label = "{:<15}".format(str(job.resource.name))

                job_creation_label="?"
                job_submission_label="?"
                job_completion_label="?"
                for entry in job.get_history():
                    if "created" in entry[1]:
                        job_creation_label=entry[0]
                    elif "submitted" in entry[1]:
                        job_submission_label=entry[0]
                    elif "completed" in entry[1]:
                        job_completion_label=entry[0]

                print(job.job_id+"\t"+job_status_label+"\t"+job_action_label+"\t"+job_resource_label+"\t"+job_submission_label+"-"+job_completion_label)

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

