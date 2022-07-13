#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
from enum import Enum


class TaskState(Enum):
    CREATED = 0
    COMPLETED = 1
    FAILED = 2


class ParameterType(Enum):
    BASIC = 0
    FILE = 1
    OBJECT = 2


class ParameterDirection(Enum):
    IN = 0
    OUT = 1
    INOUT = 2


class Parameter:

    def __init__(self, task, access):
        self.task = task
        self.type = ParameterType.BASIC
        self._access = access
        self.confirmed_dependency = None
        self.commutative_dependency = None

    def get_reason(self):
        return self.task

    def get_direction(self):
        if self._access is not None:
            return self._access.get_direction()
        return None

    def get_data(self):
        if self._access is not None:
            return self._access.get_data()
        return None

    def set_confirmed_dependency(self, task):
        self.confirmed_dependency = task

    def get_confirmed_dependency(self):
        return self.confirmed_dependency

    def set_commutative_dependency(self, groupId):
        self.commutative_dependency = groupId

    def get_commutative_dependency(self):
        return self.commutative_dependency

    def get_detected_dependency(self):
        if self._access is not None:
            return self._access.get_dependence()
        else:
            return None

    def committed(self, timestamp):
        if self._access is not None:
            self._access.committed(timestamp)

    def __str__(self):
        return "Parameter " + str(self._access)

    def __repr__(self):
        return self.__str__()


class Task:
    """

    """

    def __init__(self, task_id, method_name, timestamp):
        """

        :param task_id:
        """
        self.task_id = task_id
        self.method_name = method_name
        self.action = []
        self.state = TaskState.CREATED
        self.parameters = []
        self._history = [[timestamp, "task " + self.task_id + " created"]]

    def get_id(self):
        return self.task_id

    def get_method_name(self):
        return self.method_name

    def add_parameter(self, access):
        param = Parameter(self, access)
        self.parameters.append(param)
        return param

    def get_last_registered_parameter(self):
        if len(self.parameters) > 0:
            return self.parameters[-1]
        else:
            return None

    def get_history(self):
        history = []
        history = history + self._history
        for action in self.action:
            history = history + action.get_history()

        return sorted(history, key=lambda t: int(t[0]))

    def completed(self, timestamp):
        self.state = TaskState.COMPLETED
        self._history.append([timestamp, "task " + self.task_id + " completed"])
        for p in self.parameters:
            p.committed(timestamp)

    def failed(self, timestamp):
        self.state = TaskState.FAILED
        self._history.append([timestamp, "task " + self.task_id + " failed"])
  
    def get_state(self):
        return self.state

    def add_action(self, action):
        self.action.append(action)

    def get_actions(self):
        return self.action

    def __str__(self):
        params = ""
        for p in self.parameters:
            params = params + "\t\t " + str(p) + "\n"

        actions = ""
        for a in self.action:
            actions = actions + "\t\t " + str(a) + "\n"
        return "Task Id: " + self.task_id + " with state " + str(self.state) + " " + "\n" \
               + "\t Parameters: \n" + params \
               + "\t Actions: \n" + actions

    def __repr__(self):
        return self.__str__()


class TaskRegister:

    def __init__(self):
        self.tasks = {}  # value=Task
        self.registered_tasks_count = 0
        self.completed_tasks_count = 0
        self.failed_tasks_count = 0
        self.core_counts = {}

    def register_task(self, task_id, method_name, timestamp):
        task = self.tasks.get(task_id)
        if task is None:
            task = Task(task_id, method_name, timestamp)
            self.tasks[task_id] = task
            self.registered_tasks_count += 1
        count = self.core_counts.get(method_name)
        if count is None:
            count = 0
        count = count + 1
        self.core_counts[method_name] = count
        return task

    def get_method_names(self):
        return self.core_counts.keys();

    def get_task(self, task_id):
        return self.tasks[task_id]

    def get_tasks(self):
        return self.tasks.values()

    def __str__(self):
        string = ""
        for key,value in self.tasks.items():
            string = string + "\n * " + (str(key) + " -> "+str(value))

        return string

class Application:
    def __init__ (self, id):
        self.id = id
        self.tasks=[]

    def get_id(self):
        return self.id

    def register_task(self, task):
        self.tasks.append(task)

    def get_tasks(self):
        return self.tasks

    def __str__(self):
        return "App " + self.id

class ApplicationRegister():
    def __init__(self):
        self.apps = {}

    def register_app(self, appId):
        app = self.apps.get(appId)
        if app == None:
            app = Application(appId)
            self.apps[appId] = app
        return app

    def get_applications(self):
        return self.apps.values()
    
    def get_application(self, appId):
        return self.apps.get(appId)
