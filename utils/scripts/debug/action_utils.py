#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
from abc import abstractmethod
from enum import Enum


class ActionType(Enum):
    EXECUTION_ACTION = 0
    START_WORKER_ACTION = 1
    BUSY_WORKER_ACTION = 2
    IDLE_WORKER_ACTION = 3


class JobStatus(Enum):
    CREATED = 0
    SUBMITTED = 1
    RUNNING=2
    STALLED=3
    COMPLETED = 4
    FAILED = -1


class Job:

    def __init__(self, job_id, timestamp):
        self.job_id = job_id
        self.task = None
        self.resource = None
        self.action = None
        self.status = JobStatus.CREATED
        self.nested_app = None
        self._history = [[timestamp, "job "+job_id+" created"]]

    def get_id(self):
        return self.job_id

    def get_resource(self):
        return self.resource

    def set_nested_app(self, app, timestamp):
        self.nested_app=app
        self._history.append([timestamp, "job "+self.job_id+" becomes app "+app.get_id()])

    def get_nested_app(self):
        return self.nested_app

    def bind_to_action(self, resource):
        self.resource = resource
        for action in self.task.action:
            if action.assigned_resource == resource:
                self.action = action
                action.jobs.append(self)

    def get_status(self):
        return self.status

    def stalled(self, timestamp):
        self.status = JobStatus.STALLED
        self._history.append([timestamp, "job "+self.job_id+" stalled"])

    def unstalled(self, timestamp):
        self.status = JobStatus.SUBMITTED
        self._history.append([timestamp, "job "+self.job_id+" resumes execution"])

    def submitted(self, timestamp):
        self.status = JobStatus.SUBMITTED
        self._history.append([timestamp, "job "+self.job_id+" submitted"])

    def completed(self, timestamp):
        self.status = JobStatus.COMPLETED
        self._history.append([timestamp, "job "+self.job_id+" completed"])

    def failed(self, timestamp):
        self.status = JobStatus.FAILED
        self._history.append([timestamp, "job "+self.job_id+" failed"])

    def get_history(self):
        history = []
        history = history + self._history
        return sorted(history, key=lambda t: int(t[0]))


class JobRegister:
    def __init__(self):
        self.jobs = {}  # value=jobs
        self.last_registered_job = None

    def register_job(self, job_id, timestamp):
        job = self.jobs.get(job_id)
        if job is None:
            job = Job(job_id, timestamp)
            self.jobs[job_id] = job
        self.last_registered_job = job
        return job

    def get_last_registered_job(self):
        return self.last_registered_job

    def get_job(self, job_id):
        return self.jobs.get(job_id)

    def get_jobs(self):
        return self.jobs.values()

    def __str__(self):
        string = ""
        for key, value in self.jobs.items():
            string = string + "\n * " + (str(key) + " -> " + str(value))

        return string


class Action(object):

    @staticmethod
    def create_action(type,description, state, timestamp):
        if type == "ExecutionAction":
            return ExecutionAction(timestamp, description, state.tasks)
        elif type == "StartWorkerAction":
            return StartWorkerAction(timestamp, description, state.resources)
        elif type == "BusyWorkerAction":
            return BusyWorkerAction(timestamp, description, state.resources)
        elif type == "IdleWorkerAction":
            return IdleWorkerAction(timestamp, description, state.resources)

    @staticmethod
    def obtain_action(type, description, state, timestamp):
        if type == "ExecutionAction":
            prev_action = ExecutionAction.obtain_previous_task_action(description, state.tasks)
            if prev_action is not None:
                return prev_action
            return ExecutionAction(timestamp, description, state.tasks)
        elif type == "StartWorkerAction":
            return StartWorkerAction(timestamp, description, state.resources)
        elif type == "BusyWorkerAction":
            return BusyWorkerAction(timestamp, description, state.resources)
        elif type == "IdleWorkerAction":
            return IdleWorkerAction(timestamp, description, state.resources)

    def __init__(self, type, timestamp):
        self.type = type
        self.assigned_resource = None

    def get_type(self):
        return self.type

    def assign_resource(self, resource):
        self.assigned_resource= resource

    def get_assigned_resource(self):
        return self.assigned_resource

    def hosted(self, resource, timestamp):
        self._history.append([timestamp, resource.name + " hosts " + str(self) + " execution"])

    def unhosted(self, resource, timestamp):
        self._history.append([timestamp, resource.name + " unhosts " + str(self) + " execution"])

    def meets(self, type, description):
        if type == "ExecutionAction":
            return self._meets(ActionType.EXECUTION_ACTION, description)
        elif type == "StartWorkerAction":
            return self._meets(ActionType.START_WORKER_ACTION, description)

    @abstractmethod
    def _meets(self, type, description):
        pass

    @abstractmethod
    def get_history(self):
        pass


class ExecutionAction(Action):
    def __init__(self, timestamp, description, tasks):
        super(ExecutionAction, self).__init__(ActionType.EXECUTION_ACTION, timestamp)
        task_id = description.split()[1][:-1]
        self.task = tasks.get_task(task_id)
        self.task.add_action(self)
        self.jobs = []
        self._history = [[timestamp, "created "+str(self)]]

    @staticmethod
    def obtain_previous_task_action(description, tasks):
        task_id = description.split()[1][:-1]
        task = tasks.get_task(task_id)
        return task.action[-1]

    def _meets(self, type, description):
        if type == self.get_type():
            task_id = description.split()[1][:-1]
            return self.task.task_id == task_id
        else:
            return False

    def get_jobs(self):
        return self.jobs

    def get_history(self):
        history = []
        history = history + self._history
        for job in self.jobs:
            history = history + job.get_history()
        return sorted(history, key=lambda t: int(t[0]))

    def __str__(self):
        return "ExecutionAction for task " + str(self.task.task_id)


class StartWorkerAction(Action):
    def __init__(self, timestamp, description, resources):
        super(StartWorkerAction, self).__init__(ActionType.START_WORKER_ACTION, timestamp)
        self.resource_name = description.split()[1][:-1]
        resource = resources.register_resource(self.resource_name, timestamp)
        self.assign_resource(resource)
        self._history = [[timestamp, "created "+str(self)]]

    def _meets(self, type, description):
        if type == self.get_type():
            worker = description.split()[1][:-1]
            return worker == self.resource_name
        else:
            return False

    def get_history(self):
        history = []
        history = history + self._history
        return sorted(history, key=lambda t: int(t[0]))

    def __str__(self):
        return "StartWorkerAction for resource " + self.resource_name

class BusyWorkerAction(Action):
    def __init__(self, timestamp, description, resources):
        super(BusyWorkerAction, self).__init__(ActionType.BUSY_WORKER_ACTION, timestamp)
        self.resource_name = description.split()[1][:-1]
        resource = resources.register_resource(self.resource_name, timestamp)
        self.assign_resource(resource)
        self._history = [[timestamp, "created "+str(self)]]

    def _meets(self, type, description):
        if type == self.get_type():
            worker = description.split()[1][:-1]
            return worker == self.resource_name
        else:
            return False

    def get_history(self):
        history = []
        history = history + self._history
        return sorted(history, key=lambda t: int(t[0]))

    def __str__(self):
        return "BusyWorkerAction for resource " + self.resource_name

class IdleWorkerAction(Action):
    def __init__(self, timestamp, description, resources):
        super(IdleWorkerAction, self).__init__(ActionType.Idle_WORKER_ACTION, timestamp)
        self.resource_name = description.split()[1][:-1]
        resource = resources.register_resource(self.resource_name, timestamp)
        self.assign_resource(resource)
        self._history = [[timestamp, "created "+str(self)]]

    def _meets(self, type, description):
        if type == self.get_type():
            worker = description.split()[1][:-1]
            return worker == self.resource_name
        else:
            return False

    def get_history(self):
        history = []
        history = history + self._history
        return sorted(history, key=lambda t: int(t[0]))

    def __str__(self):
        return "IdleWorkerAction for resource " + self.resource_name