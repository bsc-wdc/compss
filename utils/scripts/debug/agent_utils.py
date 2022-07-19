#!/usr/bin/python

# -*- coding: utf-8 -*-


# Imports
# For better print formatting
from __future__ import print_function

from execution_utils import ExecutionState
from log_utils import RuntimeLog


class ExternalJob:
    def __init__ (self, id):
        self.id = id
        self.app = None

    def get_id(self):
        return self.id

    def set_app(self, app):
        self.app = app

    def get_app(self):
        return self.app

    def __str__(self):
        return "ExtenalJob " + self.id 

class Agent():

    def __init__(self, name):
        self.name=name
        self.state=ExecutionState()
        self.external_jobs={}

    def set_name(self, name):
        self.name=name

    def get_name(self):
        return self.name

    def get_execution_state(self):
        return self.state

    def register_app(self, appId):
        return self.state.apps.register_app(appId)
    
    def get_apps(self):
        return self.state.apps.get_applications()
    
    def register_external_job(self, jobId):
        job = self.external_jobs.get(jobId)
        if job == None:
            job = ExternalJob(jobId)
            self.external_jobs[jobId] = job
        return job

    def get_external_jobs(self):
        return self.external_jobs.values()

    def get_external_job(self, jobId):
        return self.external_jobs.get(jobId)


class AgentRegister():

    def __init__(self, suffix=""):
        self.agents = {}
        self.suffix = suffix


    def register_agent(self, name):
        if not name.endswith(self.suffix):
            name = name+self.suffix
    
        agent = self.agents.get(name)
        if agent == None:
            agent = Agent(name)
            self.agents[name] = agent

        return agent

    def rename_agent(self, old_name, new_name):
        agent=self.agents.pop(old_name)
        self.agents[new_name] = agent
            


    def get_agent(self, name):
        if not name.endswith(self.suffix):
            name = name+self.suffix
    
        agent = self.agents.get(name)
        return agent

    def get_agents(self):
        return self.agents.values()