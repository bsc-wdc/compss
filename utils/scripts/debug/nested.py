#!/usr/bin/python

# -*- coding: utf-8 -*-


# Imports
# For better print formatting
from __future__ import print_function
import os
import sys


from agent_utils import AgentRegister
from log_utils import AgentLog
from log_utils import RuntimeLog
from task_utils import TaskState
from action_utils import JobStatus


agents=AgentRegister()

def parse(log_folder, out_ext):
    for name in os.listdir(log_folder) :
        agent_path = log_folder + "/" + name
        if os.path.isdir(agent_path):
            if os.path.exists(agent_path + "/agent.log"):
                agent=agents.register_agent(name)
                expected_agent_name=agent.get_name()
                agent_log = AgentLog(agent_path + "/agent.log")
                agent_log.parse(agent)
                final_agent_name=agent.get_name()
                agents.rename_agent(expected_agent_name, final_agent_name)
                
                runtime_log = RuntimeLog(agent_path + "/runtime.log")
                runtime_log.parse(agent.get_execution_state())
            else:
                print("Ignoring folder " + agent_path, file=sys.stderr)

def get_task_state(task):
    if task.get_state() == TaskState.FAILED:
        return "❌"
    if task.get_state() == TaskState.COMPLETED:
        return "✔"
    return "?"

def get_job_state(job):
    if job.get_status() == JobStatus.FAILED:
        return "❌"
    if job.get_status() == JobStatus.COMPLETED:
        return "✔"
    return "?"

def trace_task(agent, task):
    trace = agent.get_name()+"{task"+task.get_id() + get_task_state(task) +","
    job=None
    
    actions = task.get_actions()
    for action in actions:
        jobs=action.get_jobs()
        for j in jobs:
            job=j
    if job is None:
        trace = trace + "Unassigned}"
        runner = None
        app= None
    else:
        trace = trace + "job"+job.get_id()+ get_job_state(job)+"}"
        runner = job.get_resource()
        runner = agents.register_agent(runner.get_name())
        if  runner == agent:
            app = job.get_nested_app()
        else:
            ej=runner.get_external_job(job.get_id())
            if ej is None:
                subtrace=runner.get_name()+"{Not arrived}"
                app = None
            else:
                e_app = ej.get_app()
                tasks=e_app.get_tasks()
                remote_task=None
                for t in tasks:
                    remote_task=t
                subtrace, runner, app = trace_task (runner, remote_task)

            trace = trace + "--> "+subtrace

    return trace, runner, app

def print_task(agent, task, tree=""):
    trace, runner, app = trace_task(agent, task)
    task_line=tree + task.get_method_name() + "-->" + trace
    print(task_line)
    tree=tree.replace("└──", "   ")
    tree=tree.replace("├──", "│  ")
    if app is not None:
        tasks = app.get_tasks()
        num_task=1
        for t in tasks:
            print_task(runner, t, tree+("└" if num_task == len(tasks) else "├")+"──")
            num_task = num_task + 1
    

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:\n"+
              "\t tree.py <log_path> <out_ext>"
              , file=sys.stderr)
        sys.exit(128)

    log_folder = sys.argv[1]
    if len(sys.argv) > 2:
        out_ext = sys.argv[2]
    else:
        out_ext = ".outputlog"

    parse(log_folder,  out_ext)

    main_job=None
    main_agent=None
    
    for agent in agents.get_agents():
        ej=agent.get_external_job("-")
        if ej is not None:
            main_agent=agent
            main_job=ej

    if not main_job is None:
        app=main_job.get_app()
        if app is not None:
            nested_tasks = app.get_tasks()
            for main_task in nested_tasks:
                print_task(main_agent, main_task)

