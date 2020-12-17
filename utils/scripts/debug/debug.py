#!/usr/bin/python

# -*- coding: utf-8 -*-


# Imports
# For better print formatting
from __future__ import print_function
from os import path
import sys

from execution_utils import ExecutionState
from log_utils import Parser
from task_utils import TaskState

state = ExecutionState()


def parse(log_file):
    with open(log_file) as log_file:

        time_stamp = ""
        date = ""
        logger = ""
        method = ""
        message = ""

        line = log_file.readline()
        while line:
            if line.startswith("[("):
                logger_end_pos = line.find("]")
                timestamp_end_pos = line.find(")")
                date_end_pos = line.find(")", timestamp_end_pos + 1)
                if (timestamp_end_pos < date_end_pos) and (date_end_pos < logger_end_pos):
                    event = Parser.parse_event(time_stamp, date, logger, method, message)
                    if event is not None:
                        event.apply(state)
                    time_stamp = line[2:timestamp_end_pos]
                    date = line[timestamp_end_pos + 2:date_end_pos]
                    logger = line[date_end_pos+1:logger_end_pos]
                    logger = logger.replace(" ", "")
                    method_start_pos = line.find("@", logger_end_pos)
                    message_start_pos = line.find("-", logger_end_pos)
                    method = line[method_start_pos+1:message_start_pos]
                    method = method.replace(" ", "")
                    message = line[message_start_pos+3:]
                else:
                    message = message + line
            else:
                message = message + line
            line = log_file.readline()

        event = Parser.parse_event(time_stamp, date, logger, method, message)
        if event is not None:
            event.apply(state)


def print_commands(file=None):
    print("Available Commands:\n" +
          " * c, current     -> print current status of the execution\n" +
          " * cn, connection -> print information related to a connection\n" +
          " * d, data        -> print information related to data values\n" +
          " * h, help        -> print available commands\n" +
          " * j, job         -> print information related to jobs\n" +
          " * q, quit        -> stop the script\n"
          " * r, resources   -> print information related to resources\n" +
          " * t, tasks       -> print information related to tasks\n" +
          " * u, update     -> re-parses the runtime.log file\n"
          , file=file)


def handle_queries(runtime_log_file):
    end_loop = False
    while not end_loop:
        input = raw_input()
        if len(input) == 0:
            print("Please, insert a command.", file=sys.stderr)
            print_commands(file=sys.stderr)
        else:
            input_array = input.split()
            if input_array[0] == "c" or input_array[0] == "current":
                print("Finished " + str(state.tasks.completed_tasks_count) + " out of " +
                      str(state.tasks.registered_tasks_count))
                if state.main_access is not None:

                    print("Main accessing " + str(state.main_access))

                for r in state.resources.get_resources():
                    print(str(r))
            elif input_array[0] == "cn" or input_array[0] == "connection":
                state.query_connection(input_array[1:])
            elif input_array[0] == "d" or input_array[0] == "data":
                state.query_data(input_array[1:])
            elif input_array[0] == "j" or input_array[0] == "job":
                state.query_job(input_array[1:])
            elif input_array[0] == "h" or input_array[0] == "help":
                print_commands()
            elif input_array[0] == "q" or input_array[0] == "quit":
                end_loop = True
            elif input_array[0] == "r" or input_array[0] == "resources":
                state.query_resource(input_array[1:])
            elif input_array[0] == "t" or input_array[0] == "tasks":
                state.query_task(input_array[1:])
            elif input_array[0] == "u" or input_array[0] == "update":
                state.clear()
                parse(runtime_log_file)
                print("Finished parsing "+runtime_log_file+". Script ready to reply queries")
            else:
                print("Unknown command " + input_array[0] + ".", file=sys.stderr)
                print_commands(file=sys.stderr)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Usage:\n"+
              "\t debug.py <runtime_log_file_path>"
              , file=sys.stderr)
        sys.exit(128)

    runtime_log_file = sys.argv[1]

    if not path.exists(runtime_log_file):
        print("Invalid parameter value:" + runtime_log_file + " does not exists.", file=sys.stderr)
        sys.exit(128)

    parse(runtime_log_file)
    print("Finished parsing "+runtime_log_file+". Script ready to reply queries")
    handle_queries(runtime_log_file)


