#!/usr/bin/python

# -*- coding: utf-8 -*-

# Imports
# For better print formatting
from __future__ import print_function

from abc import abstractmethod

from execution_utils import ExecutionState
from enum import Enum

from task_utils import TaskState
from action_utils import Action


class Loggers:
    class TaskProcessor:
        label = "TaskProcessor"

        class FileAccess:
            label = "ainAccessToFile"
            WAITING = "waiting until"
            OBTAINED = "located on"

        class ObjectAccess:
            label = "nAccessToObject"
            WAITING = "Waiting for"
            OBTAINED = "retrieved"

        class WaitEnds:
            label = "waitForTask"
            END = "End of waited task for data"

    class TaskAnalyser:
        label = "TaskAnalyser"

        class ProcessTask:
            label = "processTask"
            NEW_TASK = "New method task"

        class InputValue:
            label = "pendencyForRead"
            DEFINITION = "Checking"

        class AddDependency:
            label = "gularDependency"
            REGISTERED = "Adding dependency"

        class OutputValue:
            label = "terOutputValues"
            DEFINITION = "Checking"

        class EndTask:
            label = "endTask"
            NOTIFICATION = "Notification received"

    class DataInfoProvider:
        label = "DataInfoProvider"

        class RegisteringAccess:
            label = "willAccess"
            REGISTER = "Access:"

    class TaskScheduler:
        label = "TaskScheduler"

        class ScheduleAction:
            label = "scheduleAction"
            SCHEDULE = " Schedule Action"

        class CreateAction:
            label = "locatableAction"
            CREATE = "Registering new AllocatableAction"

        class AssignAction:
            label = "schedule"
            ASSIGN = "Assigning action"

        class HostAction:
            label = "hostAction"
            HOST = "Host action"

        class UnhostAction:
            label = "unhostAction"
            UNHOST = "Unhost action"

    class JobManager:
        label = "JobManager"

        class NewJob:
            label = "doSubmit"
            NEW = "New Job"
            TARGET_HOST = "Target host"

        class SubmittedJob:
            label = "processRequests"
            SUBMITTED = "submitted"

        class CompletedJob:
            label = "completedJob"
            COMPLETED = "Received a notification for job"

        class FailedJob:
            label = "failedJob"
            FAILED = "Received a notification for job"

    class Connection:
        label = "Connection"

        class RegisterChannel:
            label = "registerChannel"
            ASSOCIATING = "Associating Socket"
            UNREGISTERING = "Unregistering Socket"

        class EstablishedChannel:
            label = "established"
            ESTABLISHED = "established."

        class RequestStage:
            label = "requestStage"
            REQUESTING = "Requesting"

        class HandleNextTransfer:
            label = "dleNextTransfer"
            TAKES = "takes"

        class StartCurrentTransfer:
            label = "CurrentTransfer"
            STARTING = "Starting"

    class Stage:
        label = "Stage"

        class CompletedStage:
            label = "otifyCompletion"
            COMPLETED = "completed in connection"

class Parser:
    """
    Class to handle the parsing of each log line and convert it into messages
    """

    @staticmethod
    def parse_event(timestamp, date, logger, method, message):
        """

        :param timestamp: moment when the event happened
        :param date: date of the event
        :param logger: logger that registered the event
        :param method: method where the event arised
        :param message: message in the log

        :return: Event representing the line
        """
        event = None
        if logger == Loggers.TaskProcessor.label:
            if method == Loggers.TaskProcessor.FileAccess.label:
                if Loggers.TaskProcessor.FileAccess.WAITING in message:
                    event = FileAccessEvent(timestamp, message)
                if Loggers.TaskProcessor.FileAccess.OBTAINED in message:
                    event = ObtainedFileEvent(timestamp, message)
            if method == Loggers.TaskProcessor.ObjectAccess.label:
                if Loggers.TaskProcessor.ObjectAccess.WAITING in message:
                    event = ObjectAccessEvent(timestamp, message)
                if Loggers.TaskProcessor.ObjectAccess.OBTAINED in message:
                    event = ObtainedObjectEvent(timestamp, message)
            if method == Loggers.TaskProcessor.WaitEnds.label:
                if Loggers.TaskProcessor.WaitEnds.END in message:
                    event = WaitedTaskEndEvent(timestamp, message)

        if logger == Loggers.TaskAnalyser.label:
            if method == Loggers.TaskAnalyser.ProcessTask.label:
                if Loggers.TaskAnalyser.ProcessTask.NEW_TASK in message:
                    event = NewTaskEvent(timestamp, message)
            if method == Loggers.TaskAnalyser.InputValue.label:
                if Loggers.TaskAnalyser.InputValue.DEFINITION in message:
                    event = InputValueEvent(timestamp, message)
            if method == Loggers.TaskAnalyser.AddDependency.label:
                if Loggers.TaskAnalyser.AddDependency.REGISTERED in message:
                    event = AddDependencyEvent(timestamp, message)
            if method == Loggers.TaskAnalyser.OutputValue.label:
                if Loggers.TaskAnalyser.OutputValue.DEFINITION in message:
                    event = OutputValueEvent(timestamp, message)
            if method == Loggers.TaskAnalyser.EndTask.label:
                if Loggers.TaskAnalyser.EndTask.NOTIFICATION in message:
                    event = EndTaskEvent(timestamp, message)

        if logger == Loggers.DataInfoProvider.label:
            if method == Loggers.DataInfoProvider.RegisteringAccess.label:
                if Loggers.DataInfoProvider.RegisteringAccess.REGISTER in message:
                    event = RegisteringAccessEvent(timestamp, message)

        if logger == Loggers.TaskScheduler.label:
            if method == Loggers.TaskScheduler.CreateAction.label:
                if Loggers.TaskScheduler.CreateAction.CREATE in message:
                    event = CreateActionEvent(timestamp, message)
            if method == Loggers.TaskScheduler.AssignAction.label:
                if Loggers.TaskScheduler.AssignAction.ASSIGN in message:
                    event = AssignActionEvent(timestamp, message)
            if method == Loggers.TaskScheduler.HostAction.label:
                if Loggers.TaskScheduler.HostAction.HOST in message:
                    event = HostActionEvent(timestamp, message)
            if method == Loggers.TaskScheduler.UnhostAction.label:
                if Loggers.TaskScheduler.UnhostAction.UNHOST in message:
                    event = UnhostActionEvent(timestamp, message)

        if logger == Loggers.JobManager.label:
            if method == Loggers.JobManager.NewJob.label:
                if Loggers.JobManager.NewJob.NEW in message:
                    event = CreateJobEvent(timestamp, message)
                if Loggers.JobManager.NewJob.TARGET_HOST in message:
                    event = SettingHostToJobEvent(timestamp, message)
            if method == Loggers.JobManager.SubmittedJob.label:
                if Loggers.JobManager.SubmittedJob.SUBMITTED in message:
                    event = SubmittedJobEvent(timestamp, message)
            if method == Loggers.JobManager.CompletedJob.label:
                if Loggers.JobManager.CompletedJob.COMPLETED in message:
                    event = CompletedJobEvent(timestamp, message)
            if method == Loggers.JobManager.FailedJob.label:
                if Loggers.JobManager.FailedJob.FAILED in message:
                    event = CompletedJobEvent(timestamp, message)

        if logger == Loggers.Connection.label:
            if method == Loggers.Connection.RegisterChannel.label:
                if Loggers.Connection.RegisterChannel.ASSOCIATING in message:
                    event = AssociateSocketEvent(timestamp, message)
                if Loggers.Connection.RegisterChannel.UNREGISTERING in message:
                    event = ClosedSocketEvent(timestamp, message)
            if method == Loggers.Connection.EstablishedChannel.label:
                if Loggers.Connection.EstablishedChannel.ESTABLISHED in message:
                    event = EstablishedSocketEvent(timestamp, message)
            if method == Loggers.Connection.RequestStage.label:
                if Loggers.Connection.RequestStage.REQUESTING in message:
                    event = RequestStageEvent(timestamp, message)
            if method == Loggers.Connection.HandleNextTransfer.label:
                if Loggers.Connection.HandleNextTransfer.TAKES in message:
                    event = HandleNextStageEvent(timestamp, message)
            if method == Loggers.Connection.StartCurrentTransfer.label:
                if Loggers.Connection.StartCurrentTransfer.STARTING in message:
                    event = StartCurrentStageEvent(timestamp, message)

        if logger == Loggers.Stage.label:
            if method == Loggers.Stage.CompletedStage.label:
                if Loggers.Stage.CompletedStage.COMPLETED in message:
                    event = CompletedStageEvent(timestamp, message)
        return event


class Event(object):
    """
    Log entry
    """
    def __init__(self, timestamp):
        self.timestamp = timestamp

    @abstractmethod
    def apply(self, state):
        pass


# Task Analyser Events
class NewTaskEvent(Event):
    """
    Task Analyser gets a new task
    """

    def __init__(self, timestamp, message):
        """
        Constructs a new NewTaskEvent out of the message printed in the log

        :param timestamp:
        :param message: Task creation event text
        """
        super(NewTaskEvent, self).__init__(timestamp)
        line_array = message.split()
        self.task_id = line_array[5]
        method_name_start = message.find("(")
        method_name_end = message.find(")")
        self.method_name = message[method_name_start+1:method_name_end]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        state.tasks.register_task(self.task_id, self.method_name, self.timestamp)

    def __str__(self):
        return self.method_name+"(task "+self.task_id+") @ "+self.timestamp


class InputValueEvent(Event):
    """
    Task Analyser prints an input parameter for the last registered task
    """
    def __init__(self, timestamp, message):
        """
        Constructs a new InputValueEvent out of the message printed in the log

        :param timestamp:
        :param message: Input parameter event description
        """
        super(InputValueEvent, self).__init__(timestamp)
        line_array = message.split()
        self.datum_id = line_array[5]
        self.task_id = line_array[8]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        data = state.data.register_data(self.datum_id, self.timestamp)
        task = state.tasks.get_task(self.task_id)
        access = state.data.last_registered_access
        p = task.add_parameter(access)
        access.set_cause(p)
        access.register_read(data, self.timestamp)

    def __str__(self):
        return self.direction+" parameter ("+self.datum_id+" for task " + self.task_id + " @ "+self.timestamp


class AddDependencyEvent(Event):
    """
    Task Analyser registers a dependency among tasks
    """
    def __init__(self, timestamp, message):
        """
        Constructs a new AddDependencyEvent out of the message printed in the log

        :param timestamp:
        :param message: Dependency registration event description
        """
        super(AddDependencyEvent, self).__init__(timestamp)
        line_array = message.split()
        self.predecessor_id = line_array[4]
        self.successor_id = line_array[7]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        successor = state.tasks.get_task(self.successor_id)
        p = successor.get_last_registered_parameter()
        predecessor = state.tasks.get_task(self.predecessor_id)
        p.set_confirmed_dependency(predecessor)

    def __str__(self):
        return "Task " + self.successor_id + " depends on " + self.predecessor_id + " @ "+self.timestamp


class OutputValueEvent(Event):
    """
    Task Analyser prints an output parameter for the last registered task
    """
    def __init__(self, timestamp, message):
        """
        Constructs a new OutputValueEvent out of the message printed in the log

        :param timestamp:
        :param message: Input parameter event description
        """
        super(OutputValueEvent, self).__init__(timestamp)
        line_array = message.split()
        self.datum_id = line_array[5]
        self.task_id = line_array[8]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        task = state.tasks.get_task(self.task_id)
        access = state.data.last_registered_access
        p = None
        if access.get_direction() == "OUT":
            p = task.add_parameter(access)
            access.set_cause(p)
        else:
            p = task.get_last_registered_parameter()

        d = state.data.register_data(self.datum_id, self.timestamp)
        access.register_write(d, self.timestamp)

    def __str__(self):
        return self.direction+" parameter ("+self.datum_id+" for task " + self.task_id + " @ "+self.timestamp


class EndTaskEvent(Event):
    """
    Task Analyser gets a task completion notification
    """

    def __init__(self, timestamp, message):
        """
        Constructs a new NewTaskEvent out of the message printed in the log

        :param timestamp:
        :param message: Task completion event description
        """
        super(EndTaskEvent, self).__init__(timestamp)
        line_array = message.split()
        self.task_id = line_array[4]
        self.task_status = line_array[8]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        tasks = state.tasks
        task = tasks.get_task(self.task_id)
        if self.task_status.upper() == "FINISHED":
            task.completed(self.timestamp)
            tasks.completed_tasks_count += 1
        elif self.task_status.upper() == "FAILED":
            task.failed(self.timestamp)
            tasks.failed_tasks_count += 1
        else:
            print("UNKNOWN STATUS (" + self.task_status + ") FOR TASK " + self.task_id)

    def __str__(self):
        return "Completed task " + self.task_id + " @ " + self.timestamp


# Data Info provider Events
class RegisteringAccessEvent(Event):
    """
    Registers a new data access
    """
    def __init__(self, timestamp, message):
        """
        Constructs a new RegisteringAccessEvent out of the message printed in the log

        :param timestamp:
        :param message: access event description
        """
        super(RegisteringAccessEvent, self).__init__(timestamp)
        lines_array = message.split("\n")
        self.direction = lines_array[1].split()[2]

        if self.direction == "R":
            self.read_datum = lines_array[2].split()[3]
            self.writen_datum = None
        elif self.direction == "W":
            self.read_datum = None
            self.writen_datum = lines_array[2].split()[3]
        else:
            self.read_datum = lines_array[2].split()[3]
            self.writen_datum = lines_array[3].split()[3]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        state.data.register_access(self.direction, self.timestamp)

    def __str__(self):
        return "Registering " + self.direction + "access " + str(self.read_datum) + "->" + str(self.writen_datum) + "@ " + self.timestamp


# Task Scheduler Events
class CreateActionEvent(Event):
    """
    Task Scheduler registers a new Action
    """
    def __init__(self, timestamp, message):
        """
        Constructs a new CreateActionEvent out of the message printed in the log

        :param timestamp:
        :param message: Action creation event description
        """
        super(CreateActionEvent, self).__init__(timestamp)
        line_array = message.split()
        desc_start_pos = message.find("(")
        desc_end_pos = message.find(")")
        self.action_type = line_array[4]
        self.description = message[desc_start_pos:desc_end_pos + 1]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        Action.create_action(self.action_type, self.description, state, self.timestamp)

    def __str__(self):
        return "Creating action for " + self.description + " @ "+self.timestamp


class AssignActionEvent(Event):
    """
    Task Scheduler registers an Action assignment
    """
    def __init__(self, timestamp, message):
        """
        Constructs a new AssignActionEvent out of the message printed in the log

        :param timestamp:
        :param message: Action assignment event description
        """
        super(AssignActionEvent, self).__init__(timestamp)
        line_array = message.split()
        desc_start_pos = message.find("(")
        desc_end_pos = message.find(")")
        self.action_type = line_array[2]
        self.description = message[desc_start_pos:desc_end_pos + 1]
        self.resource_name = message[desc_end_pos + 2:-1].split()[2]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        action = Action.obtain_action(self.action_type, self.description, state, self.timestamp)
        resource = state.resources.get_resource(self.resource_name)
        action.assign_resource(resource)

    def __str__(self):
        return "Assigning action for " + self.description + " to " + self.resource_name + " @ "+self.timestamp


class HostActionEvent(Event):
    """
    Task Scheduler registers that a resource starts hosting an action
    """
    def __init__(self, timestamp, message):
        """
        Constructs a new HostActionEvent out of the message printed in the log

        :param timestamp:
        :param message: Action hosting event description
        """
        super(HostActionEvent, self).__init__(timestamp)
        line_array = message.split()
        self.action_type = line_array[3]
        desc_start_pos = message.find("(")
        desc_end_pos = message.find(")")
        self.description = message[desc_start_pos:desc_end_pos + 1]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        action = Action.obtain_action(self.action_type, self.description, state, self.timestamp)
        resource = action.get_assigned_resource()
        if resource is not None:
            resource.host(action, self.timestamp)
            action.hosted(resource, self.timestamp)
        else:
            print("Unassigned resource to action " + str(action))

    def __str__(self):
        return self.resource_name + " hosts action " + self.description + " @ "+self.timestamp


class UnhostActionEvent(Event):
    """
    Task Scheduler registers that a resource starts hosting an action
    """
    def __init__(self, timestamp, message):
        """
        Constructs a new UnhostActionEvent out of the message printed in the log

        :param timestamp:
        :param message: Action unhosting event description
        """
        super(UnhostActionEvent, self).__init__(timestamp)
        line_array = message.split()
        desc_start_pos = message.find("(")
        desc_end_pos = message.find(")")
        self.action_type = line_array[3]
        self.description = message[desc_start_pos:desc_end_pos + 1]
        self.resource_name = line_array[-1]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        resource = state.resources.get_resource(self.resource_name)
        for action in resource.get_host_actions():
            if action.meets(self.action_type, self.description):
                resource.unhost(action, self.timestamp)
                action.unhosted(resource, self.timestamp)

    def __str__(self):
        return self.resource_name + " stops hosting action " + self.description + " @ "+self.timestamp


# Job Manager Events
class CreateJobEvent(Event):
    """
    Job Manager creates a new job
    """

    def __init__(self, timestamp, message):
        """
        Constructs a new CreateJobEvent out of the message printed in the log

        :param timestamp:
        :param message: job creation event text
        """
        super(CreateJobEvent, self).__init__(timestamp)
        line_array = message.split()
        self.job_id = line_array[2]
        self.task_id = line_array[4][:-1]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        task = state.tasks.get_task(self.task_id)
        job = state.jobs.register_job(self.job_id, self.timestamp)
        job.task = task

    def __str__(self):
        return "New Job (" + self.job_id + ") for task " + self.task_id + " @ " + self.timestamp


class SettingHostToJobEvent(Event):
    """
    After Job Manager creates a new job, it assigns a resource to it
    """

    def __init__(self, timestamp, message):
        """
        Constructs a new SettingHostToJobEvent out of the message printed in the log

        :param timestamp:
        :param message: job resource assignment event description
        """
        super(SettingHostToJobEvent, self).__init__(timestamp)
        message = message[4:]
        line_array = message.split()
        self.resource_name = line_array[-1]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        resource = state.resources.get_resource(self.resource_name)
        last_job = state.jobs.get_last_registered_job()
        last_job.bind_to_action(resource)

    def __str__(self):
        return "Job " + self.job_id + " runs on " + self.resource_name + " @ " + self.timestamp


class SubmittedJobEvent(Event):
    """
    Job Manager submits a job
    """

    def __init__(self, timestamp, message):
        """
        Constructs a new SubmittedJobEvent out of the message printed in the log

        :param timestamp:
        :param message: job submission event description
        """
        super(SubmittedJobEvent, self).__init__(timestamp)
        line_array = message.split()
        self.job_id = line_array[1]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        job = state.jobs.get_job(self.job_id)
        job.submitted(self.timestamp)

    def __str__(self):
        return "Job " + self.job_id + " submitted @ " + self.timestamp


class CompletedJobEvent(Event):
    """
    Job Manager receives the notification of a job end
    """

    def __init__(self, timestamp, message):
        """
        Constructs a new CompletedJobEvent out of the message printed in the log

        :param timestamp:
        :param message: job completion notification event description
        """
        super(CompletedJobEvent, self).__init__(timestamp)
        line_array = message.split()
        self.job_id = line_array[5]
        self.status = line_array[8]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        job = state.jobs.get_job(self.job_id)
        if self.status == "OK":
            job.completed(self.timestamp)
        else:
            job.failed(self.timestamp)

    def __str__(self):
        return "Job " + self.job_id + " finished with status " + self.status + " @ " + self.timestamp


# Main accesses data
class FileAccessEvent(Event):
    """
    Application accesses a file
    """

    def __init__(self, timestamp, message):
        """
        Constructs a new FileAccessEvent out of the message printed in the log

        :param timestamp:
        :param message: file access description
        """
        super(FileAccessEvent, self).__init__(timestamp)
        line_array = message.split()
        self.data_id = None
        if "File not accessed before" not in message:
            if "waiting" in message:
                self.data_id = line_array[1]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        if self.data_id is not None:
            state.main_accesses_data(self.data_id, self.timestamp)

    def __str__(self):
        if self.data_id is None:
            return "Main accesses a data value out of the COMPSs system or is a subsequent access to the value"
        else:
            return "Main accesses file " + self.data_id + " @ " + self.timestamp


# Main accesses data
class ObjectAccessEvent(Event):
    """
    Application accesses a file
    """

    def __init__(self, timestamp, message):
        """
        Constructs a new ObjectAccessEvent out of the message printed in the log

        :param timestamp:
        :param message: file access description
        """
        super(ObjectAccessEvent, self).__init__(timestamp)
        line_array = message.split()
        self.data_id = None
        if "Object not accessed before" not in message:
            if "Waiting" in message:
                self.data_id = line_array[5]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        if self.data_id is not None:
            state.main_accesses_data(self.data_id, self.timestamp)

    def __str__(self):
        if self.data_id is None:
            return "Main accesses a data value out of the COMPSs system or is a subsequent access to the value"
        else:
            return "Main accesses object " + self.data_id + " @ " + self.timestamp

class WaitedTaskEndEvent(Event):
    """
    The task that the main code waits for has finished
    """

    def __init__(self, timestamp, message):
        """
        Constructs a new WaitedTaskEndEvent out of the message printed in the log

        :param timestamp:
        :param message: producing task completion description
        """
        super(WaitedTaskEndEvent, self).__init__(timestamp)
        line_array = message.split()
        self.data_id = line_array[6]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        if self.data_id is not None:
            state.main_access.exists(self.timestamp)

    def __str__(self):
        return "Task for accessing data " + self.data_id + "has finished"


class ObtainedFileEvent(Event):
    """
    The data value that the main code waits for has been obtained
    """

    def __init__(self, timestamp, message):
        """
        Constructs a new ObtainedFileEvent out of the message printed in the log

        :param timestamp:
        :param message: producing task completion description
        """
        super(ObtainedFileEvent, self).__init__(timestamp)
        line_array = message.split()
        self.data_id = line_array[2]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        state.main_access.obtained(self.timestamp)
        state.main_access = None

    def __str__(self):
        return "Obtained data " + self.data_id + " for the access"


class ObtainedObjectEvent(Event):
    """
    The data value that the main code waits for has been obtained
    """

    def __init__(self, timestamp, message):
        """
        Constructs a new ObtainedObjectEvent out of the message printed in the log

        :param timestamp:
        :param message: object retrieval description
        """
        super(ObtainedObjectEvent, self).__init__(timestamp)

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        state.main_access.obtained(self.timestamp)
        state.main_access = None

    def __str__(self):
        return "Obtained data for the access"

# Connection Management
class AssociateSocketEvent(Event):
    """
    The runtime starts a new Connection based on a socket
    """

    def __init__(self, timestamp, message):
        """
        Constructs a new AssociateSocketEvent out of the message printed in the log

        :param timestamp:
        :param message: connection-socket relationship
        """
        super(AssociateSocketEvent, self).__init__(timestamp)
        line_array = message.split()
        self.socket_id = line_array[2]
        self.connection_id = line_array[5]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        connection = state.connections.register_connection(self.connection_id, self.timestamp)
        connection.set_socket(self.socket_id, self.timestamp)

    def __str__(self):
        return "Associating Connection "+str(self.connection_id)+" to socket "+str(self.socket_id)

class EstablishedSocketEvent(Event):
    """
    The socket connection has been established
    """

    def __init__(self, timestamp, message):
        """
        Constructs a new EstablishedSocketEvent out of the message printed in the log

        :param timestamp:
        :param message: socket establishment notification
        """
        super(EstablishedSocketEvent, self).__init__(timestamp)
        line_array = message.split()
        self.connection_id = line_array[1]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        connection = state.connections.get_connection(self.connection_id)
        connection.established(self.timestamp)

    def __str__(self):
        return "Socket for connection "+str(self.connection_id)+" established"


class RequestStageEvent(Event):
    """
    A new stage has been requested to the connection
    """

    def __init__(self, timestamp, message):
        """
        Constructs a new RequestStageEvent out of the message printed in the log

        :param timestamp:
        :param message: requested stage description
        """
        super(RequestStageEvent, self).__init__(timestamp)
        line_array = message.split()
        self.connection_id = line_array[-1]
        self.stage = line_array[2]
        self.stage = self.stage[18:]
        self.stage = self.stage.split('[')[0]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        connection = state.connections.get_connection(self.connection_id)
        connection.requesting_stage(self.stage, self.timestamp)

    def __str__(self):
        return "Requesting stage " + self.stage + " to connection "+str(self.connection_id)

class HandleNextStageEvent(Event):
    """
    The connection takes the following stage
    """

    def __init__(self, timestamp, message):
        """
        Constructs a new HandleNextStageEvent out of the message printed in the log

        :param timestamp:
        :param message: requested stage description
        """
        super(HandleNextStageEvent, self).__init__(timestamp)
        line_array = message.split()
        self.connection_id = line_array[1]
        self.stage = line_array[3]
        self.stage = self.stage[18:]
        self.stage = self.stage.split('[')[0]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        connection = state.connections.get_connection(self.connection_id)
        connection.handling_stage(self.stage, self.timestamp)

    def __str__(self):
        return "Connection "+str(self.connection_id)+" handles "+str(self.stage)

class StartCurrentStageEvent(Event):
    """
    The connection takes the following stage
    """

    def __init__(self, timestamp, message):
        """
        Constructs a new StartCurrentStageEvent out of the message printed in the log

        :param timestamp:
        :param message: requested stage description
        """
        super(StartCurrentStageEvent, self).__init__(timestamp)
        line_array = message.split()
        self.connection_id = line_array[-1]
        self.connection_id = self.connection_id[:-1]
        self.stage = line_array[4]
        self.stage = self.stage[18:]
        self.stage = self.stage.split('[')[0]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        connection = state.connections.get_connection(self.connection_id)
        connection.starting_stage(self.stage, self.timestamp)

    def __str__(self):
        return "Connection "+str(self.connection_id)+" starts "+str(self.stage)

class CompletedStageEvent(Event):
    """
    A stage being handled by a connection finishes
    """

    def __init__(self, timestamp, message):
        """
        Constructs a new CompletedStageEvent out of the message printed in the log

        :param timestamp:
        :param message: completed stage description
        """
        super(CompletedStageEvent, self).__init__(timestamp)
        line_array = message.split()
        self.connection_id = line_array[-1]
        self.stage = line_array[2]
        self.stage = self.stage[18:]
        self.stage = self.stage.split('[')[0]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        connection = state.connections.get_connection(self.connection_id)
        connection.completed_stage(self.stage, self.timestamp)

    def __str__(self):
        return "Connection "+str(self.connection_id)+" finishes "+str(self.stage)

class ClosedSocketEvent(Event):
    """
    Socket closed
    """

    def __init__(self, timestamp, message):
        """
        Constructs a new ClosedSocketEvent out of the message printed in the log

        :param timestamp:
        :param message: completed stage description
        """
        super(ClosedSocketEvent, self).__init__(timestamp)
        line_array = message.split()
        self.connection_id = line_array[-1]

    def apply(self, state):
        """
        Updates the execution state according to the event

        :param state: current execution state
        """
        connection = state.connections.get_connection(self.connection_id)
        connection.closed_socket(self.timestamp)

    def __str__(self):
        return "Connection "+str(self.connection_id)+" closed its socket "
