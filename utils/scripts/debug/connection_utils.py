#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
from abc import abstractmethod
from enum import Enum

class ConnectionStatus(Enum):
    CREATED = 0
    ASSOCIATED = 1
    ESTABLISHED = 2
    CLOSING_SOCKET = 3
    CLOSED = 4

class Stage:
    def __init__(self, stage_name):
        self.name = stage_name


class Connection:
    def __init__(self, connection_id, timestamp):
        self.connection_id = connection_id
        self.socket_id = None
        self.status = ConnectionStatus.CREATED
        self._history = [[timestamp, "Connection "+connection_id + " created"]]
        self._stages = []
        self.current_stage = None

    def set_socket(self, socket_id, timestamp):
        self.socket_id = socket_id
        self.status = ConnectionStatus.ASSOCIATED
        self._history.append([timestamp, "Associating to socket " + self.socket_id])

    def established(self, timestamp):
        self.status = ConnectionStatus.ESTABLISHED
        self._history.append([timestamp, "Connection established"])

    def requesting_stage(self, stage_id, timestamp):
        stage = Stage(stage_id)
        self._stages.append(stage)
        self._history.append([timestamp, "Stage " + stage_id +" requested"])

    def handling_stage(self, stage_id, timestamp):
        stage = None
        for current_stage in self._stages:
            if current_stage.name == stage_id:
                stage = current_stage
        if stage is not None:
            self.current_stage = stage
        self._history.append([timestamp, "Stage " + stage_id +" being handled"])

    def starting_stage(self, stage_id, timestamp):
        self._history.append([timestamp, "Stage " + stage_id +" started"])

    def completed_stage(self, stage_id, timestamp):
        stage = None
        for current_stage in self._stages:
            if current_stage.name == stage_id:
                stage = current_stage
        if stage is not None:
            self.current_stage = None
            self._stages.remove(stage)
        self._history.append([timestamp, "Stage " + stage_id +" completed"])

    def closed_socket(self, timestamp):
        self.status = ConnectionStatus.CLOSED
        self._history.append([timestamp, "Socket closed"])

    def get_stages(self):
        stages = []
        stages = stages + self._stages
        return stages

    def get_history(self):
        history = []
        history = history + self._history
        return sorted(history, key=lambda t: int(t[0]))

    def __str__(self):
        return  "Connection" + str(self.connection_id)


class ConnectionRegister:
    def __init__(self):
        self.connections = {}  # value=connections

    def register_connection(self, connection_id, timestamp):
        connection = self.connections.get(connection_id)
        if connection is None:
            connection = Connection(connection_id, timestamp)
            self.connections[connection_id] = connection
        return connection

    def get_connection(self, connection_id):
        return self.connections.get(connection_id)

    def get_connections(self):
        return self.connections.values()

    def __str__(self):
        string = ""
        for key, value in self.connections.items():
            string = string + "\n * " + (str(key) + " -> " + str(value))

        return string
