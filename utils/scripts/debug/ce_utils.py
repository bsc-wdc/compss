#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
from abc import abstractmethod
from enum import Enum

class Implementation:
    def __init__ (self, signature, constraints):
        self.signature = signature
        self.constraints = constraints
        
    def get_constraints_cores(self):
        position = self.constraints.find("COMPUTING_UNITS")
        remaining = self.constraints[position+16:]
        return remaining.split()[0]
    
    def get_constraints_memory(self):
        position = self.constraints.find("MEMORY SIZE")
        remaining = self.constraints[position+12:]
        return remaining.split()[0]

    def __str__(self):
        return "Implementation " + self.signature


class CoreElement:

    def __init__(self, id, signature, timestamp):
        self.core_id = id
        self.signature = signature
        self.implementations = {}
        self._history = [[timestamp, "Registered CE "+self.signature]]

    def add_implementation(self, impl_signature, constraints, timestamp):
        impl = self.implementations.get(impl_signature)
        if impl is None:
            impl = Implementation(impl_signature, constraints)
            self.implementations[impl_signature] = impl
            self._history.append([timestamp, "Registered implementation "+impl_signature])
    
    def get_implementations(self):
        return self.implementations.values()

    def get_history(self):
        return sorted(self._history, key=lambda t: int(t[0]))

    def __str__(self):
        string = "Core Element " + str(self.core_id) + "(" + self.signature + ")"
        return string


class CoreElementRegister:

    def __init__(self):
        self.core_count = 0
        self.core_elements_by_id=[]
        self.core_elements_by_signature={}

    def register_core_element(self, ce_signature, timestamp):
        core_element = self.core_elements_by_signature.get(ce_signature)
        if core_element is None:
            core_element = CoreElement(self.core_count, ce_signature,timestamp)
            self.core_elements_by_signature[ce_signature] = core_element
            self.core_elements_by_id.append(core_element)
            self.core_count = self.core_count + 1
        return core_element

    def get_core_elements(self):
        return self.core_elements_by_id

    def get_core_element_by_id(self, core_id):
        if core_id >= 0 and core_id < len(self.core_elements_by_id):
            return self.core_elements_by_id[core_id]
        else:
            return None

    def get_core_element_by_signature(self, signature):
        return self.core_elements_by_signature.get(signature)
