#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
from pycompss.api.task import task
from pycompss.api.constraint import constraint


@constraint(ComputingUnits="${computingUnits}",
            ProcessorName="${processorName}",
            ProcessorSpeed="${processorSpeed}",
            ProcessorArchitecture="${processorArchitecture}",
            ProcessorPropertyName="${processorPropertyName}",
            ProcessorPropertyValue="${processorPropertyValue}",
            MemorySize="${memorySize}",
            MemoryType="${memoryType}",
            StorageSize="${storageSize}",
            StorageType="${storageType}",
            OperatingSystemType="${operatingSystemType}",
            OperatingSystemDistribution="${operatingSystemDistribution}",
            OperatingSystemVersion="${operatingSystemVersion}",
            AppSoftware="${appSoftware}",
            HostQueues="${hostQueues}",
            WallClockLimit="${wallClockLimit}")
@task(returns=int)
def constrained_func(value):
    return value * value * value
