#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
from pycompss.api.task import task
from pycompss.api.constraint import constraint


@constraint(computing_units="${computingUnits}",
            processor_name="${processorName}",
            processor_speed="${processorSpeed}",
            processor_architecture="${processorArchitecture}",
            processor_property_name="${processorPropertyName}",
            processor_property_value="${processorPropertyValue}",
            memory_size="${memorySize}",
            memory_type="${memoryType}",
            storage_size="${storageSize}",
            storage_type="${storageType}",
            storageBW="${storageBW}",
            operating_system_type="${operatingSystemType}",
            operating_system_distribution="${operatingSystemDistribution}",
            operating_system_version="${operatingSystemVersion}",
            app_software="${appSoftware}",
            host_queues="${hostQueues}",
            wall_clock_limit="${wallClockLimit}")
@task(returns=int)
def constrained_func(value):
    return value * value * value
