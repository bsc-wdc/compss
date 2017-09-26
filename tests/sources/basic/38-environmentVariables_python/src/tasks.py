"""
PyCOMPSs Testbench Tasks
========================
"""

from pycompss.api.task import task
from pycompss.api.constraint import constraint
from pycompss.api.parameter import *


@constraint(ComputingUnits = "${computingUnits}",
            ProcessorName = "${processorName}",
            ProcessorSpeed = "${processorSpeed}",
            ProcessorArchitecture = "${processorArchitecture}",
            ProcessorPropertyName = "${processorPropertyName}",
            ProcessorPropertyValue = "${processorPropertyValue}",
            MemorySize = "${memorySize}",
            MemoryType = "${memoryType}",
            StorageSize = "${storageSize}",
            StorageType = "${storageType}",
            OperatingSystemType = "${operatingSystemType}",
            OperatingSystemDistribution = "${operatingSystemDistribution}",
            OperatingSystemVersion = "${operatingSystemVersion}",
            AppSoftware = "${appSoftware}",
            HostQueues = "${hostQueues}",
            WallClockLimit = "${wallClockLimit}")
@task(returns = int)
def constrained_func(value):
    return value * value * value
