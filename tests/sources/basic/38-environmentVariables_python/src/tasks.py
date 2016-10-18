"""
@author: fconejer
  
PyCOMPSs Testbench Tasks
========================
"""
    
from pycompss.api.task import task
from pycompss.api.constraint import constraint
from pycompss.api.parameter import *


@constraint(computingUnits = "${computingUnits}",
            processorName = "${processorName}",
            processorSpeed = "${processorSpeed}",
            processorArchitecture = "${processorArchitecture}",
            processorPropertyName = "${processorPropertyName}",
            processorPropertyValue = "${processorPropertyValue}",
            memorySize = "${memorySize}",
            memoryType = "${memoryType}",
            storageSize = "${storageSize}",
            storageType = "${storageType}",
            operatingSystemType = "${operatingSystemType}",
            operatingSystemDistribution = "${operatingSystemDistribution}",
            operatingSystemVersion = "${operatingSystemVersion}",
            appSoftware = "${appSoftware}",
            hostQueues = "${hostQueues}",
            wallClockLimit = "${wallClockLimit}")
@task(returns = int)
def constrained_func(value):
    return value * value * value
