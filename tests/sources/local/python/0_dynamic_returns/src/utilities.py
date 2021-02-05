# Import Python libraries
import math

from pycompss.api.api import compss_barrier_group, compss_wait_on, TaskGroup
from pycompss.api.task import task
from pycompss.api.parameter import *
####################################################################################################
############################################# CLASSES ##############################################
####################################################################################################

class UnfolderManager(object):
    """
    Class used to organize a list of values into a list of sublists. Referring to f as future type, this class allows to pass from [f, f, f, ... , f] to [[f, f, f, ... , f], ... ,[f, ... , f]]. The length of the original list is "number", while the length of each sublist is group.
    A method managing multiple contributions is present as well.

    Attributes:
    - number: number of values of original list
    - group: desired length of sublists of output list.
    - groups: number of sublists of output list.

    Methods:
    - UnfoldNValues_Task: task method calling UnfoldNValues.
    - UnfoldNValues: method creating the list of sublists.
    - PostprocessContributionsPerInstance: task method summing together multiple contributions, if any. After summing all contributions, it calls UnfoldNValues to create the list of sublists.
    """

    def __init__(self, number, group):
        self.groups = int(math.ceil(number/group))
        self.number=number
        self.group=group

    def _unfoldNValues(self, values):
        """
        Task method calling UnfoldNValues.

        Inputs:
        - self: an instance of the class.
        - values: original list of values.
        """
        print("values: " + str(values))
        list_out = []
        for i in range(self.groups):
            list_out.append(i)
        return list_out

    @task(target_direction=IN,returns='value_of(self.groups)')
    def UnfoldNValues_Task(self, values):
        return self._unfoldNValues(values)

    @task(target_direction=IN,returns='value_of(values)')
    def UnfoldNValues_Task_2(self, values):
        return self._unfoldNValues(values)

    @task(target_direction=IN,returns=1)
    def UnfoldNValues_Task_3(self, values):
        return self._unfoldNValues(values)
