from pycompss.api.task import task
from pycompss.api.parameter import *


class MyClass(object):

    def __init__(self, a):
        self.a = a

    @task()
    def modify(self, x):
        self.a = self.a * x

    @task()
    def modify2(self, x):
        self.a = self.a - x
