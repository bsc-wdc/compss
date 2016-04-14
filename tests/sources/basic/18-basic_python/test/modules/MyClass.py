from pycompss.api.task import task
from pycompss.api.parameter import *


class MyClass(object):
    static_field = 'value of static field'

    def __init__(self, field=None):
        self.field = field

    @task()
    def instance_method(self):
        self.field = self.field * 2

    @task(returns=int, isModifier=False)
    def instance_method_nonmodifier(self):
        return self.field

    @classmethod
    @task(returns=str)
    def class_method(cls):
        return cls.static_field
