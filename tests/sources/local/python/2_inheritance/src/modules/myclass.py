"""
PyCOMPSs Testbench Main Classes
===============================
"""

# Imports
from pycompss.api.task import task

class myClass(object):

    static_value = 4321

    def __init__(self):
        self.value = 1234

    @task(returns = int)
    def increment_non_modifier(self, v):
        print("self.value: ", self.value)
        print("V: ", v)
        return self.value + v

    @task()
    def increment_modifier(self, v):
        print("self.value: ", self.value)
        print("V: ", v)
        self.value = self.value + v

    @classmethod
    @task(returns = int)
    def increment(cls, v):
        return cls.static_value + v

    def get_value(self):
        return self.value

class inheritedClass(myClass):

    def __init__(self):
        super(inheritedClass, self).__init__()

class inheritedClassWithOverride(myClass):

    def __init__(self):
        super(inheritedClassWithOverride, self).__init__()

    @task(returns = int)
    def increment_non_modifier(self, v):
        print("self.value: ", self.value)
        print("V: ", v)
        return 2 * (self.value + v)

    @task()
    def increment_modifier(self, v):
        print("self.value: ", self.value)
        print("V: ", v)
        self.value = 2 * (self.value + v)

    @classmethod
    @task(returns = int)
    def increment(cls, v):
        return 2 * (cls.static_value + v)

class inheritedClassExtended(myClass):

    def __init__(self):
        super(inheritedClassExtended, self).__init__()

    @task(returns = int)
    def multiplier_non_modifier(self, v):
        print("self.value: ", self.value)
        print("V: ", v)
        return self.value * v

    @task()
    def multiplier_modifier(self, v):
        print("self.value: ", self.value)
        print("V: ", v)
        self.value = self.value * v

    @classmethod
    @task(returns = int)
    def multiplier(cls, v):
        return cls.static_value * v

class inheritedClassMultilevelOverridedExtended(inheritedClassWithOverride):

    def __init__(self):
        super(inheritedClassMultilevelOverridedExtended, self).__init__()

    @task(returns = int)
    def divider_non_modifier(self, v):
        print("self.value: ", self.value)
        print("V: ", v)
        return self.value / v

    @task()
    def divider_modifier(self, v):
        print("self.value: ", self.value)
        print("V: ", v)
        self.value = self.value / v

    @classmethod
    @task(returns = int)
    def divider(cls, v):
        return cls.static_value / v

class nestedInheritance(object):
    def __init__(self):
        self.mc = myClass()

class myOtherClass(object):
    other_static_value = 5432

    def __init__(self, other_value):
        self.other_value = other_value

    @task(returns = int)
    def decrement_non_modifier(self, v):
        print("self.other_value: ", self.other_value)
        print("V: ", v)
        return self.other_value - v

    @task()
    def decrement_modifier(self, v):
        print("self.other_value: ", self.other_value)
        print("V: ", v)
        self.other_value = self.other_value - v

    @classmethod
    @task(returns = int)
    def decrement(cls, v):
        return cls.other_static_value - v

    def get_other_value(self):
        return self.other_value

class joinerClass(object):
    @task()
    def multiplier_modifier(self):
        print("self.value: ", self.value)
        print("self.other_value: ", self.other_value)
        self.joined_value = self.value * self.other_value

    @task(returns = int)
    def multiplier_non_modifier(self):
        print("self.value: ", self.value)
        print("self.other_value: ", self.other_value)
        return self.value * self.other_value

class multipleInheritanceClass(myClass, myOtherClass, joinerClass):
    def __init__(self, value):
        myClass.__init__(self)
        myOtherClass.__init__(self, value)
