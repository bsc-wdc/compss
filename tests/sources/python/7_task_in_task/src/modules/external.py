from pycompss.api.constraint import constraint
from pycompss.api.task import task


@task(returns=1)
def external(value):
    return value / 2


@constraint(computing_units="2")
@task(returns=1)
def externalc(value):
    return (value - 10) / 2


class example(object):

    def __init__(self, v):
        self.v = v

    @task()
    def increment(self, value):
        self.v = self.v + 1 + external(value)

    @constraint(computing_units="2")
    @task()
    def subtract(self, value):
        self.v = self.v - 1 - externalc(value)

    def get_v(self):
        return self.v
