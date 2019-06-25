from pycompss.api.constraint import constraint
from pycompss.api.task import task


@task(returns=1, numba=True)
def external(value):
    return value / 2


@constraint(computing_units="2")
@task(returns=1, numba=True)
def externalc(value):
    return (value - 10) / 2


class example(object):

    def __init__(self, v):
        self.v = v

    @task(numba=True)
    def increment(self, value):
        self.v = self.v + value

    @constraint(computing_units="2")
    @task(numba=True)
    def subtract(self, value):
        self.v = self.v - value

    @task(numba=True)
    def calcul(self, value):
        self.v = self.v + external(value)

    @constraint(computing_units="2")
    @task(numba=True)
    def calcul_c(self, value):
        self.v = self.v + externalc(value)

    def get_v(self):
        return self.v
