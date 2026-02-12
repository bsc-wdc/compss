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

    @staticmethod
    @task(returns=1)
    def increment(v, value):
        return v + value

    @staticmethod
    @constraint(computing_units="2")
    @task(returns=1)
    def subtract(v, value):
        return v - value

    @staticmethod
    @task(returns=1)
    def calcul(v, value):
        return v + external(value)

    @staticmethod
    @constraint(computing_units="2")
    @task(returns=1)
    def calcul_c(v, value):
        return v + externalc(value)
