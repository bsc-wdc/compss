from pycompss.api.task import task
from pycompss.api.constraint import constraint
from pycompss.api.api import compss_wait_on


@task(returns=1)
def increment(value):
    return value + 2


@constraint(computing_units=1)
@task(returns=1)
def decrement(value):
    return value - 1


def main():
    initial = 1
    partial = increment(initial)
    result = decrement(partial)
    result = compss_wait_on(result)
    assert result == initial + 1, "ERROR: Unexpected increment result."


# if __name__ == '__main__':
#     main()
