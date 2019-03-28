from pycompss.api.task import task
from pycompss.api.api import compss_wait_on

def one():
    return 1

@task(returns=int, tracing_hook=True)
def bad_to_hook(iterations):
    acum = 0
    for i in range(iterations):
        acum += one()
    return acum

def main(iterations):
    # Submit just one task that does a lot of calls to a simple funcion
    result = bad_to_hook(iterations)
    result = compss_wait_on(result)
    assert result == iterations, "Wrong result."

if __name__ == '__main__':
    iterations = 20000000  # 20m
    main(iterations)
