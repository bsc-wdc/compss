import time

from pycompss.api.task import task
from pycompss.api.api import compss_barrier, compss_wait_on


@task(returns=int)
def task_one(num):
    time.sleep(0.5)
    return num


@task(returns=int)
def task_two(num):
    time.sleep(0.5)
    return num + 1


@task(returns=int)
def task_three(num):
    time.sleep(0.5)
    return num + 1


def main():
    value = task_one(1)
    value = task_two(value)
    value = task_three(value)
    compss_barrier()

    result = compss_wait_on(value)
    print("MONITOR_NEW_RESULT:", result)

if __name__ == "__main__":
    main()
