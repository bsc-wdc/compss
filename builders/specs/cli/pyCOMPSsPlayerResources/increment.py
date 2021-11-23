import time
from pycompss.api.api import compss_wait_on
from pycompss.api.task import task

@task(returns=1)
def increment(value):
  time.sleep(value * 2)  # mimic some computational time
  return value + 1

def main():
    values = [1, 2, 3, 4]
    start_time = time.time()
    for pos in range(len(values)):
        values[pos] = increment(values[pos])
    values = compss_wait_on(values)
    assert values == [2, 3, 4, 5]
    print(values)
    print("Elapsed time: " + str(time.time() - start_time))

if __name__=='__main__':
    main()