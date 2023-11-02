
import sys

from pycompss.api.task import task


@task(returns=list)
def gen_fragment():
    import random
    range_min = 0
    range_max = sys.maxsize
    fragment = []
    for _ in range(100):
        fragment.append((random.randrange(range_min, range_max),
                         random.random()))
    return fragment


@task(returns=list)
def gen_big_fragment():
    import random
    range_min = 0
    range_max = sys.maxsize
    fragment = []
    for _ in range(1000):
        fragment.append((random.randrange(range_min, range_max),
                         random.random()))
    return fragment
