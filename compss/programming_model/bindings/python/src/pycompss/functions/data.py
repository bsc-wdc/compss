#
#  Copyright 2.02-2017 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
"""
@author: scorella

PyCOMPSs Functions: Data generators
===================================
    This file defines the common data producing functions.
"""

from pycompss.api.task import task


def chunks(l, n, balanced=False):
    """
    Generator to chunk data
    :param l: List of data to be chunked
    :param n: length of the fragments
    :param balanced: True to generate balanced fragments
    :return: yield fragments of size n from l
    """
    if not balanced or not len(l) % n:
        for i in xrange(0, len(l), n):
            yield l[i:i + n]
    else:
        rest = len(l) % n
        start = 0
        while rest:
            yield l[start: start + n + 1]
            rest -= 1
            start += n + 1
        for i in xrange(start, len(l), n):
            yield l[i:i + n]


@task(returns=list)
def _genRandom(size, sizeFrag, seed, jumps):
    import random
    random.seed(seed)
    random.jumpahead(jumps)
    return [[random.random() for _ in xrange(size)] for _ in xrange(sizeFrag)]


@task(returns=list)
def _genNormal(size, sizeFrag, seed, jumps):
    import random
    random.seed(seed)
    random.jumpahead(jumps)
    return [[random.gauss(mu=0.0, sigma=1.0) for _ in xrange(size)]
            for _ in xrange(sizeFrag)]


@task(returns=list)
def _genUniform(size, sizeFrag, seed, jumps):
    import random
    random.seed(seed)
    random.jumpahead(jumps)
    return [[random.uniform(-1.0, 1.0) for _ in xrange(size)]
            for _ in xrange(sizeFrag)]


def generator(size, numFrag, seed=None, distribution='random', wait=False):
    """
    Data generator
    :param size: (numElements,dim)
    :param numFrag: dataset's number of fragments
    :param seed: random seed. Default None, system time is used-
    :param distribution: random, normal, uniform
    :param wait: if we want to wait for result. Default False
    :return: random dataset
    """
    sizeFrag = size[0]/numFrag
    if distribution == 'random':
        data = [_genRandom(size[1], sizeFrag, seed, sizeFrag*i)
                for i in xrange(numFrag)]
    elif distribution == 'normal':
        data = [_genNormal(size[1], sizeFrag, seed, sizeFrag*i)
                for i in xrange(numFrag)]
    elif distribution == 'uniform':
        data = [_genUniform(size[1], sizeFrag, seed, sizeFrag*i)
                for i in xrange(numFrag)]
    if wait:
        from pycompss.api.api import compss_wait_on
        data = compss_wait_on(data)
    return data
